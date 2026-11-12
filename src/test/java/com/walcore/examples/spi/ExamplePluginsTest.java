package com.walcore.examples.spi;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.examples.spi.CountBasedIndexStore;
import com.walcore.examples.spi.CountingCodec;
import com.walcore.examples.spi.PrintlnReplicationSink;
import com.walcore.examples.spi.Rot13Codec;
import com.walcore.internal.plugin.PluginDiagnostics;
import com.walcore.internal.plugin.PluginManager;
import com.walcore.internal.plugin.PluginType;
import com.walcore.spi.WalPlugin;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T20 验收：<b>示例插件 + ExamplePluginsTest</b>。
 *
 * <h2>这组测试要证明的三件事</h2>
 * <ol>
 *   <li>四个示例插件都是<b>可编译、可实例化、契约自洽</b>的（不是伪代码）；</li>
 *   <li>它们能被 {@code ServiceLoader} 通过 {@code META-INF/services} <b>发现</b>，
 *       并出现在 {@code PluginManager} 的候选里（这正是使用方接入自己插件的同一条路径）；</li>
 *   <li>换掉 codec / indexStore / replicationSink 之后，<b>数据仍能正确写读</b>——
 *       这是"SPI 可扩展"的真正判据，而不是"类加载成功了"。</li>
 * </ol>
 *
 * <h2>为什么示例放在 {@code src/test/java}</h2>
 * 示例插件若打进主 jar，就会成为"我们也支持这些插件"的承诺，而它们只是教学代码
 * （ROT13 会改字节、打印 sink 会刷屏）。放进 test 源码树既能被 CI 持续编译验证，
 * 又不会污染交付物。
 *
 * <p>服务声明文件同样不放在类路径根部，而是放在
 * {@code src/test/resources/spi-fixtures/example/META-INF/services/}，
 * 由测试用独立的 {@link URLClassLoader} 喂进去，避免影响其它用例的默认插件。
 */
class ExamplePluginsTest {

    // ------------------------------------------------------------------
    // ① 契约自洽
    // ------------------------------------------------------------------

    @Test
    @DisplayName("CountingCodec：恒等映射 + 正确计数，且不修改 src 的 position/limit")
    void countingCodecIsIdentityAndObserves() {
        CountingCodec codec = new CountingCodec();
        assertEquals(CountingCodec.ID, codec.id());
        assertEquals(com.walcore.api.WalVersion.API_VERSION, codec.apiVersion());
        assertEquals(128, codec.codecId(), "第三方 codec 必须落在 128~255 区间");

        ByteBuffer src = ByteBuffer.wrap("hello-wal".getBytes(StandardCharsets.US_ASCII));
        int savedPos = src.position();
        int savedLimit = src.limit();
        ByteBuffer dst = ByteBuffer.allocate(64);

        int stored = codec.encode(src, 0, src.remaining(), dst, 8, 56);
        assertEquals(9, stored);
        assertEquals(savedPos, src.position(), "编码器不得改动调用方缓冲区的 position");
        assertEquals(savedLimit, src.limit(), "编码器不得改动调用方缓冲区的 limit");
        assertEquals('h', (char) dst.get(8));
        assertEquals('l', (char) dst.get(16));

        ByteBuffer decoded = ByteBuffer.allocate(9);
        assertEquals(9, codec.decode(dst, 8, 9, 9, decoded, 0));
        assertEquals("hello-wal",
                new String(decoded.array(), 0, 9, StandardCharsets.US_ASCII));
        assertEquals(1L, codec.encodedFrames());
        assertEquals(9L, codec.encodedBytes());
        assertEquals(1L, codec.decodedFrames());

        // 契约：容量不足必须返回 -1 而不是抛异常或越界写
        assertEquals(-1, codec.encode(src, 0, 9, ByteBuffer.allocate(4), 0, 4));
        assertEquals(9, codec.maxStoredSize(9), "maxStoredSize 必须是上界");
    }

    @Test
    @DisplayName("Rot13Codec：encode/decode 互为逆运算，非字母字节原样保留")
    void rot13CodecIsReversible() {
        Rot13Codec codec = new Rot13Codec();
        assertEquals(129, codec.codecId());

        byte[] plain = "Abc-XYZ 123".getBytes(StandardCharsets.US_ASCII);
        ByteBuffer src = ByteBuffer.wrap(plain);
        ByteBuffer enc = ByteBuffer.allocate(plain.length);
        assertEquals(plain.length, codec.encode(src, 0, plain.length, enc, 0, plain.length));

        String encoded = new String(enc.array(), 0, plain.length, StandardCharsets.US_ASCII);
        assertEquals("Nop-KLM 123", encoded, "字母 +13，其余原样");
        assertNotSame(new String(plain, StandardCharsets.US_ASCII), encoded);

        ByteBuffer dec = ByteBuffer.allocate(plain.length);
        assertEquals(plain.length, codec.decode(enc, 0, plain.length, plain.length, dec, 0));
        assertEquals(new String(plain, StandardCharsets.US_ASCII),
                new String(dec.array(), 0, plain.length, StandardCharsets.US_ASCII),
                "decode(encode(x)) 必须等于 x");
    }

    @Test
    @DisplayName("PrintlnReplicationSink：四个回调都可调用，replicatedLsn 如实返回")
    void printlnSinkHandlesAllCallbacks() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8);
        PrintlnReplicationSink sink = new PrintlnReplicationSink(out);

        assertEquals(PrintlnReplicationSink.ID, sink.id());
        assertEquals(0L, sink.replicatedLsn(0), "还没有记录时必须是 0");
        sink.onPartitionAdded(0, 1L);
        sink.onRecord(new FakeView(0, 7L, "hello"));
        sink.onTruncated(0, 5L);
        sink.onPartitionRemoved(0, 7L);
        sink.close();

        String text = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("partition-added pid=0 firstLsn=1"), text);
        assertTrue(text.contains("record pid=0 lsn=7 len=5 payload=hello"), text);
        assertTrue(text.contains("truncated pid=0 fromLsn=5"), text);
        assertTrue(text.contains("partition-removed pid=0 lastLsn=7"), text);
        assertEquals(1L, sink.recordCount());
        assertEquals(5L, sink.payloadBytes());
        assertEquals(1L, sink.truncationCount());
        assertEquals(7L, sink.replicatedLsn(0));
        assertEquals(com.walcore.spi.ReplicationSink.Mode.ASYNC, sink.mode());
    }

    @Test
    @DisplayName("CountBasedIndexStore：按间隔采样落索引项，lookup 仍能定位")
    void countBasedIndexStoreSamples(@TempDir Path dir) {
        CountBasedIndexStore store = new CountBasedIndexStore(4);
        assertEquals(CountBasedIndexStore.ID, store.id());
        assertEquals(4, store.intervalRecords());

        WalConfig config = TestWal.builder(dir, 4096L).buildConfig();
        store.init(new TestPluginContext(config, dir));
        try {
            store.openSegment(0, 1L, 4096L);
            long offset = 0L;
            for (long lsn = 1L; lsn <= 40L; lsn++) {
                store.onAppended(0, 1L, lsn, offset);
                offset += 80L;
            }
            // 每 4 条一个索引项 → lsn = 1、5、9、...
            assertEquals(1L, store.lookup(1L).lsn(), "首条必须被索引");
            assertEquals(5L, store.lookup(8L).lsn(), "lookup 必须返回 <= 目标 的最近索引项");
            assertEquals(37L, store.lookup(40L).lsn());
            assertEquals(37L, store.lastIndexedLsn());
            assertTrue(store.memoryBytes() >= 0L, "memoryBytes 必须可读（允许为 0）");
            assertEquals(1, store.trackedSegments());
        } finally {
            store.closeSegment(1L);
            store.close();
        }
        assertEquals(0, store.trackedSegments(), "关闭段后计数器必须释放，否则长期运行会泄漏");
    }

    // ------------------------------------------------------------------
    // ② 能被 ServiceLoader 发现
    // ------------------------------------------------------------------

    @Test
    @DisplayName("四个示例插件都能通过 META-INF/services 被发现")
    void examplesAreDiscoverableThroughServiceLoader() throws Exception {
        ClassLoader loader = exampleFixtures();
        List<String> codecs = loadIds(loader, com.walcore.spi.RecordCodec.class);
        assertTrue(codecs.contains(CountingCodec.ID), "示例 codec 必须被发现: " + codecs);
        assertTrue(codecs.contains(Rot13Codec.ID), "示例 codec 必须被发现: " + codecs);

        List<String> sinks = loadIds(loader, com.walcore.spi.ReplicationSink.class);
        assertTrue(sinks.contains(PrintlnReplicationSink.ID), "示例 sink 必须被发现: " + sinks);

        List<String> indexes = loadIds(loader, com.walcore.spi.IndexStore.class);
        assertTrue(indexes.contains(CountBasedIndexStore.ID), "示例索引必须被发现: " + indexes);
    }

    @Test
    @DisplayName("PluginManager 能看到示例插件，并可用显式 id 选中其中之一")
    void pluginManagerSelectsByExplicitId(@TempDir Path dir) throws Exception {
        WalBuilder builder = TestWal.builder(dir, 1L << 20)
                .pluginClassLoader(exampleFixtures())
                .codec(CountingCodec.ID)
                .indexStore(CountBasedIndexStore.ID);
        try (PluginManager manager = new PluginManager(builder.buildConfig(), dir, null)) {
            PluginDiagnostics diag = manager.scan();
            assertEquals(CountingCodec.ID, manager.current().codec().id(),
                    "显式指定 id 必须生效: " + diag);
            assertEquals(CountBasedIndexStore.ID, manager.current().indexStore().id());
            List<String> ids = new ArrayList<>();
            for (PluginType.PluginInfo info : manager.list(PluginType.CODEC)) {
                ids.add(info.id());
            }
            assertTrue(ids.contains(Rot13Codec.ID), "候选列表里应能看到另一个示例 codec: " + ids);
            assertTrue(ids.contains("builtin:raw"), "内置 codec 也应仍在候选里: " + ids);
        }
    }

    // ------------------------------------------------------------------
    // ③ 换插件后数据仍正确
    // ------------------------------------------------------------------

    @Test
    @DisplayName("用示例 codec + 示例 indexStore 真实写读 200 条，数据一条不差")
    void writeAndReadWithExamplePlugins(@TempDir Path dir) {
        WalBuilder builder = TestWal.builder(dir, 4096L)
                // 注意：Rot13 会改字节，但它与解码器成对使用，所以读回来仍是原文
                .codec(Rot13Codec.ID)
                .indexStore(CountBasedIndexStore.ID);
        try (Wal wal = builder.build()) {
            List<String> expected = new ArrayList<>();
            long last = 0L;
            for (int i = 0; i < 200; i++) {
                String text = "payload-" + i;
                expected.add(text);
                last = wal.append(0, ByteBuffer.wrap(text.getBytes(StandardCharsets.US_ASCII)),
                        AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            List<String> actual = new ArrayList<>();
            try (WalCursor cursor = wal.cursor(0,
                    CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
                WalRecord record;
                while ((record = cursor.next()) != null) {
                    ByteBuffer payload = record.payload();
                    byte[] copy = new byte[payload.remaining()];
                    payload.duplicate().get(copy);
                    actual.add(new String(copy, StandardCharsets.US_ASCII));
                }
            }
            assertEquals(expected, actual, "换了 codec 与 indexStore 之后，读回的数据必须完全一致");
            assertNotNull(wal.stats());
        }
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    /**
     * 把 {@code spi-fixtures/example} 目录包成 ClassLoader。
     *
     * @return 类加载器
     * @throws URISyntaxException 资源路径异常
     */
    private static ClassLoader exampleFixtures() throws IOException, URISyntaxException {
        URL url = ExamplePluginsTest.class.getResource("/spi-fixtures/example");
        if (url == null) {
            throw new IllegalStateException("找不到 spi-fixtures/example");
        }
        // 关键：把"资源 URL"转成"目录 URL"。目录 URL 必须带结尾斜杠，否则
        // ClassLoader 会把 META-INF/services/... 解析成父目录下的路径而找不到 provider。
        // Path#toUri() 会自动补斜杠，getResource 返回的字符串不一定有。
        URL dirUrl = Path.of(url.toURI()).toUri().toURL();
        return new URLClassLoader(new URL[] {dirUrl}, ExamplePluginsTest.class.getClassLoader());
    }

    /**
     * 用指定类加载器发现某类 SPI 的全部 id。
     *
     * @param loader 类加载器
     * @param iface SPI 接口
     * @param <T> 插件类型
     * @return id 列表
     */
    private static <T extends WalPlugin> List<String> loadIds(ClassLoader loader, Class<T> iface) {
        List<String> ids = new ArrayList<>();
        for (T plugin : ServiceLoader.load(iface, loader)) {
            ids.add(plugin.id());
        }
        return ids;
    }

    /** 最小的 {@code WalRecordView} 假实现，只为验证 sink 的回调契约。 */
    private static final class FakeView implements com.walcore.spi.WalRecordView {

        private final int partitionId;
        private final long lsn;
        private final ByteBuffer payload;

        FakeView(int partitionId, long lsn, String text) {
            this.partitionId = partitionId;
            this.lsn = lsn;
            this.payload = ByteBuffer.wrap(text.getBytes(StandardCharsets.US_ASCII));
        }

        @Override
        public long lsn() {
            return lsn;
        }

        @Override
        public long timestampNanos() {
            return 0L;
        }

        @Override
        public int partitionId() {
            return partitionId;
        }

        @Override
        public byte recordType() {
            return 0;
        }

        @Override
        public int payloadLength() {
            return payload.remaining();
        }

        @Override
        public ByteBuffer payload() {
            return payload.asReadOnlyBuffer();
        }

        @Override
        public long attributes() {
            return 0L;
        }

        @Override
        public int codecId() {
            return Rot13Codec.class.getName().hashCode() & 0;
        }
    }

    /** 最小 {@code PluginContext} 实现，够示例插件初始化用。 */
    private static final class TestPluginContext implements com.walcore.spi.PluginContext {

        private final WalConfig config;
        private final Path baseDir;

        TestPluginContext(WalConfig config, Path baseDir) {
            this.config = config;
            this.baseDir = baseDir;
        }

        @Override
        public WalConfig config() {
            return config;
        }

        @Override
        public int partitionCount() {
            return config.partitionCount();
        }

        @Override
        public com.walcore.internal.metrics.MetricsRegistry metrics() {
            return new com.walcore.internal.metrics.MetricsRegistry(false);
        }

        @Override
        public ClassLoader pluginClassLoader() {
            return ExamplePluginsTest.class.getClassLoader();
        }

        @Override
        public Path baseDir() {
            return baseDir;
        }

        @Override
        public void warn(String code, String message, Throwable cause) {
            // 示例测试不需要告警输出
        }
    }
}
