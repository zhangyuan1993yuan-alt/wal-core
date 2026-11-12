package com.walcore.read;

import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.plugin.PluginClassLoader;
import com.walcore.plugin.fixtures.PassThroughCodec;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F4 回归：非 RAW codec + 大 payload 的读回（payloadSink 扩容）。</b>
 *
 * <h2>被回归的缺陷</h2>
 * {@code CursorImpl.payloadSink} 初值 64KB 且<b>从不扩容</b>：非 RAW codec
 * （走 {@code codec.decode} 路径）解码 payload &gt; 64KB 的帧时，
 * {@code FrameDecoder.decode} 返回 {@code STATUS_PAYLOAD_SINK_TOO_SMALL}(-5)，
 * 游标把它当损坏处理<b>跳过整段</b>——静默丢数据。恢复期
 * （{@code RecoveryManager.PartitionReader}）同源同病：恢复出的 nextLsn 偏低。
 *
 * <h2>断言口径</h2>
 * 用恒等但非 RAW 路径的 {@link PassThroughCodec}（codecId=200）写 8 条 100KB 记录，
 * 游标读回必须<b>全量 8 条、payload 逐字节一致</b>——旧代码这里读回 0 条（跳段）。
 */
class CursorLargePayloadCodecTest {

    /** 单条 payload 字节数：刻意大于 sink 初值 64KB。 */
    private static final int PAYLOAD = 100 * 1024;

    /** 记录条数。 */
    private static final int RECORDS = 8;

    @Test
    @DisplayName("F4：非 RAW codec + payload>64KB → 游标全量读回（sink 扩容）")
    void cursorReadsLargePayloadWithNonRawCodec(@TempDir Path dir) throws Exception {
        // 注意：目录 URL 必须以 '/' 结尾，URLClassLoader 才会把资源根下的
        // META-INF/services 解析出来（Path.toUri() 会补上，getResource 的裸 URL 不会）
        URL fixtures = Path.of(PassThroughCodec.class.getClassLoader()
                .getResource("spi-fixtures/all").toURI()).toUri().toURL();
        assertNotNull(fixtures, "测试夹具资源缺失");
        PluginClassLoader pluginLoader = new PluginClassLoader(
                new URL[]{fixtures},
                CursorLargePayloadCodecTest.class.getClassLoader(), "f4-test", null);

        List<byte[]> originals = new ArrayList<>(RECORDS);
        try (DefaultWal wal = TestWal.open(dir, 4L * 1024 * 1024, b -> b
                .partitions(1)
                .pluginClassLoader(pluginLoader)
                .codec(PassThroughCodec.ID))) {
            assertEquals(PassThroughCodec.ID, wal.codecId(),
                    "必须真的启用非 RAW codec，否则用例测不到 codec 解码路径");

            for (int i = 0; i < RECORDS; i++) {
                byte[] bytes = new byte[PAYLOAD];
                for (int j = 0; j < PAYLOAD; j++) {
                    bytes[j] = (byte) ((i * 41 + j) & 0xFF);
                }
                originals.add(bytes);
                long lsn = wal.append(0, ByteBuffer.wrap(bytes),
                        com.walcore.api.AppendOptions.DEFAULTS);
                assertEquals(i + 1L, lsn, "LSN 应从 1 起严格递增");
            }
            assertTrue(wal.flushAndWait(0, RECORDS, 30_000L), "关闭前的持久确认失败");

            // ---- 读回：全量 8 条、逐字节一致 ----
            List<WalRecord> read = readAll(wal, 0);
            assertEquals(RECORDS, read.size(),
                    "旧代码（sink 64KB 不扩容）这里读回 0 条：PAYLOAD_SINK_TOO_SMALL 被当作损坏跳段");
            for (int i = 0; i < RECORDS; i++) {
                assertEquals(i + 1L, read.get(i).lsn(), "第 " + i + " 条 LSN 不符");
                assertEquals(PAYLOAD, read.get(i).payloadLength(), "payload 长度不符");
                assertArrayEquals(originals.get(i), read.get(i).payloadBytes(),
                        "第 " + i + " 条 payload 逐字节不一致（sink 扩容不得丢字节/错位）");
            }
        }

        // ---- 重开恢复：恢复路径（PartitionReader）同样要全量读回 ----
        PluginClassLoader reopenLoader = new PluginClassLoader(
                new URL[]{fixtures},
                CursorLargePayloadCodecTest.class.getClassLoader(), "f4-reopen", null);
        try (DefaultWal reopened = TestWal.open(dir, 4L * 1024 * 1024, b -> b
                .partitions(1)
                .pluginClassLoader(reopenLoader)
                .codec(PassThroughCodec.ID))) {
            List<WalRecord> recovered = readAll(reopened, 0);
            assertEquals(RECORDS, recovered.size(),
                    "重开后必须恢复全量记录（恢复期 payloadSink 与游标同源，旧代码同样丢段）");
            for (int i = 0; i < RECORDS; i++) {
                assertArrayEquals(originals.get(i), recovered.get(i).payloadBytes(),
                        "恢复后的第 " + i + " 条 payload 不一致");
            }
            // LSN 接续：恢复出的 nextLsn 必须覆盖全部 8 条（不会因丢段而偏低）
            long next = reopened.append(0, TestWal.payload(64),
                    com.walcore.api.AppendOptions.DEFAULTS);
            assertEquals(RECORDS + 1L, next, "恢复后 LSN 必须接在全部记录之后（丢段会偏低）");
        }
    }

    /**
     * 读回某分区全部记录副本。
     *
     * @param wal 实例
     * @param pid 分区
     * @return 记录副本
     */
    private static List<WalRecord> readAll(DefaultWal wal, int pid) {
        List<WalRecord> out = new ArrayList<>();
        try (WalCursor cursor = wal.cursor(pid,
                CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.copy());
            }
        }
        return out;
    }
}
