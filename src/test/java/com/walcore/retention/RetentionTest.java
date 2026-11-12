package com.walcore.retention;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.plugin.PluginClassLoader;
import com.walcore.internal.retention.RetentionManager;
import com.walcore.internal.spiimpl.TimeSizeRetentionPolicy;
import com.walcore.plugin.fixtures.EagerRetentionPolicy;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.SegmentInfo;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T14：保留策略与安全删除的验收测试。
 *
 * <p>分两层：
 * <ol>
 *   <li><b>策略层</b>纯函数测试（构造 {@link RetentionContext} 直接断言决策），
 *       保证"时间/大小取更严格"、"pin 保护"、"永不删最后一段"这些<b>规则</b>正确；
 *   <li><b>管理器层</b>集成测试（真 WAL + 真段文件），保证"删除真的发生、
 *       读者在用时跳过、删完还能读"这些<b>效果</b>正确。
 * </ol>
 */
@DisplayName("保留策略与安全删除")
class RetentionTest {

    private static final long SEGMENT_SIZE = 4096L;
    /** 4096B 段 / 80B 帧 ≈ 51 条；取 50 保证每 50 条一定换段。 */
    private static final int RECORDS_PER_SEGMENT = 50;

    // ==================================================================
    // 策略层：时间维度
    // ==================================================================

    @Test
    @DisplayName("时间维度：超过保留时长的段被选中，未超过的不动")
    void timeDimensionSelectsExpiredSegmentsOnly() {
        long now = 1_700_000_000_000L;
        List<SegmentInfo> segments = List.of(
                seg(0, 1L, 10L, 100L, now - 10_000L),
                seg(0, 11L, 20L, 100L, now - 5_000L),
                seg(0, 21L, 30L, 100L, now - 10L));
        RetentionContext ctx = new RetentionContext(0, segments, now, 300L, 0L, 30L);

        TimeSizeRetentionPolicy policy = new TimeSizeRetentionPolicy(1_000L, 0L);
        RetentionDecision decision = policy.evaluate(ctx);

        assertEquals(2, decision.toDelete().size(), "只有前两段超过 1s: " + decision);
        assertEquals(1L, decision.toDelete().get(0).firstLsn());
        assertEquals(11L, decision.toDelete().get(1).firstLsn());
        assertTrue(decision.reason().contains("time-or-size"), decision.reason());
    }

    @Test
    @DisplayName("大小维度：超上限时从最旧的开始删，直到降到上限以下")
    void sizeDimensionDeletesOldestFirst() {
        long now = 1_700_000_000_000L;
        List<SegmentInfo> segments = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            segments.add(seg(0, 1L + i * 10L, 10L + i * 10L, 100L, now - 1_000L + i));
        }
        RetentionContext ctx = new RetentionContext(0, segments, now, 500L, 0L, 50L);

        TimeSizeRetentionPolicy policy = new TimeSizeRetentionPolicy(0L, 250L);
        RetentionDecision decision = policy.evaluate(ctx);

        assertEquals(3, decision.toDelete().size(),
                "500B 要降到 ≤250B 需删掉 3 个 100B 的段，实际 = " + decision);
        assertEquals(1L, decision.toDelete().get(0).firstLsn(), "必须从最旧的删");
    }

    @Test
    @DisplayName("双维度取更严格：超时的小段与未超时但过大导致的段都被选中")
    void bothDimensionsAreUnionNotIntersection() {
        long now = 1_700_000_000_000L;
        // 段0 很老但很小；段1 很新但很大；段2 是最后一段（永不删）
        List<SegmentInfo> segments = List.of(
                seg(0, 1L, 10L, 10L, now - 100_000L),
                seg(0, 11L, 20L, 1_000L, now - 10L),
                seg(0, 21L, 30L, 10L, now - 5L));
        RetentionContext ctx = new RetentionContext(0, segments, now, 1_020L, 0L, 30L);

        TimeSizeRetentionPolicy policy = new TimeSizeRetentionPolicy(1_000L, 20L);
        RetentionDecision decision = policy.evaluate(ctx);

        assertEquals(2, decision.toDelete().size(),
                "时间维度命中段0、大小维度命中段1，并集应为 2 段: " + decision);
        assertTrue(decision.reason().contains("over-bytes"), "原因里要体现超量: " + decision.reason());
    }

    @Test
    @DisplayName("永不删除最后一个段（即使它超过所有阈值）")
    void lastSegmentIsNeverDeleted() {
        long now = 1_700_000_000_000L;
        List<SegmentInfo> onlyOne = List.of(seg(0, 1L, 10L, 9_999L, now - 999_999L));
        RetentionContext ctx = new RetentionContext(0, onlyOne, now, 9_999L, 0L, 10L);

        TimeSizeRetentionPolicy policy = new TimeSizeRetentionPolicy(1L, 1L);
        assertFalse(policy.evaluate(ctx).hasWork(), "只有一个段时不得删除");
    }

    @Test
    @DisplayName("尊重 minPinnedLsn：边界之后的段不删")
    void pinnedLsnProtectsNewerSegments() {
        long now = 1_700_000_000_000L;
        List<SegmentInfo> segments = List.of(
                seg(0, 1L, 10L, 100L, now - 100_000L),
                seg(0, 11L, 20L, 100L, now - 100_000L),
                seg(0, 21L, 30L, 100L, now - 100_000L));
        // 消费者停在 15：段 11..20 必须保留
        RetentionContext ctx = new RetentionContext(0, segments, now, 300L, 15L, 30L);

        TimeSizeRetentionPolicy policy = new TimeSizeRetentionPolicy(1_000L, 0L);
        RetentionDecision decision = policy.evaluate(ctx);

        assertEquals(1, decision.toDelete().size(), "只允许删段 1..10: " + decision);
        assertEquals(1L, decision.toDelete().get(0).firstLsn());
    }

    // ==================================================================
    // 管理器层：真删除
    // ==================================================================

    @Test
    @DisplayName("管理器真删段：文件消失、目录段数下降、幸存记录仍可完整读回")
    void managerDeletesSegmentsAndKeepsDataReadable(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAcrossSegments(wal, 0, 6);
            assertTrue(wal.flushAndWait(0, last, 10_000L));
            int before = wal.segmentManager().segmentCount(0);
            assertTrue(before >= 5, "应当已经产生多个段，实际 = " + before);

            RetentionManager manager = newManager(wal, new TimeSizeRetentionPolicy(0L, 1L));
            RetentionManager.RunResult result = manager.runPartition(0, false);

            assertTrue(result.deleted() > 0, "必须真的删掉一些段: " + result);
            assertTrue(wal.segmentManager().segmentCount(0) < before,
                    "段数必须下降: before=" + before + ", after=" + wal.segmentManager().segmentCount(0));
            assertEquals(1, wal.segmentManager().segmentCount(0),
                    "大小上限设成 1B 时会一路删到只剩最后一段");

            // 幸存记录必须仍能顺序读回，且是连续后缀
            List<Long> survivors = TestWal.readAllLsns(wal, 0);
            assertFalse(survivors.isEmpty(), "至少最后一段的记录必须可读");
            for (int i = 1; i < survivors.size(); i++) {
                assertEquals(survivors.get(i - 1) + 1L, survivors.get(i).longValue(),
                        "幸存记录必须连续");
            }
            assertEquals(last, survivors.get(survivors.size() - 1).longValue(),
                    "最后一条记录绝不能丢");
        }
    }

    @Test
    @DisplayName("仍有读者在读的段：本轮跳过，读者关闭后的下一轮才删")
    void segmentWithReaderIsSkippedThenDeleted(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAcrossSegments(wal, 0, 6);
            assertTrue(wal.flushAndWait(0, last, 10_000L));
            long firstSegmentLsn = wal.segmentManager().segments(0).get(0).firstLsn();

            // 刻意用一个无视 minPinnedLsn、想删掉除最后一段外全部段的策略：
            // 这样才能验证框架层的安全检查（而不是策略自己很乖）
            RetentionManager manager = newManager(wal, new RecordingPolicy());
            try (WalCursor cursor = wal.cursor(0)) {
                WalRecord first = cursor.next();
                assertNotNull(first, "游标必须能读到第一条");
                assertTrue(wal.segmentManager().segments(0).stream()
                                .anyMatch(s -> s.readerCount() > 0),
                        "打开游标后必须有段登记了读者");

                RetentionManager.RunResult skipped = manager.runPartition(0, false);
                assertTrue(skipped.skipped() > 0, "有读者的段必须被跳过: " + skipped);
                assertTrue(wal.segmentManager().segments(0).stream()
                                .anyMatch(s -> s.firstLsn() == firstSegmentLsn),
                        "被读者持有的首段不能被删掉");
            }

            RetentionManager.RunResult afterClose = manager.runPartition(0, false);
            assertTrue(afterClose.deleted() > 0, "读者关闭后必须能继续删: " + afterClose);
            assertTrue(wal.segmentManager().segmentCount(0) >= 1, "至少保留一个段");
        }
    }

    @Test
    @DisplayName("force=true 时忽略 pin 保护（并记录强制计数）")
    void forceIgnoresPinProtection(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAcrossSegments(wal, 0, 6);
            assertTrue(wal.flushAndWait(0, last, 10_000L));
            int before = wal.segmentManager().segmentCount(0);

            RetentionManager manager = newManager(wal, new TimeSizeRetentionPolicy(0L, 1L));
            // 把边界钉在"第一个段的首帧"：此时所有段的 lastLsn 都 >= 该值 ⇒ 全部受保护
            long oldestFirstLsn = wal.segmentManager().segments(0).get(0).firstLsn();
            manager.pin(oldestFirstLsn);
            RetentionManager.RunResult normal = manager.runPartition(0, false);
            assertEquals(0, normal.deleted(), "有 pin 时不得删除: " + normal);
            assertEquals(before, wal.segmentManager().segmentCount(0));

            RetentionManager.RunResult forced = manager.runPartition(0, true);
            assertTrue(forced.deleted() > 0, "force 必须无视 pin: " + forced);
            assertEquals(1, wal.segmentManager().segmentCount(0));
        }
    }

    @Test
    @DisplayName("onSegmentDeleted 回调带正确的段信息，且删除后统计一致")
    void onSegmentDeletedCallbackReceivesCorrectInfo(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAcrossSegments(wal, 0, 5);
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            RecordingPolicy policy = new RecordingPolicy();
            RetentionManager manager = newManager(wal, policy);
            manager.runPartition(0, false);

            assertEquals(manager.deletedSegmentCount(), policy.deleted.size(),
                    "每次删除都必须回调一次 onSegmentDeleted");
            assertTrue(policy.deleted.size() > 0, "本轮应当删了段");
            for (SegmentInfo info : policy.deleted) {
                assertEquals(0, info.partitionId());
                assertTrue(info.firstLsn() > 0L);
                assertTrue(info.lastLsn() >= info.firstLsn());
                assertTrue(info.sizeBytes() > 0L, "逻辑占用必须为正: " + info);
                assertNotNull(info.path());
                assertFalse(java.nio.file.Files.exists(info.path()),
                        "回调时文件必须已被删除: " + info.path());
            }
        }
    }

    @Test
    @DisplayName("配置自定义 RetentionPolicy 插件时生效（wal.spi.retention）")
    void customRetentionPolicyPluginTakesEffect(@TempDir Path dir) throws Exception {
        PluginClassLoader loader = new PluginClassLoader(
                new URL[]{fixture("retention").toUri().toURL()},
                RetentionTest.class.getClassLoader(), "retention-test", null);
        WalConfig config = TestWal.builder(dir, SEGMENT_SIZE)
                .pluginClassLoader(loader)
                .retentionPolicy(EagerRetentionPolicy.ID)
                .retentionEnabled(false)
                .buildConfig();

        try (DefaultWal wal = DefaultWal.open(config)) {
            assertNotNull(wal.pluginManager().retentionPolicy(), "必须加载到保留策略插件");
            assertEquals(EagerRetentionPolicy.ID, wal.pluginManager().retentionPolicy().id(),
                    "配置的 id 必须生效（而不是内置实现）");
            assertEquals(EagerRetentionPolicy.ID, wal.retentionManager().policy().id());
        }
    }

    @Test
    @DisplayName("删除期间写入不受影响：边写边删，写入不抛异常、LSN 仍连续")
    void writesAreUnaffectedByConcurrentRetention(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAcrossSegments(wal, 0, 6);
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            RetentionManager manager = newManager(wal, new TimeSizeRetentionPolicy(0L, 2_000L));
            AtomicInteger failures = new AtomicInteger();
            List<String> failureReasons = new ArrayList<>();
            long before = last;

            for (int round = 0; round < 20; round++) {
                manager.runPartition(0, false);
                try {
                    for (int i = 0; i < RECORDS_PER_SEGMENT; i++) {
                        before = wal.append(0, TestWal.payload(16), AppendOptions.DEFAULTS);
                    }
                    assertTrue(wal.flushAndWait(0, before, 10_000L));
                } catch (RuntimeException e) {
                    failures.incrementAndGet();
                    failureReasons.add(String.valueOf(e));
                }
            }
            assertEquals(0, failures.get(),
                    "保留策略的删除不得让写入失败（读的是不同文件，不应互相干扰）: "
                            + failureReasons);
            assertEquals(last + 20L * RECORDS_PER_SEGMENT, before,
                    "LSN 必须连续（删除不会回收 LSN）");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 构造一个假的段信息。
     *
     * @param pid 分区
     * @param firstLsn 首 LSN
     * @param lastLsn 末 LSN
     * @param size 逻辑占用
     * @param createdMillis 创建时刻
     * @return 段信息
     */
    private static SegmentInfo seg(int pid, long firstLsn, long lastLsn, long size,
                                  long createdMillis) {
        return new SegmentInfo(pid, firstLsn, lastLsn, size, createdMillis,
                Path.of("wal-p" + pid + "-" + firstLsn + ".wal"));
    }

    /**
     * 写入足够多的记录以产生多段。
     *
     * @param wal 实例
     * @param pid 分区
     * @param segments 期望的段数
     * @return 最后一条 LSN
     */
    private static long writeAcrossSegments(DefaultWal wal, int pid, int segments) {
        long last = 0L;
        for (int i = 0; i < segments * RECORDS_PER_SEGMENT; i++) {
            last = wal.append(pid, TestWal.payload(16), AppendOptions.DEFAULTS);
        }
        return last;
    }

    /**
     * 用给定策略手工装配一个管理器（不启动后台线程，由测试驱动 {@code runPartition}）。
     *
     * @param wal 实例
     * @param policy 策略
     * @return 管理器
     */
    private static RetentionManager newManager(DefaultWal wal,
                                               com.walcore.spi.RetentionPolicy policy) {
        return new RetentionManager(wal.segmentManager(), wal.registry(), policy,
                wal.config(), null, null);
    }

    /**
     * 解析 {@code spi-fixtures/<name>} 目录。
     *
     * @param name 场景名
     * @return 目录路径
     */
    private static Path fixture(String name) {
        try {
            URL url = RetentionTest.class.getClassLoader()
                    .getResource("spi-fixtures/" + name);
            assertNotNull(url, "测试资源缺失: spi-fixtures/" + name);
            return Path.of(url.toURI());
        } catch (java.net.URISyntaxException e) {
            throw new AssertionError("解析测试资源失败: " + name, e);
        }
    }

    /**
     * 记录删除回调的策略（永远想删掉除最后一段之外的全部段）。
     */
    private static final class RecordingPolicy implements com.walcore.spi.RetentionPolicy {

        private final List<SegmentInfo> deleted = new ArrayList<>();

        @Override
        public String id() {
            return "test:retention-recording";
        }

        @Override
        public int apiVersion() {
            return com.walcore.api.WalVersion.API_VERSION;
        }

        @Override
        public void init(com.walcore.spi.PluginContext context) {
            // 无状态
        }

        @Override
        public RetentionDecision evaluate(RetentionContext context) {
            List<SegmentInfo> all = context.segments();
            if (all.size() <= 1) {
                return RetentionDecision.keepAll();
            }
            List<SegmentInfo> toDelete = new ArrayList<>(all.subList(0, all.size() - 1));
            return RetentionDecision.delete(toDelete, "recording");
        }

        @Override
        public synchronized void onSegmentDeleted(SegmentInfo info) {
            deleted.add(info);
        }

        @Override
        public void close() {
            // 无外部资源
        }
    }
}
