package com.walcore.api;

import com.walcore.internal.DefaultWal;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link WalStats} 快照的契约测试（T17 的对外可见面）。
 *
 * <h2>为什么单独测"快照"而不只测指标注册中心</h2>
 * 使用方（运维看板、告警、自动扩缩容）<b>只能</b>通过 {@link Wal#stats()} 观察这个库。
 * 指标注册中心内部算得再准，如果 {@code stats()} 漏搬一项、搬错口径、或者返回了可变引用
 * （调用方一改就污染下一次快照），对外就是错的。本类正是锁这三件事：
 * <ol>
 *   <li><b>齐全</b>：PRD P1-05 的六项必须都能从一次 {@code stats()} 里拿到；</li>
 *   <li><b>口径</b>：{@code totalLogBytes} 必须是"数据字节"而不是"段容量"；</li>
 *   <li><b>只读</b>：返回的 Map 是副本，调用方改它不影响库内状态。</li>
 * </ol>
 */
@DisplayName("WalStats 快照")
class WalStatsSnapshotTest {

    private static final int PAYLOAD = 256;

    @Test
    @DisplayName("空实例：HEALTHY + 全零，且不抛异常")
    void emptyInstanceIsHealthyAndZeroed(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            WalStats stats = wal.stats();
            assertEquals(Health.HEALTHY, stats.health(), "刚打开且未失败时必须 HEALTHY");
            assertEquals(0L, stats.totalLogBytes(), "尚未写入时日志字节数必须为 0");
            assertEquals(0L, stats.totalRecords(), "尚未写入时记录数必须为 0");
            assertEquals(0L, stats.publishedLsnMax(), "尚未写入时已发布 LSN 为 0");
            assertEquals(0L, stats.durableLsnMax(), "尚未写入时持久化水位为 0");
            assertEquals(0L, stats.corruptionCount(), "健康实例不应有损坏计数");
            assertEquals(0L, stats.rotateFallbackCount(), "预热前可能有 fallback，但不应在刚打开时就非零");
        }
    }

    @Test
    @DisplayName("PRD P1-05 六项指标都能从一次 stats() 取到")
    void sixCoreMetricsAreAllReachable(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 50; i++) {
                TestWal.append(wal, 0, PAYLOAD);
            }
            assertTrue(wal.flushAndWait(0, 50, 10_000L), "必须可刷盘");
            WalStats stats = wal.stats();

            // ① 写入吞吐的两个构成量：条数 + 字节数
            assertTrue(stats.counters().containsKey(MetricsRegistry.APPEND_RECORDS),
                    "缺『写入条数』计数器，无法算条/s: " + stats.counters().keySet());
            assertTrue(stats.counters().containsKey(MetricsRegistry.APPEND_BYTES),
                    "缺『写入字节数』计数器，无法算字节/s");
            // ② append 与 flushAndWait 的分位
            assertTrue(stats.latencies().containsKey(MetricsRegistry.APPEND_LATENCY),
                    "缺 append 延迟直方图，无法算 p50/p99/p999: " + stats.latencies().keySet());
            assertTrue(stats.latencies().containsKey(MetricsRegistry.FLUSH_LATENCY),
                    "缺 flushAndWait 延迟直方图（必须与 append 分开，口径不同）");
            // ③ 组提交批大小分布
            assertTrue(stats.counters().containsKey(MetricsRegistry.BATCH_RECORDS)
                            || stats.counters().containsKey(MetricsRegistry.FSYNC_COUNT),
                    "组提交批大小/fsync 次数至少要暴露一个");
            // ④ 日志总大小
            assertTrue(stats.totalLogBytes() > 0L, "写入后日志总大小必须非零");
            // ⑤ fsync 耗时
            assertTrue(stats.latencies().containsKey(MetricsRegistry.FSYNC_LATENCY),
                    "缺 fsync 耗时直方图");
            // ⑥ 健康度
            assertNotNull(stats.health());
        }
    }

    @Test
    @DisplayName("totalLogBytes 按『数据字节』计，不是按段容量计")
    void totalLogBytesCountsDataNotCapacity(@TempDir Path dir) {
        long segmentSize = 4096L;
        int records = 40;
        try (DefaultWal wal = TestWal.open(dir, segmentSize, b -> {
        })) {
            for (int i = 0; i < records; i++) {
                TestWal.append(wal, 0, PAYLOAD);
            }
            assertTrue(wal.flushAndWait(0, records, 10_000L), "必须可刷盘");

            WalStats stats = wal.stats();
            long expectedData = 0L;
            long capacity = 0L;
            List<Segment> segments = wal.segmentManager().segments(0);
            for (Segment segment : segments) {
                capacity += segment.capacityBytes();
                expectedData += segment.isActive()
                        ? Math.max(0L, segment.publishedWritePos())
                        : Math.max(0L, segment.finalLength() >= 0L
                                ? segment.finalLength() : segment.publishedWritePos());
            }
            assertEquals(expectedData, stats.totalLogBytes(),
                    "totalLogBytes 必须等于各段『数据字节』之和（活跃段=publishedWritePos，"
                            + "封存段=finalLength）");
            assertTrue(stats.totalLogBytes() < capacity,
                    "在段未写满时，数据字节必须严格小于容量之和——否则说明又在按容量统计"
                            + "（data=" + stats.totalLogBytes() + ", capacity=" + capacity + "）");
            // 每条帧 = 64B 帧头 + 256B payload（已 8 字节对齐，无 padding）
            long expectedByFrames = (long) records * (64L + PAYLOAD);
            assertEquals(expectedByFrames, stats.totalLogBytes(),
                    "单分区连续写入时，数据字节应等于『条数 × 帧长』");
        }
    }

    @Test
    @DisplayName("totalLogBytes 与保留策略的 sizeBytes 口径一致（同一份数据不许两个数字）")
    void totalLogBytesAgreesWithRetentionAccounting(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < 100; i++) {
                TestWal.append(wal, 0, PAYLOAD);
            }
            assertTrue(wal.flushAndWait(0, 100, 10_000L), "必须可刷盘");

            long fromStats = wal.stats().totalLogBytes();
            long fromSegments = 0L;
            for (Segment segment : wal.segmentManager().segments(0)) {
                fromSegments += segment.isActive()
                        ? segment.publishedWritePos()
                        : (segment.finalLength() >= 0L ? segment.finalLength() : segment.publishedWritePos());
            }
            assertEquals(fromSegments, fromStats,
                    "『看板日志总量』与『保留策略判定依据』必须是同一个数字，"
                            + "否则会出现『看板说 100MB 但保留策略按 300MB 开始删段』");
        }
    }

    @Test
    @DisplayName("totalRecords 与 publishedLsnMax 跟随写入推进")
    void recordsAndPublishedLsnTrackWrites(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 25; i++) {
                TestWal.append(wal, 0, PAYLOAD);
            }
            WalStats after = wal.stats();
            assertTrue(after.totalRecords() >= 25L,
                    "记录总数必须覆盖已写入条数，实际 = " + after.totalRecords());
            assertEquals(25L, after.publishedLsnMax(), "已发布的最大 LSN 应等于最后一条的 LSN");
            assertTrue(after.durableLsnMax() <= after.publishedLsnMax(),
                    "持久化水位永远不能超过已发布水位（否则就是假确认）");
        }
    }

    @Test
    @DisplayName("flushAndWait 之后 durableLsnMax 才推进到已写入值")
    void durableLsnAdvancesOnlyAfterFlush(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = TestWal.append(wal, 0, PAYLOAD);
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");
            assertEquals(lsn, wal.stats().durableLsnMax(),
                    "持久确认后 durableLsnMax 必须追到该 LSN");
        }
    }

    @Test
    @DisplayName("返回的 Map 是不可变视图：调用方改不动，也污染不了下一次快照")
    void returnedMapsAreImmutableSnapshots(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, 0, PAYLOAD);
            WalStats first = wal.stats();
            long appendRecordsBefore = first.counters()
                    .getOrDefault(MetricsRegistry.APPEND_RECORDS, -1L);

            assertThrows(UnsupportedOperationException.class,
                    () -> first.counters().put("injected", 1L),
                    "counters() 必须不可写，否则调用方可篡改库内观测状态");
            assertThrows(UnsupportedOperationException.class,
                    () -> first.gauges().clear(), "gauges() 必须不可变");
            assertThrows(UnsupportedOperationException.class,
                    () -> first.latencies().clear(), "latencies() 必须不可变");
            assertThrows(UnsupportedOperationException.class,
                    () -> first.activePlugins().put("x", "y"), "activePlugins() 必须不可变");

            WalStats second = wal.stats();
            assertFalse(second.gauges().isEmpty(), "取两次快照后 gauges 不得为空");
            assertFalse(second.latencies().isEmpty(), "取两次快照后 latencies 不得为空");
            assertEquals(appendRecordsBefore,
                    second.counters().getOrDefault(MetricsRegistry.APPEND_RECORDS, -1L),
                    "累计计数器不应被外部影响");
            // 每次 stats() 都是新对象、新 Map，不能是同一个可复用容器
            assertNotSame(first.counters(), second.counters(),
                    "两次 stats() 必须各自返回独立快照（否则并发读会互相覆盖）");
        }
    }

    @Test
    @DisplayName("latency(name) 对未知名字返回 null（不抛异常、不造零值）")
    void unknownLatencyReturnsNull(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            WalStats stats = wal.stats();
            assertNull(stats.latency("wal.no.such.metric"),
                    "未知指标应返回 null，让调用方明确知道『没采集到』而不是误读为 0ns");
            WalStats.LatencyView view = stats.latency(MetricsRegistry.APPEND_LATENCY);
            assertNotNull(view, "append 延迟直方图应当总在快照里");
            assertEquals(MetricsRegistry.APPEND_LATENCY, view.name());
            assertTrue(view.p50Nanos() >= 0L);
            assertTrue(view.p99Nanos() >= view.p50Nanos(), "p99 不应小于 p50");
            assertTrue(view.p999Nanos() >= view.p99Nanos(), "p999 不应小于 p99");
        }
    }

    @Test
    @DisplayName("gauges 里能直接读到日志字节/分区数/段数/健康度")
    void gaugesExposeOperationalNumbers(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, 0, PAYLOAD);
            WalStats stats = wal.stats();
            Map<String, Long> gauges = stats.gauges();
            assertEquals(stats.totalLogBytes(),
                    gauges.getOrDefault(MetricsRegistry.LOG_BYTES, -1L),
                    "wal.log.bytes gauge 必须与 totalLogBytes 同值");
            assertEquals((long) stats.activePartitions(),
                    gauges.getOrDefault(MetricsRegistry.PARTITIONS, -1L),
                    "wal.partitions gauge 必须与 activePartitions 同值");
            assertEquals(stats.segmentCount(),
                    gauges.getOrDefault(MetricsRegistry.SEGMENTS, -1L),
                    "wal.segments gauge 必须与 segmentCount 同值");
            assertEquals((long) stats.health().ordinal(),
                    gauges.getOrDefault(MetricsRegistry.HEALTH, -1L),
                    "wal.health gauge 用 ordinal 暴露，便于告警阈值（DEGRADED=1）");
        }
    }

    @Test
    @DisplayName("activePartitions 与配置分区数一致，segmentCount 与实际段文件数一致")
    void partitionAndSegmentCountsAreConsistent(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            WalStats stats = wal.stats();
            assertEquals(2, stats.activePartitions(), "TestWal 固定 2 个分区");
            long actual = wal.segmentManager().segments(0).size()
                    + wal.segmentManager().segments(1).size();
            assertEquals(actual, stats.segmentCount(), "segmentCount 必须等于实际段对象数");
        }
    }

    @Test
    @DisplayName("close 之后 stats() 仍可读（不能因为关闭就 NPE）")
    void statsRemainsReadableAfterClose(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        TestWal.append(wal, 0, PAYLOAD);
        wal.close();
        WalStats stats = wal.stats();
        assertNotNull(stats, "关闭后仍应能取到最后一帧快照（运维需要在关闭后核对水位）");
        assertTrue(stats.totalRecords() >= 1L, "关闭后记录数不应丢");
    }

    @Test
    @DisplayName("empty() 是全零快照，可作为默认值与降级占位")
    void emptyStatsIsUsableAsPlaceholder() {
        WalStats empty = WalStats.empty();
        assertEquals(Health.HEALTHY, empty.health());
        assertEquals(0L, empty.totalLogBytes());
        assertEquals(0L, empty.totalRecords());
        assertEquals(0, empty.activePartitions());
        assertNotNull(empty.activePlugins(), "map 字段不得为 null，调用方不该写空判断");
        assertNotNull(empty.counters());
        assertNotNull(empty.latencies());
        assertNotNull(empty.gauges());
        assertTrue(empty.toString().contains("logBytes=0"), "toString 应可安全打印: " + empty);
    }
}
