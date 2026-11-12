package com.walcore.snapshot;

import com.walcore.api.AppendOptions;
import com.walcore.api.ReplayListener;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.api.RecoveryReport;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.retention.RetentionManager;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.snapshot.CheckpointManager;
import com.walcore.internal.snapshot.FileSnapshotStore;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.PluginContext;
import com.walcore.spi.SegmentInfo;
import com.walcore.spi.SnapshotMeta;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T15：快照与 checkpoint 联动的验收测试。
 *
 * <p>覆盖 TASKS T15 的六个验收点：快照后删段不影响读取、重放量下降 ≥80%、
 * SHA-256 校验失败拒绝加载、{@code pinnedLsn} 与保留策略联动、原子写、{@code list/deleteBefore} 正确。
 */
@DisplayName("快照与 checkpoint")
class SnapshotTest {

    private static final long SEGMENT_SIZE = 4096L;
    /** 4096B 段 / 80B 帧 ≈ 51 条。 */
    private static final int RECORDS_PER_SEGMENT = 50;

    // ==================================================================
    // 基本读写
    // ==================================================================

    @Test
    @DisplayName("写入后可取回：元数据正确、内容字节一致、SHA-256 为 64 位 hex")
    void writeThenReadRoundTrip(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            FileSnapshotStore store = (FileSnapshotStore) wal.snapshotStore();
            assertNotNull(store, "必须装配了快照存储");

            byte[] state = "订单簿快照-v1".getBytes(StandardCharsets.UTF_8);
            SnapshotMeta meta = store.write(0, 123L, state);

            assertEquals(0, meta.partitionId());
            assertEquals(123L, meta.uptoLsn());
            assertEquals(state.length, meta.stateBytes());
            assertEquals(64, meta.checksumHex().length(), "SHA-256 必须是 64 位 hex");
            assertNotNull(meta.path());
            assertTrue(Files.isRegularFile(meta.path()), "快照文件必须真的落盘");

            assertArrayEqualsBytes(state, store.readAll(meta));
            Optional<SnapshotMeta> latest = store.latest(0);
            assertTrue(latest.isPresent());
            assertEquals(123L, latest.get().uptoLsn());
            assertEquals(123L, store.pinnedLsn(0), "pinnedLsn = 最近快照的 uptoLsn");
        }
    }

    @Test
    @DisplayName("list() 按 uptoLsn 升序；deleteBefore 精确删除边界之前的快照")
    void listIsOrderedAndDeleteBeforeIsExact(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            FileSnapshotStore store = (FileSnapshotStore) wal.snapshotStore();
            for (long lsn : new long[]{300L, 100L, 200L}) {
                store.write(0, lsn, ("s" + lsn).getBytes(StandardCharsets.UTF_8));
            }
            store.write(1, 50L, new byte[]{1});

            List<SnapshotMeta> list = store.list(0);
            assertEquals(3, list.size());
            assertEquals(100L, list.get(0).uptoLsn());
            assertEquals(200L, list.get(1).uptoLsn());
            assertEquals(300L, list.get(2).uptoLsn(), "必须按 uptoLsn 升序");
            assertEquals(1, store.list(1).size(), "分区必须隔离");

            store.deleteBefore(0, 200L);
            List<SnapshotMeta> after = store.list(0);
            assertEquals(2, after.size(), "只删 < 200 的: " + after);
            assertEquals(200L, after.get(0).uptoLsn(), "边界本身必须保留");
            assertEquals(1, store.list(1).size(), "不得误删其它分区");
        }
    }

    // ==================================================================
    // SHA-256
    // ==================================================================

    @Test
    @DisplayName("内容被篡改后拒绝加载，异常消息含期望与实际摘要")
    void tamperedContentIsRejected(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            FileSnapshotStore store = (FileSnapshotStore) wal.snapshotStore();
            byte[] state = new byte[256];
            for (int i = 0; i < state.length; i++) {
                state[i] = (byte) i;
            }
            SnapshotMeta meta = store.write(0, 10L, state);

            // 翻转内容区的一个字节（头部之后）
            byte[] raw = Files.readAllBytes(meta.path());
            raw[raw.length - 5] ^= 0x7F;
            Files.write(meta.path(), raw);

            WalException.CorruptedException error = assertThrows(
                    WalException.CorruptedException.class, () -> store.open(meta));
            String message = error.getMessage();
            assertTrue(message.contains("SHA-256"), "消息必须点明是摘要校验失败: " + message);
            assertTrue(message.contains(meta.checksumHex()),
                    "消息必须含期望摘要: " + message);
            assertTrue(message.contains("实际"), "消息必须含实际摘要: " + message);
        }
    }

    @Test
    @DisplayName("写入用 .tmp + ATOMIC_MOVE：目录里不留半截快照")
    void noPartialSnapshotLeftBehind(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            FileSnapshotStore store = (FileSnapshotStore) wal.snapshotStore();
            for (int i = 1; i <= 5; i++) {
                store.write(0, i * 10L, ("payload-" + i).getBytes(StandardCharsets.UTF_8));
            }
            try (Stream<Path> files = Files.list(store.rootDir())) {
                long tmp = files.map(p -> p.getFileName().toString())
                        .filter(n -> n.endsWith(".tmp"))
                        .count();
                assertEquals(0L, tmp, "不得残留 .tmp");
            }
            assertEquals(5, store.list(0).size());
        }
    }

    // ==================================================================
    // 与保留策略联动 / 删段后仍可读
    // ==================================================================

    @Test
    @DisplayName("快照后删除快照点之前的段：lsn >= 快照点的读取完全不受影响")
    void deletingSegmentsBeforeSnapshotDoesNotAffectReads(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeSegments(wal, 0, 10);
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            List<Segment> segments = wal.segmentManager().segments(0);
            assertTrue(segments.size() >= 8, "应当产生 >=8 个段，实际 " + segments.size());
            Segment snapshotSegment = segments.get(segments.size() - 2);
            long snapshotLsn = snapshotSegment.firstLsn();

            SnapshotMeta meta = wal.checkpointManager().checkpoint(0, snapshotLsn,
                    "state".getBytes(StandardCharsets.UTF_8));
            assertEquals(snapshotLsn, meta.uptoLsn());

            // 用一个"想删掉除最后一段外全部段"的激进策略，但 pin 源指向快照点
            RetentionManager manager = new RetentionManager(wal.segmentManager(), wal.registry(),
                    new AggressivePolicy(), wal.config(), null,
                    wal.checkpointManager()::pinnedLsn);
            RetentionManager.RunResult result = manager.runPartition(0, false);

            assertTrue(result.deleted() > 0, "快照点之前的段应当被删: " + result);
            List<Long> remaining = new ArrayList<>();
            for (Segment s : wal.segmentManager().segments(0)) {
                remaining.add(s.firstLsn());
            }
            assertTrue(remaining.contains(snapshotSegment.firstLsn()),
                    "快照所在段绝不能被删，剩余 = " + remaining);

            // 从快照点之后读：必须一条不少、且连续
            List<Long> lsnList = readLsns(wal, 0, snapshotLsn);
            assertEquals(last - snapshotLsn + 1L, lsnList.size(),
                    "快照点之后的记录必须全部可读");
            for (int i = 0; i < lsnList.size(); i++) {
                assertEquals(snapshotLsn + i, lsnList.get(i).longValue());
            }
        }
    }

    @Test
    @DisplayName("恢复重放量下降 >= 80%：10 段日志、快照落在第 9 段首")
    void replayAfterSnapshotIsAtLeast80PercentSmaller(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeSegments(wal, 0, 10);
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            List<Segment> segments = wal.segmentManager().segments(0);
            Segment ninth = segments.get(8);
            CheckpointManager checkpoints = wal.checkpointManager();
            checkpoints.checkpoint(0, ninth.firstLsn(), "state".getBytes(StandardCharsets.UTF_8));

            CheckpointManager.ReplayResult full = checkpoints.replayFull(0, null);
            CheckpointManager.ReplayResult incremental = checkpoints.replaySinceSnapshot(0, null);

            assertEquals(last, full.replayed(), "全量重放应覆盖全部记录");
            assertTrue(incremental.replayed() < full.replayed(),
                    "增量重放必须更少: full=" + full.replayed()
                            + ", incremental=" + incremental.replayed());
            double ratio = 1.0d - (double) incremental.replayed() / (double) full.replayed();
            assertTrue(ratio >= 0.80d,
                    "重放量必须减少 >=80%，实际减少 " + String.format("%.1f%%", ratio * 100)
                            + "（full=" + full.replayed()
                            + ", incremental=" + incremental.replayed() + "）");
            // 快照语义是"状态已包含 lsn <= uptoLsn 的全部记录"，所以重放起点必须是 uptoLsn + 1，
            // 而不是 uptoLsn 本身——后者会把快照已覆盖的那一条再放一遍（重复投递）。
            assertEquals(ninth.firstLsn() + 1L, incremental.fromLsn(),
                    "增量重放的起点必须是快照点 + 1（不重复重放快照已覆盖的那条）");
            assertEquals(ninth.firstLsn() + 1L, checkpoints.resumeLsn(0),
                    "resumeLsn 与 replaySinceSnapshot 的起点必须一致");
        }
    }

    @Test
    @DisplayName("pinnedLsn 在快照点之后阻止保留策略删除（否则数据永久丢失）")
    void pinnedLsnProtectsSegmentsAfterSnapshot(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeSegments(wal, 0, 8);
            assertTrue(wal.flushAndWait(0, last, 10_000L));

            List<Segment> segments = wal.segmentManager().segments(0);
            long snapshotLsn = segments.get(segments.size() - 3).firstLsn();
            wal.checkpointManager().checkpoint(0, snapshotLsn,
                    "s".getBytes(StandardCharsets.UTF_8));

            RetentionManager manager = new RetentionManager(wal.segmentManager(), wal.registry(),
                    new AggressivePolicy(), wal.config(), null,
                    wal.checkpointManager()::pinnedLsn);
            manager.runPartition(0, false);

            for (Segment s : wal.segmentManager().segments(0)) {
                assertTrue(s.lastLsn() < snapshotLsn || s.firstLsn() >= snapshotLsn,
                        "被删的段必须是完整早于快照点的段: " + s);
            }
            // 快照点之后的记录必须一个不少
            assertEquals(last - snapshotLsn + 1L, readLsns(wal, 0, snapshotLsn).size());
        }
    }

    // ==================================================================
    // 重放回调
    // ==================================================================

    @Test
    @DisplayName("replaySinceSnapshot 按 LSN 升序回调，条数与 lastLsn 正确")
    void replayCallbackIsOrdered(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeSegments(wal, 0, 4);
            assertTrue(wal.flushAndWait(0, last, 10_000L));
            long from = last / 2L + 1L;
            wal.checkpointManager().checkpoint(0, from - 1L, new byte[]{7});

            RecordingListener listener = new RecordingListener();
            CheckpointManager.ReplayResult result =
                    wal.checkpointManager().replaySinceSnapshot(0, listener);

            assertEquals(from, result.fromLsn());
            assertEquals(last, result.lastLsn());
            assertEquals(last - from + 1L, result.replayed());
            assertEquals(result.replayed(), listener.lsns.size());
            for (int i = 0; i < listener.lsns.size(); i++) {
                assertEquals(from + i, listener.lsns.get(i).longValue(),
                        "重放必须严格按 LSN 升序");
            }
            assertTrue(listener.progressCalls > 0, "必须至少回调一次 onProgress");
        }
    }

    @Test
    @DisplayName("没有快照时 resumeLsn 回退到 1，pinnedLsn 为 0")
    void withoutSnapshotDefaultsAreSafe(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            assertEquals(1L, wal.checkpointManager().resumeLsn(0));
            assertEquals(0L, wal.checkpointManager().pinnedLsn(0));
            assertTrue(wal.snapshotStore().latest(0).isEmpty());
            assertFalse(wal.checkpointManager().snapshots(0).iterator().hasNext());
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 写入 {@code segmentCount} 段量的记录。
     *
     * @param wal 实例
     * @param pid 分区
     * @param segmentCount 段数
     * @return 最后一条 LSN（写入前已刷盘由调用方负责）
     */
    private static long writeSegments(DefaultWal wal, int pid, int segmentCount) {
        long last = 0L;
        for (int i = 0; i < segmentCount * RECORDS_PER_SEGMENT; i++) {
            last = wal.append(pid, TestWal.payload(16), AppendOptions.DEFAULTS);
        }
        return last;
    }

    /**
     * 从指定 LSN 起读回 LSN 列表。
     *
     * @param wal 实例
     * @param pid 分区
     * @param fromLsn 起始 LSN
     * @return 列表
     */
    private static List<Long> readLsns(DefaultWal wal, int pid, long fromLsn) {
        List<Long> out = new ArrayList<>();
        try (com.walcore.api.WalCursor cursor = wal.cursor(pid,
                com.walcore.api.CursorOptions.builder().fromLsn(fromLsn).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.lsn());
            }
        }
        return out;
    }

    /**
     * 断言两个字节数组相等（避免引入 assertArrayEquals 的静态导入歧义）。
     *
     * @param expected 期望
     * @param actual 实际
     */
    private static void assertArrayEqualsBytes(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], "第 " + i + " 字节不符");
        }
    }

    /**
     * 激进策略：想删掉除最后一段之外的全部段（完全无视 pin，用于验证框架层兜底）。
     */
    private static final class AggressivePolicy implements com.walcore.spi.RetentionPolicy {

        @Override
        public String id() {
            return "test:snapshot-aggressive";
        }

        @Override
        public int apiVersion() {
            return com.walcore.api.WalVersion.API_VERSION;
        }

        @Override
        public void init(PluginContext context) {
            // 无状态
        }

        @Override
        public RetentionDecision evaluate(RetentionContext context) {
            List<SegmentInfo> all = context.segments();
            if (all.size() <= 1) {
                return RetentionDecision.keepAll();
            }
            return RetentionDecision.delete(new ArrayList<>(all.subList(0, all.size() - 1)),
                    "aggressive");
        }

        @Override
        public void close() {
            // 无外部资源
        }
    }

    /**
     * 记录重放调用的监听器。
     */
    private static final class RecordingListener implements ReplayListener {

        private final List<Long> lsns = new ArrayList<>();
        private int progressCalls;

        @Override
        public void onRecord(WalRecord record) {
            lsns.add(record.lsn());
        }

        @Override
        public void onProgress(int partitionId, long lsn, long replayed) {
            progressCalls++;
        }

        @Override
        public void onCorruption(com.walcore.api.RecoveryReport.CorruptionEvent event) {
            // 本测试不涉及损坏
        }

        @Override
        public void onRecoveryComplete(RecoveryReport report) {
            // 本测试不涉及完整恢复
        }
    }

    /**
     * 占位：确保 {@link ByteArrayInputStream} 与 {@link IOException} 的导入被使用（编译器友好）。
     *
     * @return 恒为 true
     */
    @SuppressWarnings("unused")
    private static boolean selfCheck() throws IOException {
        return new ByteArrayInputStream(new byte[0]).read() < 0;
    }
}
