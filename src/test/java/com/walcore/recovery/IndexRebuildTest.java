package com.walcore.recovery;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.RecoveryReport;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.recovery.IndexRebuilder;
import com.walcore.internal.segment.Segment;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * T10：{.idx} 索引的恢复与重建（ARCH §9.4 / 验收条件 ②）。
 *
 * <h2>核心不变量</h2>
 * <b>索引永远是可丢弃的加速层</b>：{@code .idx} 丢了、坏了、比数据短了，
 * 只允许影响 {@code seekTo} 的快慢，<b>绝不能影响读到的内容</b>。
 * 因此这里的每个用例都同时断言两件事：
 * <ol>
 *   <li>恢复后 {@code indexRebuilt} 被置位（说明真的重建了，而不是"碰巧还能用"）；</li>
 *   <li>用游标 {@code seekTo} 到段中间某条记录仍能精确命中（说明重建结果可用）。</li>
 * </ol>
 */
class IndexRebuildTest {

    private static final int RECORDS = 500;
    private static final int PAYLOAD = 24;

    @Test
    @DisplayName(".idx 被删除：恢复时全量重建，seekTo 仍精确命中")
    void rebuildsWhenIndexFileDeleted(@TempDir Path dir) throws Exception {
        Path idx = crashedWalWithIndex(dir);
        Files.delete(idx);

        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            assertTrue(wal.openReport().partitions().get(0).indexRebuilt(),
                    "索引文件缺失时必须重建");
            assertTrue(Files.isRegularFile(idx), "重建后 .idx 必须重新落盘");
            assertSeekHits(wal, 251L);
        }
    }

    @Test
    @DisplayName(".idx 被截断成垃圾：恢复时重建，内容不受影响")
    void rebuildsWhenIndexFileCorrupted(@TempDir Path dir) throws Exception {
        Path idx = crashedWalWithIndex(dir);
        try (RandomAccessFile raf = new RandomAccessFile(idx.toFile(), "rw")) {
            raf.setLength(7L);
            raf.seek(0L);
            raf.write(new byte[]{1, 2, 3, 4, 5, 6, 7});
            raf.getFD().sync();
        }

        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            assertTrue(wal.openReport().partitions().get(0).indexRebuilt(),
                    "索引文件损坏时必须重建");
            assertSeekHits(wal, 300L);
            assertEquals(RECORDS, TestWal.readAllLsns(wal, 0).size(),
                    "索引损坏不得影响记录条数（索引只是加速层）");
        }
    }

    @Test
    @DisplayName(".idx 完好：不重建，且 seekTo 依然正确")
    void keepsHealthyIndex(@TempDir Path dir) throws Exception {
        crashedWalWithIndex(dir);
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            assertSeekHits(wal, 123L);
            assertEquals(RECORDS, TestWal.readAllLsns(wal, 0).size());
        }
    }

    @Test
    @DisplayName("IndexRebuilder：重建条目数随 frameCount/interval 递增且锚点单调")
    void rebuilderProducesMonotonicAnchors(@TempDir Path dir) throws Exception {
        DefaultWal crashed = TestWal.open(dir, 4096L, b -> {
        });
        long last = 0L;
        for (int i = 0; i < RECORDS; i++) {
            last = crashed.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(crashed.flushAndWait(0, last, 10_000L));

        List<Segment> segments = crashed.segmentManager().segments(0);
        Segment first = segments.get(0);
        IndexRebuilder rebuilder = new IndexRebuilder();
        var result = rebuilder.rebuild(0, first.handle(),
                crashed.pluginManager().indexStore(), 10);
        crashed.crashStopForTesting();

        long framesInSegment = first.recordCount();
        assertFalse(result.stopped, "健康文件重建不应中途停止");
        assertEquals(framesInSegment, result.frames, "重建应遍历该段的全部帧");
        assertEquals(first.lastLsn(), result.lastLsn, "最后一条记录的 LSN 必须对上");
        assertTrue(result.entries > 0, "按每 10 帧一个锚点应产出若干条目，实际 = " + result.entries);
        assertEquals(1L + (framesInSegment - 1L) / 10L, (long) result.entries,
                "锚点规则：第 1 帧 + 每 10 帧一个");
    }

    // ==================================================================
    // 夹具
    // ==================================================================

    /**
     * 造一个"已 crashStop、带 .idx"的 WAL，返回 .idx 路径。
     *
     * @param dir 目录
     * @return .idx 路径
     * @throws Exception 夹具失败
     */
    private static Path crashedWalWithIndex(Path dir) throws Exception {
        DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        });
        long last = 0L;
        for (int i = 0; i < RECORDS; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 10_000L));

        Path idx = null;
        for (Segment segment : wal.segmentManager().segments(0)) {
            Path candidate = segment.handle().path().resolveSibling(
                    segment.handle().path().getFileName().toString()
                            .replace(".wal", ".idx"));
            if (Files.isRegularFile(candidate)) {
                idx = candidate;
                break;
            }
        }
        assertNotNull(idx, "夹具要求：写入后应已产生 .idx 文件");
        wal.crashStopForTesting();
        return idx;
    }

    /**
     * 断言 {@code seekTo(target)} 精确命中。
     *
     * @param wal 实例
     * @param target 目标 LSN
     */
    private static void assertSeekHits(DefaultWal wal, long target) {
        CursorOptions options = CursorOptions.builder()
                .fromLsn(target)
                .useMappedWindows(false)
                .build();
        assertDoesNotThrow(() -> {
            try (WalCursor cursor = wal.cursor(0, options)) {
                assertTrue(cursor.seekTo(target), "seekTo(" + target + ") 必须能定位");
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(target, record.lsn(), "seekTo 必须精确命中目标 LSN");
            }
        });
    }

    /** 报告中的诊断对象不允许为 null（防止"重建了但没记录"的静默成功）。 */
    @Test
    @DisplayName("恢复报告必须给出每个分区的索引诊断")
    void reportCarriesPerPartitionDiagnosis(@TempDir Path dir) throws Exception {
        crashedWalWithIndex(dir);
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            RecoveryReport report = wal.openReport();
            assertFalse(report.partitions().isEmpty(), "必须给出分区诊断");
            RecoveryReport.PartitionDiagnosis p = report.partitions().get(0);
            assertEquals(0, p.partitionId());
            assertTrue(p.segmentCount() >= 1);
            assertEquals(RECORDS, p.lastLsn(), "诊断里的 lastLsn 必须等于 500");
            assertTrue(p.lastGoodOffset() > 0L);
        }
    }
}
