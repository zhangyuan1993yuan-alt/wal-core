package com.walcore.recovery;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.RecoveryReport;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.recovery.SegmentScanner;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * T10：三档 {@code CorruptionPolicy} 的处置语义（ARCH §9.3）。
 *
 * <h2>三档的"该做什么 / 绝不该做什么"</h2>
 * <table border="1">
 *   <caption>策略语义</caption>
 *   <tr><th>策略</th><th>尾段损坏</th><th>中间段损坏</th></tr>
 *   <tr><td>{@code TAIL_TRUNCATE}</td><td>截断到最近完整帧，继续启动</td>
 *       <td><b>抛异常</b>（截断会造出 LSN 空洞 = 已确认数据静默丢失）</td></tr>
 *   <tr><td>{@code SKIP_RECORD}</td><td>在窗口内重同步，跳过坏帧继续读</td><td>同上，抛异常</td></tr>
 *   <tr><td>{@code FAIL_FAST}</td><td><b>抛异常且一个字节都不改</b>（给人工取证留现场）</td>
 *       <td>抛异常</td></tr>
 * </table>
 *
 * <p>"中间段损坏必须硬失败"是这套策略里最重要的一条：它保护的不是启动成功率，
 * 而是<b>不会静默丢已经 ACK 过的数据</b>。
 */
class RecoveryCorruptionPolicyTest {

    private static final int RECORDS = 20;
    private static final int PAYLOAD = 16;
    private static final int FRAME = 80;
    private static final long SEGMENT_SIZE = 4096L;
    private static final long OFFSET_FRAME_10 = 10L * FRAME;

    // ==================================================================
    // FAIL_FAST
    // ==================================================================

    @Test
    @DisplayName("FAIL_FAST：拒绝启动，且文件字节一个都不改（保留取证现场）")
    void failFastRejectsAndLeavesFileUntouched(@TempDir Path dir) throws Exception {
        Path walFile = buildCrashedWal(dir);
        patch(walFile, 19L * FRAME, le32(0xDEADBEEF));
        byte[] before = Files.readAllBytes(walFile);

        WalException.CorruptedException error = assertThrows(
                WalException.CorruptedException.class,
                () -> openWith(dir, WalConfig.CorruptionPolicy.FAIL_FAST));
        assertTrue(error.getMessage().contains("FAIL_FAST"),
                "错误消息必须点明策略: " + error.getMessage());
        assertTrue(error.getMessage().contains("rule2.magic"),
                "错误消息必须带上命中的规则: " + error.getMessage());

        byte[] after = Files.readAllBytes(walFile);
        assertArrayEquals(before, after, "FAIL_FAST 绝不能修改文件内容");
    }

    @Test
    @DisplayName("TAIL_TRUNCATE：同一份损坏可正常启动，且物理截断真的发生")
    void tailTruncateRepairsPhysically(@TempDir Path dir) throws Exception {
        Path walFile = buildCrashedWal(dir);
        patch(walFile, 19L * FRAME, le32(0xDEADBEEF));
        long before = Files.size(walFile);

        try (DefaultWal wal = openWith(dir, WalConfig.CorruptionPolicy.TAIL_TRUNCATE)) {
            assertEquals(19, TestWal.readAllLsns(wal, 0).size());
            assertEquals(before - 19L * FRAME, wal.openReport().truncatedBytes());
            assertEquals(19L * FRAME, Files.size(walFile),
                    "尾部垃圾必须被物理删除，否则下次重启会再判成损坏 → 变成中间段损坏 → 起不来");
        }
    }

    // ==================================================================
    // 中间段损坏：任何策略都必须硬失败
    // ==================================================================

    @Test
    @DisplayName("TAIL_TRUNCATE：中间段损坏仍然硬失败（绝不静默截断）")
    void middleSegmentCorruptionFailsWithTailTruncate(@TempDir Path dir) throws Exception {
        Path middle = buildCrashedMultiSegmentWal(dir);
        patch(middle, FRAME, le32(0xDEADBEEF));

        WalException.CorruptedException error = assertThrows(
                WalException.CorruptedException.class,
                () -> openWith(dir, WalConfig.CorruptionPolicy.TAIL_TRUNCATE));
        assertTrue(error.getMessage().contains("非最后段"),
                "错误消息必须说明是中间段损坏: " + error.getMessage());
    }

    @Test
    @DisplayName("SKIP_RECORD：中间段损坏同样硬失败（重同步会隐藏数据丢失）")
    void middleSegmentCorruptionFailsWithSkipRecord(@TempDir Path dir) throws Exception {
        Path middle = buildCrashedMultiSegmentWal(dir);
        patch(middle, FRAME, le32(0xDEADBEEF));

        assertThrows(WalException.CorruptedException.class,
                () -> openWith(dir, WalConfig.CorruptionPolicy.SKIP_RECORD));
    }

    @Test
    @DisplayName("FAIL_FAST：中间段损坏同样硬失败")
    void middleSegmentCorruptionFailsWithFailFast(@TempDir Path dir) throws Exception {
        Path middle = buildCrashedMultiSegmentWal(dir);
        patch(middle, FRAME, le32(0xDEADBEEF));

        assertThrows(WalException.CorruptedException.class,
                () -> openWith(dir, WalConfig.CorruptionPolicy.FAIL_FAST));
    }

    // ==================================================================
    // SKIP_RECORD 的重同步（直接在 SegmentScanner 上验证）
    // ==================================================================

    @Test
    @DisplayName("SegmentScanner：不重同步时坏帧处立即停止")
    void scannerStopsAtDamageWithoutResync(@TempDir Path dir) throws Exception {
        DefaultWal wal = runnableWal(dir);
        try {
            patch(segmentFile(dir), OFFSET_FRAME_10, le32(0x11223344));

            SegmentScanner.ScanResult scan = scanFirstSegment(wal, 0);
            assertTrue(scan.stopped, "无重同步窗口时必须在坏帧处停止");
            assertEquals(OFFSET_FRAME_10, scan.lastGoodOffset);
            assertEquals(10L, scan.frameCount);
            assertEquals("rule2.magic", scan.stopRule);
            assertEquals(0L, scan.resyncCount);
        } finally {
            wal.crashStopForTesting();
        }
    }

    @Test
    @DisplayName("SegmentScanner：SKIP_RECORD 重同步后继续读完剩余帧")
    void scannerResyncsPastDamage(@TempDir Path dir) throws Exception {
        DefaultWal wal = runnableWal(dir);
        try {
            patch(segmentFile(dir), OFFSET_FRAME_10, le32(0x11223344));

            SegmentScanner.ScanResult scan = scanFirstSegment(wal, 4096);
            assertFalse(scan.stopped, "窗口足够大时必须靠重同步越过坏帧");
            assertEquals(1L, scan.resyncCount, "应恰好重同步一次");
            assertEquals(FRAME, scan.skippedBytes, "跳过的字节数应恰好是一帧");
            assertEquals(19L, scan.frameCount, "20 帧里坏 1 帧 → 读到 19 帧");
            assertEquals((long) RECORDS * FRAME, scan.lastGoodOffset,
                    "重同步后必须一路读到数据区末尾");
            assertEquals(20L, scan.lastLsn, "最后一条完整记录的 LSN 必须是 20");
        } finally {
            wal.crashStopForTesting();
        }
    }

    @Test
    @DisplayName("SegmentScanner：窗口内找不到合法帧则放弃重同步并停止")
    void scannerGivesUpWhenResyncWindowTooSmall(@TempDir Path dir) throws Exception {
        DefaultWal wal = runnableWal(dir);
        try {
            patch(segmentFile(dir), OFFSET_FRAME_10, le32(0x11223344));

            // 窗口 8 字节 < 到下一帧帧首的距离 80 字节
            SegmentScanner.ScanResult scan = scanFirstSegment(wal, 8);
            assertTrue(scan.stopped, "窗口太小必须放弃重同步");
            assertEquals(OFFSET_FRAME_10, scan.lastGoodOffset);
            assertEquals(10L, scan.frameCount);
        } finally {
            wal.crashStopForTesting();
        }
    }

    @Test
    @DisplayName("SKIP_RECORD 策略：报告里有 corruption 事件，且绝不回收已用过的 LSN")
    void skipRecordPolicyReportsCorruptionAndKeepsLsnSpace(@TempDir Path dir) throws Exception {
        Path walFile = buildCrashedWal(dir);
        patch(walFile, OFFSET_FRAME_10, le32(0x11223344));

        try (DefaultWal recovered = openWith(dir, WalConfig.CorruptionPolicy.SKIP_RECORD)) {
            RecoveryReport report = recovered.openReport();
            assertFalse(report.corruptions().isEmpty(),
                    "SKIP_RECORD 跳过坏帧时必须留下 corruption 事件");
            RecoveryReport.CorruptionEvent event = report.corruptions().get(0);
            assertEquals(0, event.partitionId());
            assertEquals(OFFSET_FRAME_10, event.fileOffset());
            assertEquals(FRAME, event.skippedBytes());
            assertTrue(event.rule().startsWith("rule2"), "规则标识不符: " + event.rule());

            assertEquals(21L, report.nextLsn(),
                    "跳过一帧也不能回收它的 LSN（否则会重发已用过的 LSN，破坏下游幂等）");
            assertEquals(21L, recovered.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS),
                    "续写必须拿到 21");
        }
    }

    // ==================================================================
    // 夹具
    // ==================================================================

    private static DefaultWal openWith(Path dir, WalConfig.CorruptionPolicy policy) {
        return TestWal.open(dir, SEGMENT_SIZE, b -> b.corruptionPolicy(policy));
    }

    /**
     * 造一个崩溃态（已 crashStop）的单段 WAL，返回段文件路径。
     *
     * @param dir 目录
     * @return 段文件
     * @throws Exception 夹具失败
     */
    private static Path buildCrashedWal(Path dir) throws Exception {
        DefaultWal wal = runnableWal(dir);
        wal.crashStopForTesting();
        return segmentFile(dir);
    }

    /**
     * 造一个"数据已持久化但实例仍打开"的 WAL（供直接扫描用；调用方负责 crashStop）。
     *
     * @param dir 目录
     * @return 实例
     * @throws Exception 夹具失败
     */
    private static DefaultWal runnableWal(Path dir) throws Exception {
        DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        });
        long last = 0L;
        for (int i = 0; i < RECORDS; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 10_000L));
        return wal;
    }

    /**
     * 造一个多段分区（已 crashStop），返回<b>中间段</b>文件路径。
     *
     * @param dir 目录
     * @return 中间段
     * @throws Exception 夹具失败
     */
    private static Path buildCrashedMultiSegmentWal(Path dir) throws Exception {
        DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        });
        long last = 0L;
        for (int i = 0; i < 30; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 10_000L));
        wal.crashStopForTesting();

        List<Path> files = segmentFiles(dir);
        assertTrue(files.size() >= 3, "夹具要求至少 3 个段，实际 = " + files);
        return files.get(1);
    }

    private static List<Path> segmentFiles(Path dir) throws Exception {
        Path pdir = dir.resolve("partitions").resolve("p0000");
        try (var stream = Files.list(pdir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".wal"))
                    .sorted().toList();
        }
    }

    private static Path segmentFile(Path dir) throws Exception {
        return segmentFiles(dir).get(0);
    }

    private static SegmentScanner.ScanResult scanFirstSegment(DefaultWal wal, int resyncWindow)
            throws Exception {
        SegmentScanner scanner = new SegmentScanner();
        var segment = wal.segmentManager().segments(0).get(0);
        // 注意：这里必须用<b>逻辑长度</b>（appendPosition）而不是物理长度。
        // 活跃段的句柄只暴露 [0, appendPosition)，拿 Files.size() 去扫会读到
        // "句柄拒绝供数"的区域，扫描器会报"读取不足"。
        return scanner.scan(segment.handle(), 0L, segment.handle().appendPosition(),
                true, resyncWindow, null, null);
    }

    private static void patch(Path file, long offset, byte[] bytes) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            raf.write(bytes);
            raf.getFD().sync();
        }
    }

    private static byte[] le32(int value) {
        return new byte[]{
                (byte) (value & 0xFF), (byte) ((value >>> 8) & 0xFF),
                (byte) ((value >>> 16) & 0xFF), (byte) ((value >>> 24) & 0xFF)};
    }
}
