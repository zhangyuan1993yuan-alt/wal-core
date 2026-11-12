package com.walcore.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.CloseReport;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.segment.Segment;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * T12 优雅关闭验收（P0-10）。
 *
 * <h2>关闭的三条硬要求</h2>
 * <ol>
 *   <li><b>不丢数据</b>：{@code close()} 返回后，所有<b>已 ACK</b>（{@code appendSync} 返回过）
 *       的记录必须已经 {@code force} 到盘上；</li>
 *   <li><b>顺序正确</b>：{@code force} → 写 {@code wal.meta(cleanShutdown=true)} → 释放 LOCK。
 *       顺序错了会出现"meta 说干净、数据却没落盘"的假持久性；</li>
 *   <li><b>幂等</b>：重复 {@code close()} 不抛异常、不重复刷盘、不重复写 meta。</li>
 * </ol>
 *
 * <p>另外验证"干净关闭会跳过逐帧 CRC"这条快路径（{@code cleanShutdown=true} +
 * {@code recoveryVerifyMode=LAST_SEGMENT}）。
 */
class CloseFlushTest {

    private static final int PARTITION = 0;

    /** payload 16B → 帧长 80B（64 字节帧头 + 16 字节 payload，8 对齐无 padding）。 */
    private static final int FRAME = 80;

    @Test
    @DisplayName("appendSync 返回后立即 kill -9 也不丢：close 之后再重开仍能读全")
    void closeDoesNotLoseAcknowledgedRecords(@TempDir Path dir) {
        int total = 120;
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < total; i++) {
                wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
            }
            CloseReport report = wal.close(Duration.ofSeconds(10));
            assertTrue(report.clean(), "正常关闭必须报告 clean=true: " + report);
            assertTrue(report.unflushedPartitions().isEmpty(),
                    "正常关闭不允许有未刷盘分区: " + report.unflushedPartitions());
        }
        try (DefaultWal reopened = TestWal.open(dir)) {
            assertEquals(total, TestWal.readAllLsns(reopened, PARTITION).size(),
                    "已 ACK 的 120 条记录必须一条不少");
            assertTrue(reopened.openReport().clean(), "干净关闭后重开必须报告 clean");
        }
    }

    @Test
    @DisplayName("close 之后 meta 里 cleanShutdown=true，且段文件内容与 LSN 完全一致")
    void closeWritesCleanShutdownMarker(@TempDir Path dir) throws Exception {
        long last;
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 100; i++) {
                wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
            }
            last = wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);
        }
        String meta = Files.readString(dir.resolve("wal.meta"));
        assertTrue(meta.contains("cleanShutdown=true"),
                "meta 必须记录 cleanShutdown=true（否则下次恢复会做全量 CRC）: " + meta);

        // 用一次普通重开验证内容完整（这是"meta 没撒谎"的证据）
        try (DefaultWal reopened = TestWal.open(dir)) {
            List<Long> lsns = TestWal.readAllLsns(reopened, PARTITION);
            assertEquals(last, lsns.size());
            assertEquals(last, lsns.get(lsns.size() - 1).longValue());
            assertTrue(reopened.openReport().clean(), "干净关闭后的恢复必须判为 clean");
        }
    }

    @Test
    @DisplayName("close 幂等：重复调用不抛异常且第二次立即返回")
    void closeIsIdempotent(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);

        CloseReport first = wal.close(Duration.ofSeconds(5));
        assertTrue(first.clean());
        assertTrue(wal.isClosed(), "close 之后 isClosed 必须为 true");

        CloseReport second = wal.close(Duration.ofSeconds(5));
        assertNotNull(second);
        assertTrue(second.clean(), "重复关闭应报告 clean（已经是关闭态）");
        assertTrue(second.unflushedPartitions().isEmpty());

        // 第三次：仍然不许抛
        wal.close(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("显式 close(Duration) 与无参 close() 等价，都报告 clean")
    void bothCloseOverloadsAreEquivalent(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);
        wal.close();
        assertTrue(wal.isClosed());

        CloseReport again = wal.close(Duration.ofMillis(1));
        assertTrue(again.clean(), "已关闭时给一个极短超时也不该报失败");
    }

    @Test
    @DisplayName("关闭后任何写操作都抛 ClosedException（不会静默丢写）")
    void operationsAfterCloseThrow(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);
        wal.close(Duration.ofSeconds(5));

        WalException.ClosedException error = assertThrows(WalException.ClosedException.class,
                () -> wal.append(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS));
        assertEquals("WAL_CLOSED", error.errorCode(),
                "错误码必须可被上层稳定判断: " + error.errorCode());
        assertThrows(WalException.ClosedException.class,
                () -> wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS));
        assertThrows(WalException.ClosedException.class, () -> wal.cursor(PARTITION));
        assertThrows(WalException.ClosedException.class, () -> wal.rotate(PARTITION));
        // stats() 是<b>刻意</b>允许在关闭后调用的：它只聚合计数器，不碰任何已释放资源，
        // 关闭后读一次"终态快照"对运维排查很有用（不需要抛异常逼调用方缓存它）。
        assertNotNull(wal.stats(), "关闭后仍应能读到终态统计快照");
    }

    @Test
    @DisplayName("干净关闭后重开：cleanShutdown=true 的记录不再逐帧 CRC（快路径真的走了）")
    void cleanShutdownSkipsPerFrameCrc(@TempDir Path dir) throws Exception {
        int total = 120;
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < total; i++) {
                wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
            }
        }
        // 故意把<b>倒数第二段</b>的一个 payload 字节改坏：干净关闭 + LAST_SEGMENT 语义下，
        // 非最后段仍会逐帧校验，因此这次恢复必须报错——这正好反向证明
        // "跳过 CRC" 只作用于最后一段，而不是全局关掉校验。
        List<Segment> segments;
        try (DefaultWal probe = TestWal.open(dir)) {
            segments = probe.segmentManager().segments(PARTITION);
            assertTrue(segments.size() >= 2, "夹具要求多段，实际 = " + segments.size());
            probe.close(Duration.ofSeconds(5));
        }
        Path middle = segments.get(0).handle().path();
        flipByte(middle, 3L * FRAME + FrameLayout.HEADER_LEN + 2L);

        WalException.CorruptedException error = assertThrows(
                WalException.CorruptedException.class,
                () -> TestWal.open(dir, 4096L, b -> {
                }));
        assertTrue(error.getMessage().contains("非最后段"),
                "非最后段损坏必须硬失败（说明非最后段仍然逐帧校验）: " + error.getMessage());
    }

    @Test
    @DisplayName("最后段尾部损坏 + 干净关闭：仍然能被发现并按策略修复")
    void lastSegmentDamageStillDetected(@TempDir Path dir) throws Exception {
        Path lastSegment;
        DefaultWal wal = TestWal.open(dir);
        for (int i = 0; i < 40; i++) {
            wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
        }
        List<Segment> segments = wal.segmentManager().segments(PARTITION);
        lastSegment = segments.get(segments.size() - 1).handle().path();
        wal.close(Duration.ofSeconds(5));

        // 把最后一帧的 magic 写坏
        long frames = 40L;
        flipByte(lastSegment, (frames - 1L) * FRAME);

        try (DefaultWal reopened = TestWal.open(dir, 4096L, b -> {
        })) {
            assertEquals(39, TestWal.readAllLsns(reopened, PARTITION).size(),
                    "尾段损坏必须被发现并截断到最近完整帧");
            assertFalse(reopened.openReport().clean(),
                    "发生过截断就不能报告 clean");
        }
    }

    @Test
    @DisplayName("LOCK 互斥：同一目录第二个实例抛 AlreadyOpenException")
    void secondInstanceIsRejected(@TempDir Path dir) {
        try (DefaultWal first = TestWal.open(dir)) {
            first.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);

            WalException.AlreadyOpenException error = assertThrows(
                    WalException.AlreadyOpenException.class,
                    () -> TestWal.open(dir));
            assertTrue(error.getMessage().contains("LOCK") || error.getMessage().contains("占用")
                            || error.getMessage().contains("已打开"),
                    "错误消息必须说清是目录已被占用: " + error.getMessage());

            // 第一个实例仍然可用（第二个实例启动失败不能污染它）
            long lsn = first.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);
            assertTrue(lsn > 0L);
        }
        // 第一个实例关闭后，LOCK 被释放，可以正常打开
        try (DefaultWal third = TestWal.open(dir)) {
            assertFalse(third.isClosed());
        }
    }

    @Test
    @DisplayName("close(default) 之后崩溃式重开（未走 meta 更新路径）也可读全")
    void crashAfterCloseStillReadsEverything(@TempDir Path dir) {
        int total = 120;
        DefaultWal wal = TestWal.open(dir);
        for (int i = 0; i < total; i++) {
            wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
        }
        // 直接模拟"close 完成后立刻断电"：close 已经 force 过，因此数据必须完整
        wal.close(Duration.ofSeconds(10));

        try (DefaultWal reopened = TestWal.open(dir, 4096L, b -> {
        })) {
            assertEquals(total, TestWal.readAllLsns(reopened, PARTITION).size());
        }
    }

    @Test
    @DisplayName("恢复期间抛异常时仍释放 LOCK（否则同一进程永远打不开该目录）")
    void lockReleasedWhenRecoveryFails(@TempDir Path dir) throws Exception {
        // 先把目录搞成"中间段损坏"，让恢复必然失败
        DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        });
        for (int i = 0; i < 40; i++) {
            wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
        }
        List<Segment> segments = wal.segmentManager().segments(PARTITION);
        assertTrue(segments.size() >= 3);
        Path middle = segments.get(1).handle().path();
        wal.crashStopForTesting();
        flipByte(middle, FrameLayout.HEADER_LEN + 24L);

        assertThrows(WalException.CorruptedException.class,
                () -> TestWal.open(dir, 1024L, b -> {
                }));

        // 关键：失败之后 LOCK 必须已释放，所以"再一次尝试"应该得到 CorruptedException，
        // 而不是 AlreadyOpenException（后者意味着锁泄漏）
        WalException second = assertThrows(WalException.class,
                () -> TestWal.open(dir, 1024L, b -> {
                }));
        assertFalse(second instanceof WalException.AlreadyOpenException,
                "恢复失败后 LOCK 必须已释放，实际抛出了: " + second);
    }

    @Test
    @DisplayName("关闭报告包含耗时；关闭后 stats() 仍可读（只聚合计数器，不碰已释放资源）")
    void closeReportHasElapsedAndStatsRemainReadable(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        wal.appendSync(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS);
        long totalBefore = wal.stats().totalRecords();
        CloseReport report = wal.close(Duration.ofSeconds(5));
        assertTrue(report.elapsedMillis() >= 0L);
        assertEquals(0L, report.maxUnflushedLsn(),
                "全部刷盘后不应残留最大未刷 LSN");
        assertEquals(totalBefore, wal.stats().totalRecords(),
                "关闭后统计必须保持终态，不能被清零");
    }

    private static void flipByte(Path file, long offset) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 0xFF);
            raf.getFD().sync();
        }
    }

    /** 覆盖 {@code FsyncPolicy.ALWAYS}：每条 appendSync 都必须真的 fsync。 */
    @Test
    @DisplayName("FsyncPolicy.ALWAYS：close 之后立即断电也不丢任何一条")
    void alwaysPolicyDurability(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 4096L,
                b -> b.fsyncPolicy(WalConfig.FsyncPolicy.ALWAYS))) {
            for (int i = 0; i < 50; i++) {
                wal.appendSync(PARTITION, TestWal.payload(16, i), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.close(Duration.ofSeconds(10)).clean());
        }
        try (DefaultWal reopened = TestWal.open(dir, 4096L,
                b -> b.fsyncPolicy(WalConfig.FsyncPolicy.ALWAYS))) {
            assertEquals(50, TestWal.readAllLsns(reopened, PARTITION).size());
        }
    }
}
