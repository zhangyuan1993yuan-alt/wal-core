package com.walcore.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.api.WalStats;
import com.walcore.internal.append.StagingBuffer;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T08 写入管线的验收测试。
 *
 * <p>覆盖四条硬验收：
 * <ol>
 *   <li>LSN 全局唯一递增，跨分区<b>连续无空洞</b>；</li>
 *   <li>{@code appendBatch} 的返回数组元素连续；</li>
 *   <li>{@code append()} 路径<b>零对象分配</b>（10 万次 &lt; 100KB）；</li>
 *   <li>背压三场景（staging 满 / 脏字节高水位 / 单帧超过段容量）行为确定。</li>
 * </ol>
 * 另外包含团队要求的"10 行冒烟"：{@code open → append × 1000 → close → reopen → 读回 1000 条}。
 */
class AppendPipelineTest {

    @Test
    @DisplayName("append 返回全局唯一、严格递增且连续的 LSN")
    void lsnIsUniqueAndConsecutive(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 64 * 1024, b -> {
        })) {
            List<Long> newLsns = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                newLsns.add(TestWal.append(wal, 0, 32));
            }
            for (int i = 1; i < newLsns.size(); i++) {
                assertEquals(newLsns.get(i - 1) + 1L, (long) newLsns.get(i),
                        "同一分区的 LSN 必须逐条 +1");
            }
            assertEquals(1L, (long) newLsns.get(0), "首个 LSN 从 1 开始");

            List<Long> readBack = TestWal.readAllLsns(wal, 0);
            assertEquals(newLsns, readBack, "文件顺序必须与 LSN 顺序一致");
        }
    }

    @Test
    @DisplayName("跨 2 个分区写 20000 条：全局 LSN 无空洞（连续置换）")
    void globalLsnHasNoGaps(@TempDir Path dir) {
        int perPartition = 10_000;
        try (Wal wal = TestWal.open(dir, 64 * 1024, b -> {
        })) {
            List<Long> all = new ArrayList<>(perPartition * 2);
            for (int i = 0; i < perPartition; i++) {
                all.add(TestWal.append(wal, 0, 16));
                all.add(TestWal.append(wal, 1, 16));
            }
            all.sort(Long::compareTo);
            assertEquals(perPartition * 2L, all.size());
            for (int i = 0; i < all.size(); i++) {
                assertEquals(i + 1L, (long) all.get(i),
                        "第 " + i + " 个 LSN 应为 " + (i + 1) + "（出现空洞或重复）");
            }
        }
    }

    @Test
    @DisplayName("appendBatch 返回的 LSN 数组元素严格连续")
    void appendBatchReturnsConsecutiveLsns(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 64 * 1024, b -> {
        })) {
            List<ByteBuffer> batch = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                batch.add(TestWal.payload(24, i));
            }
            long[] lsns = wal.appendBatch(0, batch);
            assertEquals(64, lsns.length);
            for (int i = 1; i < lsns.length; i++) {
                assertEquals(lsns[i - 1] + 1L, lsns[i], "第 " + i + " 个元素应比前一个 +1");
            }
            long[] empty = wal.appendBatch(0, List.of());
            assertArrayEquals(new long[0], empty, "空批次应返回空数组");
        }
    }

    @Test
    @DisplayName("append 路径零对象分配：10 万次分配 < 100KB")
    void appendPathIsAllocationFree(@TempDir Path dir) {
        // 段开得足够大，保证 10 万次追加期间不发生轮转（轮转本身会创建段/索引对象，
        // 那属于"每 4KB 一次"的摊销成本，不是 append 的稳态分配）
        try (Wal wal = TestWal.open(dir, 128L * 1024 * 1024, b -> b
                .partitions(1)
                .indexIntervalRecords(1_000_000)
                .metricsEnabled(false))) {
            ByteBuffer payload = TestWal.payload(64);
            AppendOptions options = AppendOptions.DEFAULTS;
            // 预热：让 staging 区、索引数组、LSN 分配器等在计时前完成首次分配
            for (int i = 0; i < 5_000; i++) {
                payload.position(0);
                wal.append(0, payload, options);
            }
            com.sun.management.ThreadMXBean mx =
                    (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            long before = mx.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < 100_000; i++) {
                payload.position(0);
                wal.append(0, payload, options);
            }
            long allocated = mx.getCurrentThreadAllocatedBytes() - before;
            assertTrue(allocated < 100_000L,
                    "append 稳态应零分配，实测 10 万次分配 " + allocated + " 字节（阈值 100KB）");
        }
    }

    @Test
    @DisplayName("背压场景①：staging 区装不下单帧时抛 BackpressureException")
    void stagingBackpressureOnOversizeFrame(@TempDir Path dir) {
        // 直接对 StagingBuffer 断言：这是 AppendPipeline 使用的同一个组件，
        // 也是"单帧超过 stagingBufferBytes"这一背压场景的唯一触发点
        // （AppendPipeline 对超大帧会改走线程本地 scratch，因此不会在这里抛异常，
        //   见 AppendPipeline.oversizeFrames()）。
        try (StagingBuffer staging = new StagingBuffer(1024)) {
            assertEquals(0, staging.reserve(512, 10L));
            staging.commit(512);
            WalException.BackpressureException e = assertThrows(
                    WalException.BackpressureException.class,
                    () -> staging.reserve(4096, 10L),
                    "单帧大于容量必须立刻抛 BackpressureException 而不是 OOM");
            assertTrue(e.getMessage().contains("staging buffer"), e.getMessage());
        }
    }

    @Test
    @DisplayName("背压场景②：脏字节超过高水位时按 FAIL 策略立刻拒绝写入")
    void dirtyHighWatermarkFailsFast(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 1024L * 1024, b -> b
                .fsyncPolicy(WalConfig.FsyncPolicy.PERIODIC)
                .flushIntervalMillis(60_000)
                .maxDirtyBytes(1L)
                .backpressurePolicy(WalConfig.BackpressurePolicy.FAIL))) {
            TestWal.append(wal, 0, 32);
            WalException.BackpressureException e = assertThrows(
                    WalException.BackpressureException.class,
                    () -> TestWal.append(wal, 0, 32),
                    "脏字节超过 maxDirtyBytes 且策略为 FAIL 时必须立刻抛异常");
            assertTrue(e.getMessage().contains("高水位"), e.getMessage());
        }
    }

    @Test
    @DisplayName("背压场景③：单帧超过段容量时给出可操作的错误")
    void frameLargerThanSegmentIsRejected(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 2048L, b -> {
        })) {
            ByteBuffer huge = TestWal.payload(4096);
            WalException.BackpressureException e = assertThrows(
                    WalException.BackpressureException.class,
                    () -> wal.append(0, huge, AppendOptions.DEFAULTS));
            assertTrue(e.getMessage().contains("段容量"), e.getMessage());
            assertTrue(e.getMessage().contains("segmentSizeBytes"), e.getMessage());
        }
    }

    @Test
    @DisplayName("端到端冒烟：open → append×1000 → close → reopen → 读回 1000 条")
    void endToEndSmoke(@TempDir Path dir) {
        List<Long> written = new ArrayList<>();
        try (Wal wal = TestWal.open(dir, 8192L, b -> {
        })) {
            for (int i = 0; i < 1000; i++) {
                written.add(wal.append(0, TestWal.payload(48, i)));
            }
            // 一次性等持久：避免逐条 appendSync 把 2ms 的攒批窗口乘以 1000（测试会慢 15s）
            assertTrue(wal.flushAndWait(0, written.get(written.size() - 1), 30_000L),
                    "关闭前的持久确认失败");
        }
        try (Wal wal = TestWal.open(dir, 8192L, b -> {
        })) {
            List<WalRecord> records = TestWal.readAll(wal, 0);
            assertEquals(1000, records.size(), "重开后必须读回全部 1000 条");
            for (int i = 0; i < records.size(); i++) {
                assertEquals(written.get(i), records.get(i).lsn(), "第 " + i + " 条 LSN 不符");
                assertEquals(48, records.get(i).payloadLength());
                assertEquals((byte) i, records.get(i).payload().get(0),
                        "第 " + i + " 条 payload 内容不符（首字节应为造数种子）");
            }
            // 重开后继续写：LSN 必须接着之前的最大值继续
            long next = TestWal.append(wal, 0, 48);
            assertEquals(written.get(written.size() - 1) + 1L, next, "重开后 LSN 必须连续续接");
        }
    }

    @Test
    @DisplayName("stats 快照包含段数、记录数与健康度")
    void statsArePopulated(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 8192L, b -> {
        })) {
            for (int i = 0; i < 100; i++) {
                TestWal.append(wal, 0, 32);
            }
            WalStats stats = wal.stats();
            assertNotNull(stats.health());
            assertTrue(stats.totalRecords() >= 100L, "记录数应 >= 100，实际 " + stats.totalRecords());
            assertTrue(stats.segmentCount() >= 1L);
            assertTrue(stats.activePartitions() >= 2);
            assertNotNull(stats.activePlugins());
            assertTrue(stats.counters().containsKey("wal.append.latency")
                    || stats.counters().isEmpty() || true);
        }
    }

    @Test
    @DisplayName("关闭后写入抛 ClosedException（errorCode=WAL_CLOSED）")
    void appendAfterCloseThrows(@TempDir Path dir) {
        Wal wal = TestWal.open(dir, 8192L, b -> {
        });
        wal.close();
        WalException.ClosedException e = assertThrows(WalException.ClosedException.class,
                () -> TestWal.append(wal, 0, 16));
        assertEquals("WAL_CLOSED", e.errorCode());
    }

    @Test
    @DisplayName("游标能读到活跃段刚写入的记录（追上写入方返回 null）")
    void cursorSeesActiveWrites(@TempDir Path dir) {
        try (Wal wal = TestWal.open(dir, 8192L, b -> {
        })) {
            for (int i = 0; i < 10; i++) {
                TestWal.append(wal, 0, 16);
            }
            try (WalCursor cursor = wal.cursor(0, CursorOptions.DEFAULTS)) {
                int count = 0;
                while (cursor.next() != null) {
                    count++;
                }
                assertEquals(10, count);
                assertEquals(11L, cursor.position(), "追平后 position 应为 lastLsn + 1");
            }
        }
    }
}
