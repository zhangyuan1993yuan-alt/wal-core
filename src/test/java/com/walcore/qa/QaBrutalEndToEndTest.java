package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalRecord;
import com.walcore.api.WalStats;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 对抗性端到端暴力场景（README/PRD P0-01①/P0-08① 的独立复核）。
 *
 * <p>16 分区 × 8 线程并发写 80000 条（payload 混合 48B / 256B / 1KB），
 * flushAndWait → close → reopen → 全量读回。断言：
 * <ol>
 *   <li>LSN 恰为 1..80000 的完整置换（无丢失、无重复、无空洞）；</li>
 *   <li>payload 逐条字节级一致；</li>
 *   <li>重开后 {@code stats()} 的条数/字节口径正确（数据字节，非段容量）。</li>
 * </ol>
 */
@DisplayName("QA: 16分区×8线程×80000条端到端暴力验证")
class QaBrutalEndToEndTest {

    private static final int PARTITIONS = 16;
    private static final int THREADS_PER_PARTITION = 8;
    private static final int RECORDS_PER_THREAD = 625;      // 16×8×625 = 80000
    private static final int[] PAYLOAD_SIZES = {48, 256, 1024};
    private static final long SEGMENT_BYTES = 1L << 20;     // 1MB 段 → 必然多次轮转

    @Test
    @DisplayName("并发 80000 条混合 payload：close→reopen 后 LSN 为 1..80000 的置换且 payload 逐条一致")
    void brutalEndToEnd(@TempDir Path dir) throws Exception {
        Map<Long, byte[]> expectedByLsn = new ConcurrentHashMap<>();
        // 多线程并发累加必须用 LongAdder（普通 long[] += 会丢更新，这是 QA 测试自身的坑）
        java.util.concurrent.atomic.LongAdder expectedBytes = new java.util.concurrent.atomic.LongAdder();

        try (DefaultWal wal = TestWal.open(dir, SEGMENT_BYTES,
                b -> b.partitions(PARTITIONS).groupCommitThreads(2))) {

            long[] lastLsnPerPartition = new long[PARTITIONS];
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            AtomicInteger failures = new AtomicInteger();

            for (int pid = 0; pid < PARTITIONS; pid++) {
                for (int t = 0; t < THREADS_PER_PARTITION; t++) {
                    final int partitionId = pid;
                    final int threadIdx = t;
                    Thread thread = new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                                int payloadLen = PAYLOAD_SIZES[(threadIdx + i) % PAYLOAD_SIZES.length];
                                byte[] bytes = QaSupport.seededPayload(payloadLen,
                                        partitionId * 100_003L + threadIdx * 1_000_003L + i);
                                long lsn = wal.append(partitionId,
                                        ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
                                assertNull(expectedByLsn.put(lsn, bytes),
                                        "LSN 重复分配: " + lsn);
                                // frameLen = 64 + storedLen（本例 payload 均为 8 的倍数，无 padding）
                                expectedBytes.add(64L + payloadLen);
                                synchronized (lastLsnPerPartition) {
                                    if (lsn > lastLsnPerPartition[partitionId]) {
                                        lastLsnPerPartition[partitionId] = lsn;
                                    }
                                }
                            }
                        } catch (Throwable e) {
                            failures.incrementAndGet();
                            e.printStackTrace();
                        }
                    }, "qa-brutal-p" + pid + "-t" + t);
                    thread.start();
                    threads.add(thread);
                }
            }

            start.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            assertEquals(0, failures.get(), "写入线程不允许有任何失败");

            // 写入线程全部完成后：已发布水位必须恰好覆盖 1..80000
            WalStats stats = wal.stats();
            assertEquals(80_000L, stats.totalRecords(), "写入后 stats().totalRecords 应为 80000");
            assertEquals(80_000L, stats.publishedLsnMax(), "写入后 publishedLsnMax 应为 80000");

            // 持久确认：每分区刷到各自最后一条
            for (int pid = 0; pid < PARTITIONS; pid++) {
                assertTrue(wal.flushAndWait(pid, lastLsnPerPartition[pid], 60_000L),
                        "分区 " + pid + " 的最后一条 LSN " + lastLsnPerPartition[pid]
                                + " 必须能持久确认");
            }
        }

        // ---- close → reopen ----
        try (DefaultWal reopened = TestWal.open(dir, SEGMENT_BYTES,
                b -> b.partitions(PARTITIONS).groupCommitThreads(2))) {

            Map<Long, byte[]> actualByLsn = new ConcurrentHashMap<>();
            for (int pid = 0; pid < PARTITIONS; pid++) {
                for (WalRecord record : QaSupport.readAll(reopened, pid)) {
                    assertNull(actualByLsn.put(record.lsn(), record.payloadBytes()),
                            "重开后 LSN 出现重复: " + record.lsn());
                }
            }

            assertEquals(80_000L, actualByLsn.size(),
                    "重开后总记录数必须恰为 80000（无丢失、无重复）");
            for (long lsn = 1L; lsn <= 80_000L; lsn++) {
                byte[] actual = actualByLsn.get(lsn);
                assertNotNull(actual, "LSN " + lsn + " 丢失（LSN 空洞）");
                byte[] expected = expectedByLsn.get(lsn);
                assertNotNull(expected, "写入侧未记录 LSN " + lsn);
                org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual,
                        "LSN " + lsn + " 的 payload 与写入时不一致");
            }

            WalStats stats = reopened.stats();
            assertEquals(80_000L, stats.totalRecords(), "重开后 totalRecords 应为 80000");
            // 口径：totalLogBytes = 数据字节（Σ frameLen），不是段容量
            assertEquals(expectedBytes.sum(), stats.totalLogBytes(),
                    "totalLogBytes 必须是数据字节口径: 期望 Σ(64+payloadLen)=" + expectedBytes.sum()
                            + "，实际 " + stats.totalLogBytes());
        }
    }
}
