package com.walcore.append;

import com.walcore.api.Wal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T08 并发写入验收测试。
 *
 * <p><b>核心不变量（P0-01 ①，必须专测）</b>：同分区 8 线程并发写入后，
 * 用游标按<b>文件顺序</b>读出来的 LSN 序列必须<b>严格递增</b>。
 *
 * <p>这条断言的价值在于：它同时校验了"LSN 在锁内分配"（约束⑤）与"帧不跨段"
 * 两个最容易被改坏的地方。若把 {@code lsnAllocator.allocate()} 挪到锁外，
 * 单线程测试依然全绿，只有本测试会以"读到的 LSN 逆序"的方式失败。
 */
class ConcurrentAppendTest {

    @Test
    @DisplayName("同分区 8 线程 × 2000 条：文件内 LSN 严格递增且无重复")
    void samePartitionConcurrentWritesAreOrdered(@TempDir Path dir) throws Exception {
        final int threads = 8;
        final int perThread = 2000;
        try (Wal wal = TestWal.open(dir, 16 * 1024, b -> {
        })) {
            runConcurrently(wal, threads, perThread, 0);

            List<Long> inFileOrder = TestWal.readAllLsns(wal, 0);
            assertEquals(threads * perThread, inFileOrder.size(),
                    "读回的记录数必须等于写入总数（有丢失或重复）");
            for (int i = 1; i < inFileOrder.size(); i++) {
                long prev = inFileOrder.get(i - 1);
                long cur = inFileOrder.get(i);
                assertTrue(cur > prev,
                        "文件内 LSN 必须严格递增：第 " + i + " 条 " + cur
                                + " 不大于前一条 " + prev + "（LSN 是否在锁外分配？）");
            }
            assertEquals(inFileOrder.size(), new HashSet<>(inFileOrder).size(),
                    "LSN 不得重复");
        }
    }

    @Test
    @DisplayName("同分区并发写入后，LSN 集合恰好是 [1..N] 的置换（无空洞）")
    void concurrentWritesLeaveNoGaps(@TempDir Path dir) throws Exception {
        final int threads = 8;
        final int perThread = 2000;
        try (Wal wal = TestWal.open(dir, 16 * 1024, b -> {
        })) {
            runConcurrently(wal, threads, perThread, 0);
            List<Long> lsns = TestWal.readAllLsns(wal, 0);
            long[] sorted = new long[lsns.size()];
            for (int i = 0; i < sorted.length; i++) {
                sorted[i] = lsns.get(i);
            }
            Arrays.sort(sorted);
            for (int i = 0; i < sorted.length; i++) {
                assertEquals(i + 1L, sorted[i],
                        "第 " + i + " 个 LSN 应为 " + (i + 1) + "（出现空洞）");
            }
        }
    }

    @Test
    @DisplayName("多分区并发：每分区内部有序，全局 LSN 无空洞")
    void multiPartitionConcurrentWrites(@TempDir Path dir) throws Exception {
        final int threadsPerPartition = 4;
        final int perThread = 1000;
        final int partitions = 2;
        try (Wal wal = TestWal.open(dir, 32 * 1024, b -> {
        })) {
            List<Thread> workers = new ArrayList<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            List<List<Long>> collected = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                for (int t = 0; t < threadsPerPartition; t++) {
                    List<Long> sink = new ArrayList<>(perThread);
                    collected.add(sink);
                    final int partitionId = p;
                    Thread worker = new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < perThread; i++) {
                                ByteBuffer payload = TestWal.payload(32, i);
                                sink.add(wal.append(partitionId, payload));
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    }, "wal-worker-p" + p + "-t" + t);
                    worker.setDaemon(true);
                    workers.add(worker);
                    worker.start();
                }
            }
            start.countDown();
            for (Thread worker : workers) {
                worker.join(TimeUnit.SECONDS.toMillis(60));
            }
            assertNull(failure.get(), "并发写入抛异常: " + failure.get());

            Set<Long> all = new HashSet<>();
            for (List<Long> sink : collected) {
                all.addAll(sink);
            }
            assertEquals(partitions * threadsPerPartition * perThread, all.size(),
                    "全局 LSN 必须两两不同");
            long max = 0L;
            for (long lsn : all) {
                assertTrue(lsn >= 1L && lsn <= all.size(), "LSN 越界: " + lsn);
                max = Math.max(max, lsn);
            }
            assertEquals(all.size(), max, "最大 LSN 必须等于总条数（说明无空洞、无浪费）");

            for (int p = 0; p < partitions; p++) {
                List<Long> inFileOrder = TestWal.readAllLsns(wal, p);
                assertEquals(threadsPerPartition * perThread, inFileOrder.size(),
                        "分区 " + p + " 读回条数不符");
                for (int i = 1; i < inFileOrder.size(); i++) {
                    assertTrue(inFileOrder.get(i) > inFileOrder.get(i - 1),
                            "分区 " + p + " 文件内 LSN 必须严格递增");
                }
            }
        }
    }

    @Test
    @DisplayName("并发写入期间轮转频繁发生，仍不丢帧不跨界")
    void rotationUnderConcurrentLoad(@TempDir Path dir) throws Exception {
        try (Wal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            runConcurrently(wal, 4, 1500, 0);
            List<Long> lsns = TestWal.readAllLsns(wal, 0);
            assertEquals(6000, lsns.size(), "轮转期间不得丢帧");
            for (int i = 1; i < lsns.size(); i++) {
                assertTrue(lsns.get(i) > lsns.get(i - 1), "轮转后 LSN 顺序被破坏");
            }
        }
    }

    private static void runConcurrently(Wal wal, int threads, int perThread, int partitionId)
            throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        wal.append(partitionId, TestWal.payload(32, i));
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "wal-concurrent-" + t);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(120));
        }
        assertNull(failure.get(), "并发写入抛异常: " + failure.get());
    }
}
