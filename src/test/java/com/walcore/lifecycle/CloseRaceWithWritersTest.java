package com.walcore.lifecycle;

import com.walcore.api.CloseReport;
import com.walcore.api.PartitionInfo;
import com.walcore.api.RemoveMode;
import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F2 回归（生命周期侧）：close / removePartition 与 in-flight 写入并发。</b>
 *
 * <h2>被回归的缺陷</h2>
 * <ul>
 *   <li>旧 {@code close()} 第一步就 {@code pipeline.close()}（unmap staging 区）并冻结段，
 *       而一个已经通过预检、正在临界区内写帧的线程会与 unmap 竞争——use-after-unmap
 *       （native 崩溃风险）。修复后 close 先逐分区拿写锁（限期重试）再动 pipeline/段；</li>
 *   <li>旧 {@code removePartition(FREEZE)} 不持分区写锁封存段，与 in-flight append 竞争
 *       （复活段 / 写指针越过 finalLength）。修复后 freezePartition 全程持分区写锁。</li>
 * </ul>
 *
 * <h2>覆盖方式</h2>
 * <ol>
 *   <li>close vs 多写线程：写线程打到 close 为止；断言 CloseReport 干净、
 *       全部返回 LSN 全局唯一、无意外异常类型（确定性的锁内复核用例在
 *       {@code LockedStateRecheckTest}，此处验证的是完整装配下的生命周期顺序）；</li>
 *   <li>removePartition(FREEZE) vs 写线程：断言冻结后 segmentCount 不增长（不复活）、
 *       后续写入被拒、其他分区不受影响。</li>
 * </ol>
 */
class CloseRaceWithWritersTest {

    @Test
    @DisplayName("F2：close 与多写线程并发 → 干净关闭 + 全局 LSN 唯一")
    void closeRacesWithWriters(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 64L * 1024, b -> {
        })) {
            ConcurrentLinkedQueue<Long> seen = new ConcurrentLinkedQueue<>();
            AtomicBoolean stop = new AtomicBoolean(false);
            List<Thread> writers = new ArrayList<>();
            List<AtomicReference<Throwable>> unexpected = new ArrayList<>();
            int threadsPerPartition = 2;
            for (int pid = 0; pid <= 1; pid++) {
                for (int t = 0; t < threadsPerPartition; t++) {
                    final int partitionId = pid;
                    AtomicReference<Throwable> bad = new AtomicReference<>();
                    unexpected.add(bad);
                    Thread w = new Thread(() -> {
                        try {
                            while (!stop.get()) {
                                seen.add(wal.append(partitionId,
                                        TestWal.payload(32), com.walcore.api.AppendOptions.DEFAULTS));
                            }
                        } catch (WalException.ClosedException expected) {
                            // close 之后：正常退出
                        } catch (WalException.PartitionException expected) {
                            // 冻结类迁移（本用例没有，但同类异常也算受控退出）
                        } catch (Throwable t2) {
                            bad.set(t2);
                        }
                    }, "f2-close-writer-" + pid + "-" + t);
                    writers.add(w);
                    w.start();
                }
            }

            // 让写线程真正跑起来（打满临界区，制造与 close 的交错）
            awaitAtLeast(seen, 2_000, 5_000L);

            CloseReport report = wal.close(Duration.ofSeconds(30));
            stop.set(true);
            for (Thread w : writers) {
                w.join(TimeUnit.SECONDS.toMillis(30));
                assertEquals(Thread.State.TERMINATED, w.getState(), "写线程必须在超时内结束");
            }

            for (AtomicReference<Throwable> bad : unexpected) {
                assertNull(bad.get(), "close 并发窗口内不允许出现意外异常: " + bad.get());
            }
            assertTrue(report.clean(),
                    "写线程全部停止、无锁超时，close 必须干净完成: " + report);
            assertTrue(report.unflushedPartitions().isEmpty(),
                    "不应有未刷盘分区: " + report.unflushedPartitions());

            Set<Long> unique = new HashSet<>();
            seen.forEach(unique::add);
            assertEquals(seen.size(), unique.size(), "close 竞态期间 LSN 仍全局唯一");
        }
    }

    @Test
    @DisplayName("F2：removePartition(FREEZE) 与写线程并发 → 段不复活、写入被拒、他分区无损")
    void freezeRacesWithWriter(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 64L * 1024, b -> {
        })) {
            // 预热分区 0 的首段，保证有且只有 1 个段可被复活检测
            for (int i = 0; i < 10; i++) {
                TestWal.append(wal, 0, 32);
            }

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicReference<Throwable> unexpected = new AtomicReference<>();
            CountDownLatch writerStarted = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                writerStarted.countDown();
                try {
                    while (!stop.get()) {
                        wal.append(0, TestWal.payload(32), com.walcore.api.AppendOptions.DEFAULTS);
                    }
                } catch (WalException.PartitionException expected) {
                    // 冻结后写入被拒：受控退出
                } catch (Throwable t) {
                    unexpected.set(t);
                }
            }, "f2-freeze-writer");
            writer.start();
            assertTrue(writerStarted.await(5, TimeUnit.SECONDS), "写线程必须先启动");

            // 与 in-flight 写并发执行冻结（freezePartition 内部会拿分区写锁排队等临界区排干）
            wal.removePartition(0, RemoveMode.FREEZE);
            writer.join(TimeUnit.SECONDS.toMillis(30));
            assertEquals(Thread.State.TERMINATED, writer.getState(), "写线程必须在超时内结束");
            stop.set(true);

            assertNull(unexpected.get(), "冻结并发窗口内不允许出现意外异常: " + unexpected.get());

            PartitionInfo info = wal.partitionInfo(0);
            assertEquals(1, info.segmentCount(),
                    "冻结后分区不得复活新段（旧代码的 TOCTOU 会 createNextSegment）");
            assertEquals(PartitionInfo.State.FROZEN, info.state());

            WalException.PartitionException rejected = assertThrows(
                    WalException.PartitionException.class,
                    () -> wal.append(0, TestWal.payload(32), com.walcore.api.AppendOptions.DEFAULTS),
                    "冻结分区的后续写入必须被拒绝");
            assertTrue(rejected.getMessage().contains("不可写"), rejected.getMessage());

            // 其他分区完全不受影响
            long lsn1 = TestWal.append(wal, 1, 32);
            assertTrue(lsn1 > 0, "分区 1 必须仍可写（隔离性）");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 等待并发队列积累到至少 {@code min} 个元素。
     *
     * @param queue 队列
     * @param min 最少元素数
     * @param timeoutMillis 超时
     * @return 达标返回 true
     */
    private static boolean awaitAtLeast(ConcurrentLinkedQueue<Long> queue, int min,
                                        long timeoutMillis) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (queue.size() >= min) {
                return true;
            }
            Thread.yield();
        }
        return queue.size() >= min;
    }
}
