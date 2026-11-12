package com.walcore.groupcommit;

import com.walcore.api.Health;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.append.DirtyTracker;
import com.walcore.internal.append.LsnAllocator;
import com.walcore.internal.groupcommit.FlushTicket;
import com.walcore.internal.groupcommit.GroupCommitCoordinator;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.spi.SegmentHandle;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T07 验收之二：<b>组提交的三重触发、三档策略、真 fsync、失败重试</b>。
 *
 * <p>本测试用一个会记录"谁在什么线程上调了 force"的假段句柄
 * （{@link FakeHandle}），从而把 ARCH §7 里那些"只能靠代码审查"的断言变成可执行断言：
 * <ul>
 *   <li>写入线程在 {@code BATCH} 下<b>从不</b>调用 {@code force}
 *       —— 这是"append 不被 fsync 阻塞"的唯一硬证据；</li>
 *   <li>每个批次对每个脏分区<b>各</b>调用一次 {@code force(false)}
 *       —— 证明没有"假 fsync 覆盖全部分区"；</li>
 *   <li>{@code force} 抛 {@code IOException} 后 {@code durableLsn} <b>不前进</b>
 *       —— 证明不会假确认。</li>
 * </ul>
 */
class GroupCommitTest {

    private static final int PARTITIONS = 16;

    // ==================================================================
    // 1. 三重触发
    // ==================================================================

    @Test
    @DisplayName("时间窗触发：无外部唤醒时，也要在 ~batchIntervalMicros 内完成")
    void timeWindowTrigger(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(20_000)
                .batchMaxRecords(10_000_000).batchMaxBytes(1 << 24))) {
            rig.start();
            rig.publish(1, 1L);
            rig.coordinator.onAppend(1, 320); // 单条 → 条数/字节都不触发

            long t0 = System.nanoTime();
            rig.coordinator.awaitDurable(1, 1L, 5_000L);
            long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;

            assertEquals(1L, rig.partition(1).durableLsn());
            assertTrue(elapsedMillis <= 2_000L,
                    "20ms 时间窗内必须完成，实际 " + elapsedMillis + "ms");
        }
    }

    @Test
    @DisplayName("条数触发：达到 batchMaxRecords 立即唤醒，不等时间窗")
    void recordCountTrigger(@TempDir Path dir) throws Exception {
        // 时间窗给 30s：若不靠"条数触发"，本测试会超时
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(30_000_000)
                .batchMaxRecords(100).batchMaxBytes(1 << 24))) {
            rig.start();
            long t0 = System.nanoTime();
            for (int i = 1; i <= 100; i++) {
                rig.publish(2, i);
                rig.coordinator.onAppend(2, 320);
            }
            rig.coordinator.awaitDurable(2, 100L, 5_000L);
            long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;
            assertEquals(100L, rig.partition(2).durableLsn());
            assertTrue(elapsedMillis < 5_000L,
                    "条数触发必须在毫秒级完成（时间窗是 30s），实际 " + elapsedMillis + "ms");
        }
    }

    @Test
    @DisplayName("字节触发：单条大记录达到 batchMaxBytes 立即唤醒")
    void byteTrigger(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(30_000_000)
                .batchMaxRecords(10_000_000).batchMaxBytes(4_096))) {
            rig.start();
            long t0 = System.nanoTime();
            rig.publish(3, 1L);
            rig.coordinator.onAppend(3, 8_192); // 单条就超过 batchMaxBytes
            rig.coordinator.awaitDurable(3, 1L, 5_000L);
            long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue(elapsedMillis < 5_000L,
                    "字节触发必须在毫秒级完成（时间窗是 30s），实际 " + elapsedMillis + "ms");
        }
    }

    @Test
    @DisplayName("PERIODIC 策略忽略条数/字节触发，只按 flushIntervalMillis 刷")
    void periodicIgnoresRecordAndByteTriggers(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.fsyncPolicy(WalConfig.FsyncPolicy.PERIODIC)
                .batchIntervalMicros(1_000).batchMaxRecords(2).batchMaxBytes(64)
                .flushIntervalMillis(150))) {
            rig.start();
            rig.publish(4, 1L);
            for (int i = 0; i < 200; i++) {
                rig.coordinator.onAppend(4, 320); // 条数与字节都远超阈值
            }
            Thread.sleep(60L);
            assertEquals(0, rig.handle(4).forceCount(),
                    "PERIODIC 下 60ms 内不该有任何 force（周期是 150ms）");

            rig.coordinator.awaitDurable(4, 1L, 5_000L);
            assertTrue(rig.handle(4).forceCount() >= 1,
                    "到达周期后必须刷盘，实际 force 次数 " + rig.handle(4).forceCount());
        }
    }

    @Test
    @DisplayName("BATCH 策略下写入线程从不调用 force（关键断言）")
    void writeThreadNeverCallsForceUnderBatch(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
                .batchIntervalMicros(500).batchMaxRecords(64).batchMaxBytes(4_096))) {
            rig.start();
            rig.publish(5, 1L);
            Thread writer = new Thread(() -> {
                for (int i = 1; i <= 2_000; i++) {
                    rig.publish(5, i);
                    rig.coordinator.onAppend(5, 320);
                }
            }, "app-writer-thread");
            writer.start();
            writer.join(10_000);
            assertFalse(writer.isAlive());

            rig.coordinator.awaitDurable(5, 2_000L, 5_000L);
            List<String> threads = rig.handle(5).forceThreadNames();
            assertFalse(threads.isEmpty(), "应当发生过 force");
            for (String name : threads) {
                assertFalse("app-writer-thread".equals(name),
                        "写入线程绝不允许调用 force（否则 append 被 fsync 阻塞）。"
                                + "实际在该线程上发生了 " + Collections.frequency(threads, name)
                                + " 次 force");
            }
            for (String name : threads) {
                assertTrue(name.startsWith("wal-groupcommit-"),
                        "force 必须只在组提交线程上发生，实际: " + name);
            }
        }
    }

    // ==================================================================
    // 2. 真 fsync / 按分区
    // ==================================================================

    @Test
    @DisplayName("每个脏分区各自 force 一次（不做假覆盖）")
    void forcesEveryDirtyPartition(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000).groupCommitThreads(1))) {
            rig.start();
            for (int pid = 0; pid < 8; pid++) {
                rig.publish(pid, 100L + pid);
                rig.coordinator.onAppend(pid, 320);
            }
            for (int pid = 0; pid < 8; pid++) {
                rig.coordinator.awaitDurable(pid, 100L + pid, 5_000L);
                assertEquals(1, rig.handle(pid).forceCount(),
                        "分区 " + pid + " 应恰好被 force 一次（多分区批次必须逐个 force）");
            }
            assertEquals(8L, rig.coordinator.stats().forcedPartitions(),
                    "累计 force 的分区次数应等于 8");
        }
    }

    @Test
    @DisplayName("force(false) 传的是 metaData=false（fdatasync 语义）")
    void forceUsesNonMetadataMode(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000))) {
            rig.start();
            rig.publish(0, 1L);
            rig.coordinator.onAppend(0, 320);
            rig.coordinator.awaitDurable(0, 1L, 5_000L);
            assertEquals(Boolean.FALSE, rig.handle(0).lastMetaData(),
                    "活跃段刷盘必须是 force(false)；force(true) 只在创建/封存时用");
        }
    }

    @Test
    @DisplayName("多 flusher 按 pid % N 亲和分摊分区")
    void flushersSplitPartitionsByAffinity(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000).groupCommitThreads(2))) {
            rig.start();
            for (int pid = 0; pid < 8; pid++) {
                rig.publish(pid, 50L);
                rig.coordinator.onAppend(pid, 320);
            }
            for (int pid = 0; pid < 8; pid++) {
                rig.coordinator.awaitDurable(pid, 50L, 5_000L);
            }
            for (int pid = 0; pid < 8; pid++) {
                List<String> names = rig.handle(pid).forceThreadNames();
                assertFalse(names.isEmpty(), "分区 " + pid + " 从未被 force");
                // 线程名形如 wal-groupcommit-0-0（前缀-index-序号）
                String expectedPrefix = "wal-groupcommit-" + (pid % 2) + "-";
                for (String n : names) {
                    assertTrue(n.startsWith(expectedPrefix),
                            "分区 " + pid + " 应由 " + expectedPrefix
                                    + "* 负责（pid % N 亲和），实际: " + n);
                }
            }
        }
    }

    // ==================================================================
    // 3. flushAndWait 快/慢路径
    // ==================================================================

    @Test
    @DisplayName("快路径：durableLsn 已达标时 0 次 force、立即返回")
    void fastPathDoesNoForcing(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000))) {
            rig.start();
            rig.publish(6, 10L);
            rig.coordinator.onAppend(6, 320);
            rig.coordinator.awaitDurable(6, 10L, 5_000L);
            int before = rig.handle(6).forceCount();

            long t0 = System.nanoTime();
            for (int i = 0; i < 100_000; i++) {
                rig.coordinator.awaitDurable(6, 10L, 5_000L);
            }
            long elapsedMicros = (System.nanoTime() - t0) / 1_000L;
            assertEquals(before, rig.handle(6).forceCount(),
                    "快路径不得产生任何新的 force（0 系统调用）");
            assertTrue(elapsedMicros < 2_000_000L,
                    "10 万次快路径应在 2s 内完成，实际 " + elapsedMicros + "µs");
        }
    }

    @Test
    @DisplayName("慢路径：等待者被正确唤醒，水位推进")
    void slowPathWakesUpWaiter(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(2_000))) {
            rig.start();
            rig.publish(7, 1L);
            rig.coordinator.onAppend(7, 320);

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                try {
                    rig.coordinator.awaitDurable(7, 1L, 5_000L);
                } catch (Throwable e) {
                    failure.set(e);
                } finally {
                    done.countDown();
                }
            }, "flush-waiter");
            waiter.start();
            assertTrue(done.await(5, TimeUnit.SECONDS), "等待者未被唤醒");
            assertEquals(null, failure.get());
            assertEquals(1L, rig.partition(7).durableLsn());
        }
    }

    @Test
    @DisplayName("超时：目标 LSN 永远不到时抛 TimeoutException")
    void timeoutWhenNeverDurable(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000))) {
            rig.start();
            rig.publish(8, 5L);
            rig.coordinator.onAppend(8, 320);
            rig.coordinator.awaitDurable(8, 5L, 5_000L); // 先正常落盘到 5
            // 再等一个永远不会被发布的 LSN：没有脏位 → 没人会去 force → 只能超时。
            //
            // 注意（T14 修复带来的前提变更）：旧写法是"publish(8,5) 但不置脏"，
            // 靠"永远不刷"来构造超时——但"published > durable 却没有脏位"恰恰是
            // 丢位缺陷的状态，现在会被兜底扫描（sweepStaleDirtyBits）在 1s 内修复，
            // 于是那条用例变成"等它超时，结果它被补刷了"。改用"目标超过已发布水位"
            // 来构造，语义（目标永远达不到 → 超时）不变，且不再依赖一个非法状态。
            assertThrows(WalException.TimeoutException.class,
                    () -> rig.coordinator.awaitDurable(8, 6L, 50L));
        }
    }

    @Test
    @DisplayName("2 万次并发 flushAndWait：无丢失唤醒，最终水位正确")
    void concurrentAwaitersNoLostWakeup(@TempDir Path dir) throws Exception {
        final int threads = 4;
        final int perThread = 5_000;
        // batchMaxRecords=1：每次 onAppend 都会 unpark，因此本测试验证的是"唤醒链路"，
        // 不受 parkNanos 的平台粒度影响（Windows 上 parkNanos(200µs) 实际约 1ms，
        // 目标平台 Linux 才是性能基线；这里不让测试依赖那个数字）
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(200)
                .batchMaxRecords(1).groupCommitThreads(2))) {
            rig.start();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong maxDurable = new AtomicLong();
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                for (int t = 0; t < threads; t++) {
                    pool.execute(() -> {
                        try {
                            start.await();
                            for (int i = 1; i <= perThread; i++) {
                                rig.publish(9, i);
                                rig.coordinator.onAppend(9, 320);
                                rig.coordinator.awaitDurable(9, i, 10_000L);
                                maxDurable.accumulateAndGet(rig.partition(9).durableLsn(),
                                        Math::max);
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                }
                start.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS),
                        "并发 flushAndWait 超时（疑似丢失唤醒）");
            } finally {
                pool.shutdownNow();
            }
            assertEquals(null, failure.get(), "并发等待出现异常");
            assertEquals(0L, rig.coordinator.waiterCount(9),
                    "全部完成后不应残留等待者（残留说明有唤醒丢失）");
            assertTrue(maxDurable.get() >= perThread,
                    "最终持久水位应至少覆盖每个线程最后写入的 LSN");
        }
    }

    // ==================================================================
    // 4. 失败处理
    // ==================================================================

    @Test
    @DisplayName("force 抛 IOException：DEGRADED、水位不推进、等待者收 IO 异常、重试后恢复")
    void forceFailureDegradesAndRecovers(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000), 150L)) {
            rig.start();
            AtomicLong durableAtFailure = new AtomicLong(-1L);
            rig.publish(10, 42L);
            rig.handle(10).failNextForceWith(new IOException("模拟 ENOSPC"));
            rig.coordinator.onAppend(10, 320);

            WalException.IoException e = assertThrows(WalException.IoException.class,
                    () -> rig.coordinator.awaitDurable(10, 42L, 5_000L));
            assertEquals("WAL_IO", e.errorCode());
            durableAtFailure.set(rig.partition(10).durableLsn());
            assertEquals(0L, durableAtFailure.get(),
                    "force 失败后 durableLsn 绝不能推进（否则是假持久性）");
            assertEquals(Health.DEGRADED, rig.coordinator.health(),
                    "force 失败必须把健康度置为 DEGRADED");

            // 重试窗口（150ms）过后应自动重试并恢复。
            //
            // ---- 观测竞态说明（flaky 修复）：HEALTHY 必须与水位一起轮询 ----
            // 刷盘线程对这两个状态的发布是<b>先后</b>的：forcePartition 先推进
            // durableLsn（=42），runBatch 收尾才把 health 从 DEGRADED 翻回 HEALTHY。
            // 只轮询水位的旧写法会在"水位刚达标、健康度尚未翻转"的窗口内退出循环，
            // 随后的 health 断言随机红——全量套件的高负载并发会拉宽该窗口，
            // 隔离跑窗口极窄所以不复发。两断言的失败形态也佐证定性：
            // 水位断言已通过、只有 HEALTHY 断言红，且 DEGRADED 是保守方向，
            // 属测试观测竞态而非源码缺陷。修法即把 HEALTHY 纳入轮询条件，
            // 两项同时达标才退出（deadline 总预算仍 10s）。
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (System.nanoTime() < deadline
                    && (rig.partition(10).durableLsn() < 42L
                        || rig.coordinator.health() != Health.HEALTHY)) {
                Thread.sleep(10L);
            }
            assertEquals(42L, rig.partition(10).durableLsn(),
                    "故障恢复后应补齐持久水位");
            assertEquals(Health.HEALTHY, rig.coordinator.health(),
                    "重试成功后必须恢复 HEALTHY");
            assertTrue(rig.coordinator.stats().failedBatches() >= 1L);
        }
    }

    @Test
    @DisplayName("失败期间新等待者立刻拿到异常，不会白等到超时")
    void waitersFailFastDuringDegraded(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000), 60_000L)) {
            rig.start();
            rig.publish(11, 7L);
            rig.handle(11).failNextForceWith(new IOException("模拟 EIO"));
            rig.coordinator.onAppend(11, 320);
            assertThrows(WalException.IoException.class,
                    () -> rig.coordinator.awaitDurable(11, 7L, 5_000L));

            // 仍处于退避窗口内：脏位保留，再等一次还是失败而不是超时
            rig.coordinator.onAppend(11, 320);
            long t0 = System.nanoTime();
            assertThrows(WalException.IoException.class,
                    () -> rig.coordinator.awaitDurable(11, 7L, 5_000L));
            long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue(elapsedMillis < 3_000L,
                    "应快速失败而不是等满超时，实际 " + elapsedMillis + "ms");
        }
    }

    @Test
    @DisplayName("批内某分区 force 失败：同批健康分区仍被刷盘（脏位不被失败分区带走）")
    void failureInOnePartitionDoesNotShedOthersDirtyBit(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000), 60_000L)) {
            rig.start();
            rig.publish(0, 10L);   // pid0：本批失败
            rig.publish(2, 20L);   // pid2：必须照常落盘
            rig.handle(0).failNextForceWith(new IOException("模拟 pid0 EIO"));
            rig.coordinator.onAppend(0, 320);
            rig.coordinator.onAppend(2, 320);

            assertThrows(WalException.IoException.class,
                    () -> rig.coordinator.awaitDurable(0, 10L, 5_000L));
            assertEquals(0L, rig.partition(0).durableLsn(), "失败分区水位绝不推进");

            // 关键断言：pid2 此后一次写入都没有，仍必须被刷到本批水位目标。
            // 旧实现在 pid0 失败处直接 return，pid2 的脏位随批次蒸发（全局位图里已取走、
            // 也没人 redirty）→ 这里会一直等到 deadline 仍为 0。
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (System.nanoTime() < deadline && rig.partition(2).durableLsn() < 20L) {
                Thread.sleep(5L);
            }
            assertEquals(20L, rig.partition(2).durableLsn(),
                    "同批健康分区必须照常落盘（脏位不得被失败分区带走）");
            assertTrue(rig.handle(2).forceCount() >= 1, "pid2 至少应被 force 一次");
        }
    }

    @Test
    @DisplayName("批内后续分区失败：已刷成功分区的等待者被立即唤醒（不假报失败）")
    void completedPartitionsWakeWaitersWhenLaterPartitionFails(@TempDir Path dir) throws Exception {
        // 时间窗放大到 300ms：批次只由下面的 requestFlush() 触发，便于把等待票先排进队列
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(300_000), 60_000L)) {
            rig.start();
            // 先在没有脏数据的时候把 pid0 的等待票排进队列（此时 durable=0 < 目标 10，它会 park）
            AtomicReference<Throwable> pid0Error = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    rig.coordinator.awaitDurable(0, 10L, 5_000L);
                } catch (Throwable e) {
                    pid0Error.set(e);
                }
            }, "pid0-waiter");
            waiter.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.sleep(100L); // 让票真正入队并 park；此期间无脏数据，flusher 不会动

            rig.publish(0, 10L);
            rig.publish(2, 20L);
            rig.handle(2).failNextForceWith(new IOException("模拟 pid2 EIO"));
            rig.coordinator.onAppend(0, 320);
            rig.coordinator.onAppend(2, 320);
            rig.coordinator.requestFlush(); // 同一批里：pid0 先成功，pid2 后失败

            waiter.join(5_000L);
            assertFalse(waiter.isAlive(), "pid0 已落盘，等待者不该睡满 5s 超时");
            assertNull(pid0Error.get(),
                    "pid0 已 force 成功，等待者必须拿到成功而不是异常：" + pid0Error.get());
            assertEquals(10L, rig.partition(0).durableLsn());
            // 失败分区照常上报，且是快速失败而非等满超时
            assertThrows(WalException.IoException.class,
                    () -> rig.coordinator.awaitDurable(2, 20L, 5_000L));
        }
    }

    @Test
    @DisplayName("丢位兜底：脏位被意外取走时，1s 扫描会重新置脏并补刷")
    void sweepRecoversLostDirtyBit(@TempDir Path dir) throws Exception {
        try (Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000), 60_000L)) {
            rig.publish(0, 10L);
            rig.coordinator.onAppend(0, 320);
            // 模拟"某次批次把脏位取走后没有还回来"（本次修复的那类缺陷）：直接 drain 掉且不归还。
            // 必须在 start() 之前做，否则 flusher 可能已经把这一位正常刷掉了。
            long[] stolen = new long[rig.tracker.wordCount()];
            assertTrue(rig.tracker.drain(stolen) > 0, "前提：确实取走了 pid0 的脏位");
            assertEquals(0L, rig.partition(0).durableLsn(), "取走后没有任何人会刷它");

            rig.start(); // 0 号 flusher 首次 tick 就会扫一遍（nextSweepNanos 初值 0）

            long deadline = System.nanoTime() + 8_000_000_000L;
            while (System.nanoTime() < deadline && rig.partition(0).durableLsn() < 10L) {
                Thread.sleep(20L);
            }
            assertEquals(10L, rig.partition(0).durableLsn(),
                    "兜底扫描必须重新置脏并补刷，否则丢位会让数据无限期留在 page cache");
        }
    }

    // ==================================================================
    // 5. 优雅关闭
    // ==================================================================

    @Test
    @DisplayName("drainAllAndStop：停车前排空所有脏分区（P0-10 不丢数据）")
    void drainAllAndStopFlushesRemaining(@TempDir Path dir) throws Exception {
        Rig rig = new Rig(dir, b -> b.batchIntervalMicros(30_000_000));
        try {
            rig.start();
            for (int pid = 0; pid < 4; pid++) {
                rig.publish(pid, 900L + pid);
                rig.coordinator.onAppend(pid, 320); // 时间窗 30s，绝不会自发刷
            }
            rig.coordinator.drainAllAndStop(5_000L);
            for (int pid = 0; pid < 4; pid++) {
                assertEquals(900L + pid, rig.partition(pid).durableLsn(),
                        "关闭前必须把所有脏分区刷完（分区 " + pid + "）");
            }
        } finally {
            rig.close();
        }
    }

    @Test
    @DisplayName("关闭后 awaitDurable 抛 ClosedException，不静默返回")
    void awaitAfterStopThrowsClosed(@TempDir Path dir) throws Exception {
        Rig rig = new Rig(dir, b -> b.batchIntervalMicros(1_000));
        try {
            rig.start();
            rig.publish(12, 1L);
            rig.coordinator.drainAllAndStop(1_000L);
            assertFalse(rig.coordinator.isRunning());
            assertThrows(WalException.ClosedException.class,
                    () -> rig.coordinator.awaitDurable(12, 999L, 100L));
        } finally {
            rig.close();
        }
    }

    // ==================================================================
    // 6. DirtyTracker
    // ==================================================================

    @Test
    @DisplayName("DirtyTracker：4096 分区 = 64 word，drain 后位图清零且不丢并发标记")
    void dirtyTrackerWordsAndDrain() {
        DirtyTracker tracker = new DirtyTracker(4096, 32L << 20);
        assertEquals(64, tracker.wordCount());
        assertEquals(1, new DirtyTracker(1, 0L).wordCount());
        assertEquals(1, new DirtyTracker(64, 0L).wordCount());
        assertEquals(2, new DirtyTracker(65, 0L).wordCount());

        tracker.markDirty(0, 100);
        tracker.markDirty(63, 100);
        tracker.markDirty(64, 100);
        tracker.markDirty(4095, 100);
        assertEquals(4L, tracker.totalPendingRecords(), "4 次 markDirty = 4 条待刷记录");
        assertEquals(400L, tracker.pendingBytes(), "4 × 100 字节");

        long[] mask = new long[64];
        int upper = tracker.drain(mask);
        assertEquals(64, upper, "最高脏 word 下标 63 → 上界 64");
        assertTrue((mask[0] & 1L) != 0L, "bit 0 应置位");
        assertTrue((mask[0] & (1L << 63)) != 0L, "bit 63 应置位");
        assertTrue((mask[1] & 1L) != 0L, "bit 64（word 1 bit 0）应置位");
        assertTrue((mask[63] & (1L << 63)) != 0L, "bit 4095（word 63 bit 63）应置位");
        assertEquals(400L, tracker.drainBytes());
        assertEquals(0L, tracker.pendingBytes(), "drain 后必须清零");

        // 已 drain 干净
        long[] second = new long[64];
        assertEquals(0, tracker.drain(second));

        // 迭代器：按升序给出脏分区
        tracker.markDirty(100, 1);
        tracker.markDirty(200, 1);
        tracker.markDirty(4095, 1);
        long[] m = new long[64];
        tracker.drain(m);
        List<Integer> pids = new ArrayList<>();
        int pid = tracker.nextDirtyPartition(m, 0);
        while (pid >= 0) {
            pids.add(pid);
            pid = tracker.nextDirtyPartition(m, pid + 1);
        }
        assertEquals(List.of(100, 200, 4095), pids);
    }

    @Test
    @DisplayName("DirtyTracker.drainMine：多 flusher 互不抢位（防饥饿）")
    void drainMinePartitionsWithoutStealing() {
        DirtyTracker tracker = new DirtyTracker(16, 0L);
        long[] maskA = new long[1];
        long[] maskB = new long[1];
        long[] bitsA = {DirtyTracker.bitsForWord(0, 0, 2, 16)};
        long[] bitsB = {DirtyTracker.bitsForWord(0, 1, 2, 16)};

        for (int pid = 0; pid < 8; pid++) {
            tracker.markDirty(pid, 1);
        }
        int upperA = tracker.drainMine(maskA, bitsA);
        assertEquals(1, upperA);
        assertEquals(0b0101_0101L, maskA[0], "flusher0 只应拿到偶数分区");
        int upperB = tracker.drainMine(maskB, bitsB);
        assertEquals(1, upperB);
        assertEquals(0b1010_1010L, maskB[0], "flusher1 只应拿到奇数分区");

        // 重要：A 取走自己的位后，B 的位必须还在（不能被"取全部再放回"的方式抢走）
        for (int pid = 0; pid < 8; pid++) {
            tracker.markDirty(pid, 1);
        }
        tracker.drainMine(maskA, bitsA);
        long[] again = new long[1];
        assertEquals(1, tracker.drainMine(again, bitsB),
                "B 的位不得被 A 抢空（否则 B 会长期饿死，wait 偶发超时）");
        assertEquals(0b1010_1010L, again[0]);

        // 与我无关的分区不会被取走
        tracker.markDirty(9, 1); // 奇数 → 属于 B
        long[] empty = new long[1];
        assertEquals(0, tracker.drainMine(empty, bitsA), "A 不该动 B 的 pid 9");
    }

    @Test
    @DisplayName("DirtyTracker：脏字节高水位判断")
    void highWatermark() {
        DirtyTracker tracker = new DirtyTracker(4, 1_000L);
        assertFalse(tracker.overHighWatermark());
        tracker.markDirty(0, 600);
        assertFalse(tracker.overHighWatermark());
        tracker.markDirty(0, 600);
        assertTrue(tracker.overHighWatermark(), "累计 1200 > 1000 必须触发背压信号");
        tracker.reset();
        assertFalse(tracker.overHighWatermark());
        assertEquals(0L, tracker.pendingBytes());
    }

    // ==================================================================
    // 夹具
    // ==================================================================

    /**
     * 测试夹具：注册表 + 假段句柄 + 脏追踪器 + 协调器。
     */
    private static final class Rig implements AutoCloseable {

        final PartitionRegistry registry = new PartitionRegistry(PARTITIONS);
        final DirtyTracker tracker = new DirtyTracker(PARTITIONS, 32L << 20);
        final GroupCommitCoordinator coordinator;
        final LsnAllocator lsnAllocator = new LsnAllocator();
        private final FakeHandle[] handles = new FakeHandle[PARTITIONS];

        Rig(Path dir, java.util.function.Consumer<WalBuilder> customizer) throws IOException {
            this(dir, customizer, GroupCommitCoordinator.DEFAULT_FORCE_RETRY_MILLIS);
        }

        Rig(Path dir, java.util.function.Consumer<WalBuilder> customizer, long retryMillis)
                throws IOException {
            WalBuilder builder = new WalBuilder()
                    .baseDir(dir)
                    .groupCommitThreads(1)
                    .batchIntervalMicros(1_000)
                    .batchMaxRecords(4_096)
                    .batchMaxBytes(1 << 20);
            customizer.accept(builder);
            WalConfig config = builder.buildConfig();
            for (int pid = 0; pid < PARTITIONS; pid++) {
                FakeHandle handle = new FakeHandle(pid);
                handles[pid] = handle;
                PartitionImpl partition = new PartitionImpl(pid);
                partition.setActiveSegment(new Segment(pid, 1L, handle));
                registry.register(partition);
            }
            this.coordinator = new GroupCommitCoordinator(registry, tracker, config, null,
                    retryMillis);
        }

        void start() {
            coordinator.start();
        }

        void publish(int pid, long lsn) {
            registry.require(pid).publishLsn(lsn);
        }

        PartitionImpl partition(int pid) {
            return registry.require(pid);
        }

        FakeHandle handle(int pid) {
            return handles[pid];
        }

        @Override
        public void close() {
            coordinator.close();
        }
    }

    /**
     * 记录型假段句柄：统计 force 次数、调用线程名、metaData 参数，并可注入一次性失败。
     */
    private static final class FakeHandle implements SegmentHandle {

        private final int pid;
        private final Path path;
        private final AtomicLong appendPosition = new AtomicLong();
        private final AtomicInteger forceCount = new AtomicInteger();
        private final List<String> forceThreads = new CopyOnWriteArrayList<>();
        private final AtomicReference<Boolean> lastMetaData = new AtomicReference<>();
        private final AtomicReference<IOException> pendingFailure = new AtomicReference<>();

        FakeHandle(int pid) {
            this.pid = pid;
            this.path = Paths.get("build", "fake-wal", "wal-p" + pid + ".wal");
        }

        void failNextForceWith(IOException e) {
            pendingFailure.set(e);
        }

        int forceCount() {
            return forceCount.get();
        }

        List<String> forceThreadNames() {
            return new ArrayList<>(forceThreads);
        }

        Boolean lastMetaData() {
            return lastMetaData.get();
        }

        @Override
        public Path path() {
            return path;
        }

        @Override
        public long firstLsn() {
            return 1L;
        }

        @Override
        public long capacityBytes() {
            return 256L * 1024 * 1024;
        }

        @Override
        public long appendPosition() {
            return appendPosition.get();
        }

        @Override
        public void append(ByteBuffer frame) {
            appendPosition.addAndGet(frame.remaining());
        }

        @Override
        public int read(long fileOffset, ByteBuffer dst, int len) {
            return 0;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            forceCount.incrementAndGet();
            lastMetaData.set(metaData);
            forceThreads.add(Thread.currentThread().getName());
            IOException e = pendingFailure.getAndSet(null);
            if (e != null) {
                throw e;
            }
        }

        @Override
        public void truncate(long newLength) {
            appendPosition.set(newLength);
        }

        @Override
        public long physicalLength() {
            return appendPosition.get();
        }

        @Override
        public boolean isMapped() {
            return true;
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public String toString() {
            return "FakeHandle{pid=" + pid + ", forced=" + forceCount.get() + '}';
        }
    }
}
