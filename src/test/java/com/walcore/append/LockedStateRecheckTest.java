package com.walcore.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.PartitionInfo;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.append.AppendPipeline;
import com.walcore.internal.append.DirtyTracker;
import com.walcore.internal.append.FrameEncoder;
import com.walcore.internal.append.LsnAllocator;
import com.walcore.internal.io.HeapStorageBackend;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.spiimpl.RawRecordCodec;
import com.walcore.spi.StorageConfig;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F2 回归（写入侧）：锁外可写性预检存在 TOCTOU 窗口，必须在分区写锁内复核。</b>
 *
 * <h2>被回归的缺陷</h2>
 * 旧 {@code AppendPipeline.append} 只在锁外做一次 {@code isWritable} 预检。
 * 竞态交错：写线程 A 预检通过（分区仍 ACTIVE）→ 尚未拿到锁 →
 * {@code removePartition(FREEZE)} 冻结该分区（封存段、清活跃指针、状态 FROZEN）→
 * A 拿到锁继续走 → {@code ensureWritableSegment} 发现没有活跃段 →
 * {@code createNextSegment} <b>复活出一个新段</b>并写帧。结果：冻结分区又长出
 * 一个段、{@code PartitionInfo.segmentCount} 与磁盘状态不一致，封存语义被破坏。
 *
 * <h2>确定性构造</h2>
 * 主线程先拿住 pid0 的写锁，再启动写线程（预检必然通过、然后阻塞在锁上）；
 * 确认写线程 parked 在锁上之后，主线程<b>持锁</b>完成与
 * {@code DefaultWal.freezePartition} 锁内段完全一致的状态迁移
 * （transitionTo(FROZEN) + setActiveSegment(null)），放锁。
 * 写线程醒来后必须被<b>锁内复核</b>拒绝——旧代码会走到 createNextSegment 复活段。
 */
class LockedStateRecheckTest {

    @Test
    @DisplayName("F2：冻结发生在写线程等锁期间 → 锁内复核拒绝，段不复活")
    void writerBlockedOnLockIsRejectedByInLockRecheck(@TempDir Path dir) throws Exception {
        WalConfig config = TestWal.builder(dir, 256L * 1024)
                .metricsEnabled(false)
                .buildConfig();
        HeapStorageBackend backend = new HeapStorageBackend();
        backend.init(new StorageConfig(dir, config.segmentSizeBytes(), false,
                config.mappedWindowBytes(), config.mappedWindowCount(),
                config.forceParentDirOnCreate()));
        PartitionRegistry registry = new PartitionRegistry(config.maxPartitions());
        LsnAllocator allocator = new LsnAllocator(1L);
        SegmentManager segmentManager = new SegmentManager(backend, registry, dir,
                config, allocator, null, null);
        PartitionImpl p0 = new PartitionImpl(0);
        registry.register(p0);
        p0.setActiveSegment(segmentManager.createFirstSegment(0, allocator.peek()));

        DirtyTracker tracker = new DirtyTracker(config.maxPartitions(), Long.MAX_VALUE);
        AppendPipeline pipeline = new AppendPipeline(registry, allocator,
                new FrameEncoder(new RawRecordCodec()), tracker, null,
                segmentManager, config, null);

        CountDownLatch writerStarted = new CountDownLatch(1);
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        AtomicReference<Long> writerLsn = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            writerStarted.countDown();
            try {
                writerLsn.set(pipeline.append(0, TestWal.payload(64), AppendOptions.DEFAULTS));
            } catch (Throwable t) {
                writerError.set(t);
            }
        }, "f2-writer");

        // ---- 主线程持锁，让写线程停在锁上（此刻锁外预检已通过）----
        p0.lock();
        try {
            writer.start();
            assertTrue(writerStarted.await(5, TimeUnit.SECONDS), "写线程必须先启动");
            assertTrue(awaitParkedOnLock(writer, 5_000L),
                    "写线程必须停在分区写锁上（预检已通过、尚未进临界区）");

            // ---- 持锁完成冻结（与 DefaultWal.freezePartition 的锁内段一致）----
            p0.transitionTo(PartitionInfo.State.FROZEN);
            p0.setActiveSegment(null);
        } finally {
            p0.unlock();
        }

        writer.join(TimeUnit.SECONDS.toMillis(10));
        assertEquals(Thread.State.TERMINATED, writer.getState(), "写线程必须在超时内结束");

        // ---- 写线程醒来：必须被锁内复核拒绝，而不是复活段 ----
        assertTrue(writerError.get() instanceof WalException.PartitionException,
                "写线程必须收到 PartitionException（锁内复核），实际: " + writerError.get());
        assertNull(writerLsn.get(), "冻结之后写线程绝不能成功写入");
        assertEquals(PartitionInfo.State.FROZEN, p0.state(), "分区状态必须是 FROZEN");
        assertEquals(1, segmentManager.segmentCount(0),
                "冻结后分区不得复活新段（旧代码会 createNextSegment，segmentCount=2）");

        pipeline.close();
        segmentManager.close();
        backend.close();
    }

    /**
     * 轮询等待线程停在（或阻塞在）分区锁上。
     *
     * @param thread 目标线程
     * @param timeoutMillis 超时
     * @return 确认 parked 返回 true；超时或已终止返回 false
     */
    private static boolean awaitParkedOnLock(Thread thread, long timeoutMillis) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.BLOCKED) {
                return true;
            }
            if (state == Thread.State.TERMINATED) {
                return false;
            }
            Thread.yield();
        }
        return false;
    }
}
