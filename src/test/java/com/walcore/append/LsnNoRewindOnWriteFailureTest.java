package com.walcore.append;

import com.walcore.api.AppendOptions;
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
import com.walcore.spi.PluginContext;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F1 回归：写失败路径绝不回退 LSN 分配器（重复比空洞危害更大）。</b>
 *
 * <h2>被回归的缺陷</h2>
 * 旧 {@code AppendPipeline.append} 在 {@code IOException} 分支调用
 * {@code lsnAllocator.rewind(lsn)}。分区写锁只串行化<b>本分区</b>：
 * 失败窗口内（allocate 之后、rewind 之前）其他分区可以继续 allocate 并把
 * <b>更大的 LSN 落盘</b>。rewind 把全局计数器拉回 {@code lsn} 后，后续的 allocate
 * 会重新发出那些<b>已经发布</b>的 LSN——两条不同的记录拿到同一个 LSN，
 * 恢复期的按 LSN 全局归并重放直接错乱。
 *
 * <h2>确定性构造</h2>
 * 用一个可注错的 {@link StorageBackend} 包装：失败注入点在
 * {@link SegmentHandle#append}（此刻调用线程持有 pid0 的写锁、LSN 已分配）。
 * 失败<b>之前</b>在钩子里嵌套调用 {@code pipeline.append(1, ...)} 写另一个分区
 * （pid1 的锁空闲，必然成功）——单线程内精确复刻"失败窗口内其他分区拿到了更大 LSN"
 * 的交错。旧代码随后的 rewind 会让 pid0 的后续 append 重新发出嵌套写入已经用掉的
 * LSN，全局唯一性断言立刻失败。
 *
 * <h2>手工装配</h2>
 * 不走 {@code DefaultWal.open}：本测试只需要 StorageBackend + PartitionRegistry +
 * SegmentManager + AppendPipeline 这条写入链（组提交协调器传 null，纯内存路径）。
 */
class LsnNoRewindOnWriteFailureTest {

    /** 从失败异常消息里解析 "lsn=N"（AppendPipeline 的失败消息带上了已分配的 LSN）。 */
    private static final Pattern LSN_IN_MESSAGE = Pattern.compile("lsn=(\\d+)");

    // ==================================================================
    // 用例
    // ==================================================================

    @Test
    @DisplayName("F1：写失败窗口内其他分区写入更大 LSN → 全局 LSN 仍无重复")
    void noRewindWhenOtherPartitionWritesDuringFailureWindow(@TempDir Path dir) throws Exception {
        Fixture fx = new Fixture(dir, 0);

        // 前置：pid0 写 2 条（LSN 1,2），pid1 写 2 条（LSN 3,4）
        List<Long> all = new ArrayList<>();
        all.add(fx.pipeline.append(0, TestWal.payload(64), AppendOptions.DEFAULTS));
        all.add(fx.pipeline.append(0, TestWal.payload(64), AppendOptions.DEFAULTS));
        all.add(fx.pipeline.append(1, TestWal.payload(64), AppendOptions.DEFAULTS));
        all.add(fx.pipeline.append(1, TestWal.payload(64), AppendOptions.DEFAULTS));

        // ---- 制造确定性竞态 ----
        // pid0 的第 3 次 append 分配 LSN 5 后在 handle.append 处失败；
        // 失败前的钩子向 pid1 嵌套写入 2 条（LSN 6、7，必然成功并发布）。
        List<Long> nested = new ArrayList<>();
        fx.backend.beforeFailure = () -> {
            nested.add(fx.pipeline.append(1, TestWal.payload(64), AppendOptions.DEFAULTS));
            nested.add(fx.pipeline.append(1, TestWal.payload(64), AppendOptions.DEFAULTS));
        };
        fx.backend.failNext.set(true);

        WalException.IoException failure = assertThrows(WalException.IoException.class,
                () -> fx.pipeline.append(0, TestWal.payload(64), AppendOptions.DEFAULTS),
                "注入的 handle.append 失败必须以 IoException 冒泡");
        long failedLsn = parseLsn(failure);
        assertEquals(5L, failedLsn, "失败的那次 append 应持有 LSN 5（紧接前置 4 条）");
        assertEquals(List.of(6L, 7L), nested, "失败窗口内的嵌套写入应拿到 LSN 6、7");

        // ---- 关键断言：分配器从未回退 ----
        long peekAfterFailure = fx.allocator.peek();
        assertEquals(8L, peekAfterFailure,
                "失败后 next 必须仍是 8（不 rewind）。旧代码这里会是 5——正是重复 LSN 的来源");

        // ---- 失败之后继续写：全部 LSN 全局唯一 ----
        for (int i = 0; i < 5; i++) {
            all.add(fx.pipeline.append(0, TestWal.payload(64), AppendOptions.DEFAULTS));
            all.add(fx.pipeline.append(1, TestWal.payload(64), AppendOptions.DEFAULTS));
        }
        all.addAll(nested);

        Set<Long> unique = new HashSet<>(all);
        assertEquals(all.size(), unique.size(),
                "所有成功返回的 LSN 必须全局唯一。旧代码（rewind）会把 6、7 重新发给 pid0 的后续 append");

        assertTrue(unique.stream().noneMatch(l -> l == failedLsn),
                "失败的 LSN " + failedLsn + " 定义为从未存在，不得出现在任何成功结果里（允许空洞）");
        long finalPeek = fx.allocator.peek();
        assertEquals(18L, finalPeek,
                "收尾时 next 必须是 18（4 前置 + 1 失败 + 2 嵌套 + 10 后续 = 17 个已发出）。"
                        + "旧代码会因 rewind 被拉回 5");
        for (long lsn : all) {
            assertTrue(lsn < finalPeek, "分配器必须单调前进：已发出的 " + lsn + " 应小于 next");
        }
        fx.close();
    }

    @Test
    @DisplayName("F1：多分区并发写 + 间歇性写失败 → 全局 LSN 仍无重复")
    void concurrentWritesWithIntermittentFailuresKeepLsnUnique(@TempDir Path dir) throws Exception {
        Fixture fx = new Fixture(dir, 0);
        // 每 3 次 pid0 的 append 注入一次失败（无钩子，纯失败）
        AtomicInteger countdown = new AtomicInteger(3);
        fx.backend.failPredicate = () -> countdown.decrementAndGet() == 0;

        ConcurrentLinkedQueue<Long> seen = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        int threadsPerPartition = 3;
        int perThread = 400;
        List<Thread> writers = new ArrayList<>();
        for (int pid = 0; pid <= 1; pid++) {
            for (int t = 0; t < threadsPerPartition; t++) {
                final int partitionId = pid;
                Thread w = new Thread(() -> {
                    ByteBuffer payload = TestWal.payload(64);
                    for (int i = 0; i < perThread && !stop.get(); i++) {
                        payload.position(0);
                        try {
                            seen.add(fx.pipeline.append(partitionId, payload, AppendOptions.DEFAULTS));
                        } catch (WalException.IoException expected) {
                            // 注入的写失败：允许（这正是被测路径）
                        } catch (RuntimeException e) {
                            throw new AssertionError("并发写入只允许注定的 IoException，实际: " + e, e);
                        }
                    }
                }, "f1-writer-" + pid + "-" + t);
                writers.add(w);
                w.start();
            }
        }
        for (Thread w : writers) {
            w.join(60_000);
            assertEquals(Thread.State.TERMINATED, w.getState(), "写线程必须在超时内结束");
        }

        Set<Long> unique = new HashSet<>();
        seen.forEach(unique::add);
        assertEquals(seen.size(), unique.size(),
                "并发 + 间歇失败下所有成功 LSN 必须全局唯一（seen=" + seen.size() + "）");
        fx.close();
    }

    // ==================================================================
    // 夹具：手工装配写入链
    // ==================================================================

    /**
     * 一次装配：HeapStorageBackend（真实句柄语义、无文件系统副作用） +
     * 可注错包装 + 两分区注册 + 各自首段 + AppendPipeline。
     */
    private static final class Fixture implements AutoCloseable {
        final FailingBackend backend;
        final PartitionRegistry registry;
        final LsnAllocator allocator;
        final SegmentManager segmentManager;
        final AppendPipeline pipeline;

        Fixture(Path dir, int failingPid) throws IOException {
            WalConfig config = TestWal.builder(dir, 256L * 1024)
                    .metricsEnabled(false)
                    .buildConfig();
            HeapStorageBackend real = new HeapStorageBackend();
            real.init(new StorageConfig(dir, config.segmentSizeBytes(), false,
                    config.mappedWindowBytes(), config.mappedWindowCount(),
                    config.forceParentDirOnCreate()));
            this.backend = new FailingBackend(real, failingPid);

            this.registry = new PartitionRegistry(config.maxPartitions());
            this.allocator = new LsnAllocator(1L);
            this.segmentManager = new SegmentManager(backend, registry, dir,
                    config, allocator, null, null);

            for (int pid = 0; pid <= 1; pid++) {
                PartitionImpl partition = new PartitionImpl(pid);
                registry.register(partition);
                Segment first = segmentManager.createFirstSegment(pid, allocator.peek());
                partition.setActiveSegment(first);
            }

            DirtyTracker tracker = new DirtyTracker(config.maxPartitions(),
                    Long.MAX_VALUE);
            this.pipeline = new AppendPipeline(registry, allocator,
                    new FrameEncoder(new RawRecordCodec()), tracker, null,
                    segmentManager, config, null);
        }

        @Override
        public void close() {
            pipeline.close();
            segmentManager.close();
            backend.close();
        }
    }

    /**
     * 可注错的 StorageBackend：只对目标分区的句柄做包装，其余全部透传。
     */
    private static final class FailingBackend implements StorageBackend {
        private final StorageBackend delegate;
        private final int failingPid;

        /** 下一次 append 注入失败（一次性开关）。 */
        final AtomicBoolean failNext = new AtomicBoolean(false);
        /** 持续判据：返回 true 时本次 append 失败（并发场景）。 */
        volatile java.util.function.BooleanSupplier failPredicate;
        /** 失败前执行的钩子（在持有目标分区写锁、LSN 已分配的窗口内运行）。 */
        volatile Runnable beforeFailure;

        FailingBackend(StorageBackend delegate, int failingPid) {
            this.delegate = delegate;
            this.failingPid = failingPid;
        }

        @Override
        public SegmentHandle create(int partitionId, long firstLsn, long segmentSizeBytes)
                throws IOException {
            SegmentHandle handle = delegate.create(partitionId, firstLsn, segmentSizeBytes);
            return partitionId == failingPid ? new FailingHandle(handle, this) : handle;
        }

        @Override
        public SegmentHandle open(Path path, long firstLsn, boolean writable) throws IOException {
            SegmentHandle handle = delegate.open(path, firstLsn, writable);
            return path.toString().contains("p" + String.format("%04d", failingPid))
                    ? new FailingHandle(handle, this) : handle;
        }

        @Override
        public boolean supportsZeroCopyAppend() {
            return delegate.supportsZeroCopyAppend();
        }

        @Override
        public void init(StorageConfig config) throws IOException {
            delegate.init(config);
        }

        @Override
        public void delete(Path path) throws IOException {
            delegate.delete(path);
        }

        @Override
        public boolean accepts(Path path) {
            return delegate.accepts(path);
        }

        @Override
        public String id() {
            return delegate.id();
        }

        @Override
        public int apiVersion() {
            return delegate.apiVersion();
        }

        @Override
        public void init(PluginContext context) {
            delegate.init(context);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * 可注错的段句柄：append 时按后端的开关决定是否失败；失败前先跑钩子
     * （钩子里嵌套调用 pipeline 写其他分区，即被测竞态的确定性构造）。
     */
    private static final class FailingHandle implements SegmentHandle {
        private final SegmentHandle delegate;
        private final FailingBackend owner;

        FailingHandle(SegmentHandle delegate, FailingBackend owner) {
            this.delegate = delegate;
            this.owner = owner;
        }

        @Override
        public void append(ByteBuffer frame) throws IOException {
            boolean fail = owner.failNext.compareAndSet(true, false)
                    || (owner.failPredicate != null && owner.failPredicate.getAsBoolean());
            if (fail) {
                Runnable hook = owner.beforeFailure;
                if (hook != null) {
                    hook.run();
                }
                throw new IOException("注入的段写入失败（F1 回归测试）");
            }
            delegate.append(frame);
        }

        @Override
        public Path path() {
            return delegate.path();
        }

        @Override
        public long firstLsn() {
            return delegate.firstLsn();
        }

        @Override
        public long capacityBytes() {
            return delegate.capacityBytes();
        }

        @Override
        public long appendPosition() {
            return delegate.appendPosition();
        }

        @Override
        public void restoreAppendPosition(long position) throws IOException {
            delegate.restoreAppendPosition(position);
        }

        @Override
        public int read(long fileOffset, ByteBuffer dst, int len) throws IOException {
            return delegate.read(fileOffset, dst, len);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            delegate.force(metaData);
        }

        @Override
        public void truncate(long newLength) throws IOException {
            delegate.truncate(newLength);
        }

        @Override
        public long physicalLength() throws IOException {
            return delegate.physicalLength();
        }

        @Override
        public boolean isMapped() {
            return delegate.isMapped();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 从 AppendPipeline 的失败消息里解析已分配的 LSN。
     *
     * @param e 失败异常
     * @return LSN
     */
    private static long parseLsn(WalException.IoException e) {
        Matcher m = LSN_IN_MESSAGE.matcher(String.valueOf(e.getMessage()));
        assertTrue(m.find(), "失败消息必须携带 lsn=N，实际: " + e.getMessage());
        return Long.parseLong(m.group(1));
    }
}
