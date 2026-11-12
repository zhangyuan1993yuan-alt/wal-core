package com.walcore.internal.segment;

import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.util.NamedThreadFactory;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 下一个段的后台<b>预建 + 预映射</b>线程：零阻塞轮转的关键（PRD P0-02 ③）。
 *
 * <h2>为什么必须有这个线程</h2>
 * 创建 256MB 的段文件（{@code setLength}）加 {@code mmap}，实测 1~3ms。如果把这件事放在
 * 写入线程的轮转路径上，每 256MB 就会出现一次毫秒级尖刺，p99 直接被毁掉。
 * 因此把它移到一个专属线程：轮转瞬间写入线程只做<b>一次引用交换</b>（&lt; 1µs），
 * 然后 {@code unpark} 本线程去补建下一份。
 *
 * <h2>降级路径</h2>
 * 突发流量把预建份额吃光时（{@code ready} 为空），写入线程只能<b>内联创建</b>。
 * 这是全链路唯一允许的超 1ms 尖刺，且必须被 {@code wal.rotate.fallback} 指标显式暴露——
 * "有尖刺但看不见"比"有尖刺"严重得多。
 *
 * <h2>命名的一个易错点</h2>
 * 预建段的名字用<b>当时的 {@code LSN} 计数器值</b>（{@code peek()}，不消耗），
 * 它可能显著小于该段实际首条记录的 LSN（预建发生在轮转之前）。
 * 这<b>不影响正确性</b>：{@code findSegmentFor} 只要求"名字 LSN ≤ 段内所有真实 LSN"，
 * 以及段间名字严格递增。反过来若名字比真实 LSN 大，二分就会漏段、导致"读不到日志"，
 * 所以 {@link SegmentManager} 在接收预建段时还会再校验一次严格递增。
 *
 * <h2>线程模型</h2>
 * 单线程（ARCH §4 线程表第 3 行）、daemon、最低优先级。空闲时 {@code parkNanos}，
 * 唤醒只有两条路：{@link #warmUp(int)} 的 {@code unpark}，或 20ms 周期兜底。
 * 周期兜底是为了防止"unpark 丢失"（{@code unpark} 在 {@code park} 之前到达时是合法的
 * 许可预存，但若线程正在处理别的分区则不会重试），代价可忽略。
 */
public final class NextSegmentPreparer implements AutoCloseable {

    /**
     * 计算"下一个段应该用的名字 LSN"。
     *
     * <p>抽成接口而不是直接传 {@code LsnAllocator}，是因为还需要考虑
     * "严格大于上一个段的名字"这条约束，该逻辑属于 {@link SegmentManager}。
     */
    public interface FirstLsnSource {

        /**
         * 返回分区下一个段可用的名字 LSN。
         *
         * @param partitionId 分区 ID
         * @return 名字 LSN（必须为正，且严格大于当前活跃段的名字）
         */
        long nextFirstLsn(int partitionId);
    }

    /** 空闲等待周期（纳秒）。20ms 足以让 CPU 完全休眠，又不至于让补建明显滞后。 */
    private static final long IDLE_WAIT_NANOS = 20_000_000L;

    /** 关闭时等待线程退出的上限（毫秒）。 */
    private static final long STOP_JOIN_MILLIS = 5_000L;

    private final StorageBackend backend;
    private final FirstLsnSource firstLsnSource;
    private final long segmentSizeBytes;
    private final MetricsRegistry metrics;
    private final String threadName;

    /** 已备好的段：{@code pid -> Segment}。取走即移除，因此天然"每次轮转独占一份"。 */
    private final Map<Integer, Segment> ready = new ConcurrentHashMap<>();

    /** 待补建的分区 ID 集合（并发安全，允许重复 add）。 */
    private final Set<Integer> wanted = ConcurrentHashMap.newKeySet();

    /**
     * 已取消预建的分区（分区被删除/冻结时）。
     *
     * <p>必须有这个集合，否则会出现"刚删完分区目录，预建线程又把文件建回来"的竞态：
     * 预建是在另一个线程里跑 {@code create → ATOMIC_MOVE}，删除方无法用"检查后再删"解决。
     * 正确做法是先把 pid 标记为取消，预建线程在 {@code create} 返回后<b>自己检查</b>
     * 并把刚建好的文件删掉。
     */
    private final Set<Integer> cancelled = ConcurrentHashMap.newKeySet();

    private final AtomicLong preparedCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong discardedCount = new AtomicLong();
    private final AtomicLong staleDiscardedCount = new AtomicLong();
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    private volatile boolean running = true;
    private volatile Thread thread;

    /**
     * 构造预建器（不启动线程；必须显式 {@link #start()}）。
     *
     * @param backend 存储后端
     * @param segmentSizeBytes 段容量
     * @param firstLsnSource 名字 LSN 来源
     * @param metrics 指标注册表（可为 null）
     * @param threadNamePrefix 线程名前缀
     */
    public NextSegmentPreparer(StorageBackend backend, long segmentSizeBytes,
                               FirstLsnSource firstLsnSource, MetricsRegistry metrics,
                               String threadNamePrefix) {
        this.backend = Preconditions.checkNotNull(backend, "backend");
        this.firstLsnSource = Preconditions.checkNotNull(firstLsnSource, "firstLsnSource");
        Preconditions.checkPositive(segmentSizeBytes, "segmentSizeBytes");
        this.segmentSizeBytes = segmentSizeBytes;
        this.metrics = metrics;
        this.threadName = (threadNamePrefix == null || threadNamePrefix.isEmpty()
                ? "wal-preparer" : threadNamePrefix + "-preparer");
    }

    /**
     * 启动后台线程（幂等）。
     */
    public void start() {
        if (thread != null) {
            return;
        }
        synchronized (this) {
            if (thread != null) {
                return;
            }
            running = true;
            Thread t = new NamedThreadFactory(threadName, true)
                    .newThread(this::runLoop);
            t.setPriority(Thread.MIN_PRIORITY);
            this.thread = t;
            t.start();
        }
    }

    /**
     * 请求为某分区预建下一段（通常由轮转路径在交换指针后调用）。
     *
     * @param partitionId 分区 ID
     */
    public void warmUp(int partitionId) {
        if (!running || partitionId < 0) {
            return;
        }
        if (cancelled.contains(partitionId)) {
            return;
        }
        if (ready.containsKey(partitionId)) {
            return;
        }
        wanted.add(partitionId);
        Thread t = this.thread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * 取消某分区的预建（分区被删除/冻结时），并丢弃已有储备。
     *
     * <p>与 {@link #warmUp(int)} 的区别：取消是"粘性"的，会一直生效直到
     * {@link #resume(int)}，因此不会出现"删完文件又被建回来"。
     *
     * @param partitionId 分区 ID
     */
    public void cancel(int partitionId) {
        cancelled.add(partitionId);
        wanted.remove(partitionId);
        discardReady(partitionId);
    }

    /**
     * 恢复某分区的预建（分区被重新创建时）。
     *
     * @param partitionId 分区 ID
     */
    public void resume(int partitionId) {
        cancelled.remove(partitionId);
    }

    /**
     * 该分区是否已被取消预建。
     *
     * @param partitionId 分区 ID
     * @return 已取消返回 true
     */
    public boolean isCancelled(int partitionId) {
        return cancelled.contains(partitionId);
    }

    /**
     * 取走已备好的段（<b>一次性</b>，取走后该分区即为"无储备"状态）。
     *
     * @param partitionId 分区 ID
     * @return 段；尚无储备返回 null（调用方须走内联降级）
     */
    public Segment takeReady(int partitionId) {
        return ready.remove(partitionId);
    }

    /**
     * 是否已有备好的段（诊断/测试用，不消耗）。
     *
     * @param partitionId 分区 ID
     * @return 有储备返回 true
     */
    public boolean hasReady(int partitionId) {
        return ready.containsKey(partitionId);
    }

    /**
     * 丢弃某分区的储备（接收方校验不通过时调用，避免留下孤儿文件）。
     *
     * @param partitionId 分区 ID
     * @return 被丢弃的段；无储备返回 null
     */
    public Segment discardReady(int partitionId) {
        Segment s = ready.remove(partitionId);
        if (s != null) {
            discardedCount.incrementAndGet();
            closeQuietly(s);
        }
        return s;
    }

    /**
     * 丢弃一个<b>已被取走</b>的储备段，并把它的文件从磁盘上删掉。
     *
     * <h2>为什么单独需要这个方法（T05 空段泄漏的根因修复）</h2>
     * 轮转路径是 {@code takeReady()} → 校验名字严格递增 → 通过才挂进段序列。校验不通过的
     * 那个段<b>已经从 {@code ready} 里被移走</b>，此时再调 {@link #discardReady(int)}
     * 只会返回 {@code null} 并<b>什么都不做</b>——段句柄不关、{@code .wal}/{@code .idx}
     * 文件不删。竞态一旦发生（内联降级抢先 {@code reserveNameLsn}，预建线程手里的名字
     * 反而更小），后续<b>每一次</b>轮转都会命中同一条分支，于是"每次轮转泄漏一个空段"，
     * 目录里的 {@code .wal} 数量持续多于逻辑段数，恢复时还要把这些空段逐个扫掉。
     *
     * <p>本方法把"取走后的段"也纳入统一收尸路径：关句柄 → 删 {@code .wal} → 删 {@code .idx}。
     *
     * @param segment 已被取走但未被采用的段，可为 null
     * @return 实际收尸返回 true
     */
    public boolean discardSegment(Segment segment) {
        if (segment == null) {
            return false;
        }
        discardedCount.incrementAndGet();
        staleDiscardedCount.incrementAndGet();
        closeQuietly(segment);
        return true;
    }

    /**
     * 因"名字不满足严格递增"而被丢弃的段数（诊断用，理想值为 0 或极小）。
     *
     * @return 数量
     */
    public long staleDiscardedCount() {
        return staleDiscardedCount.get();
    }

    /**
     * 后台主循环。
     *
     * <p>刻意做成"每次只处理一轮 wanted 就回到 park"：预建是低频操作（每 256MB 一次），
     * 不需要忙轮询，也避免与其他线程抢 CPU。
     */
    private void runLoop() {
        while (running) {
            if (wanted.isEmpty()) {
                // 空闲：不分配任何对象，直接 park（这是常态，占生命周期 99.99% 的时间）
                LockSupport.parkNanos(this, IDLE_WAIT_NANOS);
                continue;
            }
            // 使用快照遍历：wanted 是并发集合，允许在迭代期间被修改
            List<Integer> batch = new ArrayList<>(wanted);
            for (Integer pid : batch) {
                if (!running) {
                    break;
                }
                if (pid == null) {
                    continue;
                }
                if (ready.containsKey(pid)) {
                    wanted.remove(pid);
                    continue;
                }
                try {
                    Segment s = prepare(pid);
                    wanted.remove(pid);
                    if (s != null) {
                        ready.put(pid, s);
                        preparedCount.incrementAndGet();
                    }
                } catch (Throwable e) {
                    wanted.remove(pid);
                    failedCount.incrementAndGet();
                    lastFailure.set(e);
                    if (metrics != null) {
                        // 预建失败会让后续轮转走内联降级，与"轮转降级"是同一类事件
                        metrics.increment(MetricsRegistry.ROTATE_FALLBACK);
                    }
                }
            }
        }
        releaseAll();
    }

    /**
     * 真正创建并"预映射"一个段。
     *
     * <p>{@code backend.create} 内部已经是 {@code .tmp → ATOMIC_MOVE → mmap}，
     * 因此本方法返回时段的映射已就绪，轮转只需交换引用。
     *
     * @param partitionId 分区 ID
     * @return 段；已被别人抢先备好时返回 null
     * @throws IOException 创建失败
     */
    private Segment prepare(int partitionId) throws IOException {
        if (!running || cancelled.contains(partitionId) || ready.containsKey(partitionId)) {
            return null;
        }
        long firstLsn = firstLsnSource.nextFirstLsn(partitionId);
        SegmentHandle handle = backend.create(partitionId, firstLsn, segmentSizeBytes);
        Segment seg = new Segment(partitionId, firstLsn, handle);
        if (cancelled.contains(partitionId)) {
            // 创建期间分区被删除：自己收拾干净，否则目录里会留下"孤儿段"，
            // 导致分区目录无法删除、下次启动恢复时看到一个空段
            closeQuietly(seg);
            return null;
        }
        return seg;
    }

    /**
     * 释放所有未取走的储备（关闭线程时调用，防止文件句柄泄漏）。
     */
    private void releaseAll() {
        for (Integer pid : new ArrayList<>(ready.keySet())) {
            Segment s = ready.remove(pid);
            if (s != null) {
                closeQuietly(s);
            }
        }
    }

    /**
     * 关闭段句柄；若段已落盘则顺手删掉对应文件，避免重启后残留"空段"。
     *
     * @param s 段
     */
    private void closeQuietly(Segment s) {
        try {
            s.handle().close();
        } catch (IOException ignored) {
            // 关闭失败不改变上层语义
        }
        try {
            Files.deleteIfExists(s.handle().path());
            Files.deleteIfExists(idxSibling(s.handle().path()));
        } catch (IOException ignored) {
            // Windows 上可能因句柄未及时释放而失败，交由下次启动的清理逻辑处理
        }
    }

    /**
     * 推导同名 {@code .idx} 路径。
     *
     * @param walPath {@code .wal} 路径
     * @return {@code .idx} 路径
     */
    private static java.nio.file.Path idxSibling(java.nio.file.Path walPath) {
        String name = walPath.getFileName().toString();
        if (name.endsWith(SegmentNaming.WAL_EXT)) {
            return walPath.resolveSibling(name.substring(0,
                    name.length() - SegmentNaming.WAL_EXT.length()) + SegmentNaming.IDX_EXT);
        }
        return walPath;
    }

    /**
     * 停止线程并释放储备。
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        Thread t = this.thread;
        if (t != null) {
            LockSupport.unpark(t);
            try {
                t.join(STOP_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        releaseAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * 已成功预建的段数。
     *
     * @return 数量
     */
    public long preparedCount() {
        return preparedCount.get();
    }

    /**
     * 预建失败次数。
     *
     * @return 次数
     */
    public long failedCount() {
        return failedCount.get();
    }

    /**
     * 被丢弃的储备数（校验不通过的主机数量，理想值为 0）。
     *
     * @return 数量
     */
    public long discardedCount() {
        return discardedCount.get();
    }

    /**
     * 最近一次失败原因。
     *
     * @return 原因；从未失败返回 null
     */
    public Throwable lastFailure() {
        return lastFailure.get();
    }

    /**
     * 当前储备总量（所有分区）。
     *
     * @return 数量
     */
    public int readyCount() {
        return ready.size();
    }

    /**
     * 是否仍在运行。
     *
     * @return 运行中返回 true
     */
    public boolean isRunning() {
        return running;
    }
}
