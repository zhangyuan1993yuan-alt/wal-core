package com.walcore.internal.groupcommit;

import com.walcore.api.Health;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.append.DirtyTracker;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.util.NamedThreadFactory;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 组提交协调器：<b>三重攒批 + 三档 fsync 策略 + 按分区真 fsync + 失败重试</b>（ARCH §7）。
 *
 * <h2>为什么"按分区 force"而不是"force 一个假装覆盖全部"</h2>
 * {@code fdatasync(fd)} 只保证<b>该 fd</b> 的数据落盘。若为了少一次系统调用而只 force 一个
 * 分区、却把所有分区的 {@code durableLsn} 都推进，那是<b>假持久性</b>——崩溃后会丢数据而框架
 * 声称已确认。PRD §5 要求"已确认写入丢失率 = 0"，因此这里对批内每个分区都调用
 * {@code handle.force(false)}。
 *
 * <p>性能上并不吃亏：多个 {@code fdatasync} 连续下发时，块层会把 flush 请求合并/并行处理，
 * 实测 16 分区 ≈ 300~800µs（不是 16 × 300µs）。{@code groupCommitThreads} 再把分区按
 * {@code pid % N} 拆成 N 条并行流。
 *
 * <h2>写入线程为什么不阻塞在 fsync 上</h2>
 * 写入线程只做 {@link #onAppend(int, int)}（DirtyTracker 的位图 set + 条带化计数，零分配、无锁 RMW），然后立刻返回。
 * 唯一可能的等待是背压（由 {@code AppendPipeline} 基于 {@link DirtyTracker#overHighWatermark()}
 * 决定），不在本类里。
 *
 * <h2>三重触发</h2>
 * <table border="1">
 *   <caption>触发条件</caption>
 *   <tr><th>维度</th><th>实现位置</th><th>作用</th></tr>
 *   <tr><td>时间窗 {@code batchIntervalMicros}</td>
 *       <td>{@code parkNanos} 的超时</td><td>决定 p99 延迟上界</td></tr>
 *   <tr><td>条数 {@code batchMaxRecords}</td>
 *       <td>{@link #onAppend} 里判断后 {@code unpark}</td><td>高负载下不等满窗</td></tr>
 *   <tr><td>字节 {@code batchMaxBytes}</td>
 *       <td>同上</td><td>防止大记录把单次 fsync 拖长</td></tr>
 * </table>
 *
 * <h2>失败处理（ARCH §7.6）</h2>
 * {@code force} 抛 {@link IOException} 时：置 {@link Health#DEGRADED}、
 * <b>绝不推进</b> {@code durableLsn}、脏位保留以便重试、之后每 {@code forceRetryMillis}
 * 重试一次，成功后恢复 {@link Health#HEALTHY}。
 * 线程被意外 {@code Throwable} 打死时最多重启 {@value #MAX_RESTARTS} 次，
 * 之后置 {@link Health#FAILED}（宁可显式失败，不可静默假活）。
 *
 * <h2>批次内失败为什么不能"就地返回"（T14 修复）</h2>
 * 脏位在进入批次<b>之前</b>就已被 {@link DirtyTracker#drainMine} 从全局位图取走，
 * 只存在于本批次的掩码快照里。因此失败后若直接 {@code return}，同批"尚未处理"
 * 的分区就永久丢失脏位——它们此后如果没有新写入，数据会<b>无限期</b>停在 page cache，
 * 而健康度又会被下一个成功批次翻回 {@link Health#HEALTHY}，属完全不报警的静默降级。
 * 现在的三步处理：
 * <ol>
 *   <li><b>不中断批次</b>：失败分区 {@link DirtyTracker#redirty} 后继续处理剩余分区，
 *       能刷成功的立刻刷（一个坏分区不拖累同批健康分区，也不延长它们的 RPO）；</li>
 *   <li><b>按水位真值结算等待者</b>：见 {@link #settleAfterFailure}——已达成水位目标的
 *       分区唤醒等待者（旧代码只在"整批全成功"时唤醒，导致已落盘的等待者白睡到超时、
 *       拿到假的 {@code false}），未达成的才上报 {@link WalException.IoException}；</li>
 *   <li><b>丢位兜底扫描</b>：{@code sweepStaleDirtyBits} 每秒把
 *       {@code publishedLsn > durableLsn} 的活跃分区重新置脏，把"位图丢位"这类
 *       静默缺陷的最坏后果从"无界延迟"压到"最多一个扫描周期"。</li>
 * </ol>
 *
 * <p><b>已知的有界代价（不修）</b>：失败后 {@code forceRetryMillis}（默认 5s）内
 * 本 flusher 整体退避，其名下分区的新写入要等退避结束才落盘。这段时间的 RPO 上界
 * 是退避时长，不是 {@code batchIntervalMicros}；换掉它需要按分区退避 + 每分区健康度，
 * 属独立设计项，本版本不做。
 */
public final class GroupCommitCoordinator implements FlushBatch.Listener, AutoCloseable {

    /** flusher 线程最大重启次数。 */
    private static final int MAX_RESTARTS = 3;

    /** 默认 force 失败后的重试间隔（毫秒），对应 ARCH §7.6 的 5s。 */
    public static final long DEFAULT_FORCE_RETRY_MILLIS = 5_000L;

    /** 关闭时等待 flusher 排空的默认上限。 */
    private static final long DEFAULT_DRAIN_TIMEOUT_MILLIS = 5_000L;

    /**
     * 丢位兜底扫描的间隔（纳秒）：1s。
     *
     * <p>取值权衡：这是"防静默丢位"的最后一道防线，只要求"有界"而不是"及时"，
     * 所以不需要跟着 {@code batchIntervalMicros} 走；1s 的 O(分区数) 只读扫描
     * （默认 16 次、上限 65536 次 {@code volatile} 读）对写路径零影响。
     */
    private static final long SWEEP_INTERVAL_NANOS = 1_000_000_000L;

    private final PartitionRegistry registry;
    private final DirtyTracker tracker;
    private final MetricsRegistry metrics;
    private final WalConfig.FsyncPolicy policy;
    private final int batchIntervalMicros;
    private final int batchMaxRecords;
    private final int batchMaxBytes;
    private final int flushIntervalMillis;
    private final long forceRetryMillis;
    private final int flusherCount;

    /** 每分区等待者队列（只在 {@code flushAndWait} 路径上分配，与 append 无关）。 */
    private final Map<Integer, ConcurrentLinkedQueue<FlushTicket>> waiters = new ConcurrentHashMap<>();

    private final AtomicLong batchCount = new AtomicLong();
    private final AtomicLong forcedPartitions = new AtomicLong();
    private final AtomicLong failedBatches = new AtomicLong();
    private final AtomicLong maxBatchRecords = new AtomicLong();
    private final AtomicLong maxBatchBytes = new AtomicLong();
    private final AtomicLong restarts = new AtomicLong();

    private final Flusher[] flushers;

    private volatile Health health = Health.HEALTHY;
    private volatile boolean running;
    private volatile Throwable lastFailure;

    /**
     * 构造协调器（不启动线程）。
     *
     * @param registry 分区注册表
     * @param tracker 脏追踪器
     * @param config 配置（读取策略与三个阈值）
     * @param metrics 指标（可为 null）
     */
    public GroupCommitCoordinator(PartitionRegistry registry, DirtyTracker tracker,
                                  WalConfig config, MetricsRegistry metrics) {
        this(registry, tracker, config, metrics, DEFAULT_FORCE_RETRY_MILLIS);
    }

    /**
     * 构造协调器（可指定重试间隔，便于测试）。
     *
     * @param registry 分区注册表
     * @param tracker 脏追踪器
     * @param config 配置
     * @param metrics 指标（可为 null）
     * @param forceRetryMillis force 失败后的重试间隔（毫秒）
     */
    public GroupCommitCoordinator(PartitionRegistry registry, DirtyTracker tracker,
                                  WalConfig config, MetricsRegistry metrics,
                                  long forceRetryMillis) {
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.tracker = Preconditions.checkNotNull(tracker, "tracker");
        WalConfig cfg = Preconditions.checkNotNull(config, "config");
        this.metrics = metrics;
        this.policy = cfg.fsyncPolicy();
        this.batchIntervalMicros = cfg.batchIntervalMicros();
        this.batchMaxRecords = cfg.batchMaxRecords();
        this.batchMaxBytes = cfg.batchMaxBytes();
        this.flushIntervalMillis = cfg.flushIntervalMillis();
        this.forceRetryMillis = Math.max(1L, forceRetryMillis);
        this.flusherCount = Math.max(1, cfg.groupCommitThreads());
        this.flushers = new Flusher[flusherCount];
        for (int i = 0; i < flusherCount; i++) {
            flushers[i] = new Flusher(i);
        }
    }

    // ------------------------------------------------------------------
    // 生命周期
    // ------------------------------------------------------------------

    /**
     * 启动 flusher 线程（幂等）。
     */
    public void start() {
        if (running) {
            return;
        }
        synchronized (this) {
            if (running) {
                return;
            }
            running = true;
            for (Flusher f : flushers) {
                f.start();
            }
        }
    }

    /**
     * 停止并排空，然后关闭。
     */
    @Override
    public void close() {
        drainAllAndStop(DEFAULT_DRAIN_TIMEOUT_MILLIS);
    }

    /**
     * 排空所有脏分区后停止 flusher（优雅关闭阶段 2，P0-10）。
     *
     * <p>顺序：先置 {@code running=false} 并唤醒线程，<b>再</b> join，
     * 最后做一次同步排空。若把"排空"放在 join 之前或把 {@code draining}
     * 之类标志放在 join 之后，会直接死锁（线程等标志、主线程等线程）。
     *
     * @param timeoutMillis 每个线程的 join 超时
     */
    public void drainAllAndStop(long timeoutMillis) {
        if (!running) {
            return;
        }
        running = false;
        for (Flusher f : flushers) {
            LockSupport.unpark(f.thread);
        }
        for (Flusher f : flushers) {
            try {
                f.join(timeoutMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 停车瞬间可能仍有未刷的脏分区（比如 flusher 正好在 park 中被叫停）
        forceAllDirty();
    }

    /**
     * 无视批次，立刻把所有脏分区同步刷一遍。
     *
     * @return 刷了的分区数
     */
    public int forceAllDirty() {
        long[] mask = new long[tracker.wordCount()];
        int upper = tracker.drain(mask);
        if (upper == 0) {
            return 0;
        }
        int forced = 0;
        int pid = tracker.nextDirtyPartition(mask, 0);
        while (pid >= 0) {
            PartitionImpl p = registry.get(pid);
            if (p != null) {
                // 先捕获水位，再 force：宁可少报，绝不假报
                long target = p.publishedLsn();
                try {
                    if (forcePartition(p, target)) {
                        forced++;
                    }
                    signalWaiters(pid, p.durableLsn());
                } catch (IOException e) {
                    lastFailure = e;
                    health = Health.DEGRADED;
                    failWaiters(pid, e);
                }
            }
            pid = tracker.nextDirtyPartition(mask, pid + 1);
        }
        return forced;
    }

    // ------------------------------------------------------------------
    // 写入侧入口（写线程调用，零分配）
    // ------------------------------------------------------------------

    /**
     * 记录一次写入（<b>写线程热路径</b>）。
     *
     * <p>本方法只有 {@link DirtyTracker#markDirty} 的几个原子操作，无分配、无锁、
     * 无系统调用。三个触发条件里的"条数"与"字节"在这里判断，命中就
     * {@code unpark} flusher——这是"高负载下不等满时间窗"的唯一实现点。
     *
     * <p><b>F8：不再自设计数器。</b>旧代码在 markDirty 之外再做
     * {@code recordsSinceDrain.incrementAndGet()} 与 {@code bytesSinceDrain.addAndGet()}
     * ——每条 append 多两次全局 RMW，与 markDirty 里的计数完全重复，纯粹在同一条
     * cache line 上加压。现在条数/字节阈值直接读 {@code DirtyTracker} 的全局计数
     * （条数是 LongAdder 条带化的 {@code sum()}，字节是 {@code AtomicLong.get()}），
     * 都是只读操作：读方不锁 cache line，写方仍只碰各自的 Cell。
     * 与旧计数"同一 drain 周期"等价——flusher 0 号在取走脏字节的同时清零这两个计数。
     *
     * @param partitionId 分区 ID
     * @param bytes 本次写入字节数
     */
    public void onAppend(int partitionId, int bytes) {
        tracker.markDirty(partitionId, bytes);
        if (policy == WalConfig.FsyncPolicy.PERIODIC) {
            return; // PERIODIC 只认时间窗，不认条数/字节
        }
        if (tracker.totalPendingRecords() >= batchMaxRecords
                || tracker.pendingBytes() >= batchMaxBytes) {
            requestFlush();
        }
    }

    /**
     * 请求尽快刷一轮（{@code unpark} 所有 flusher）。
     */
    public void requestFlush() {
        for (Flusher f : flushers) {
            LockSupport.unpark(f.thread);
        }
    }

    // ------------------------------------------------------------------
    // 等待持久化
    // ------------------------------------------------------------------

    /**
     * 等待某分区的 {@code lsn} 被持久化。
     *
     * <p><b>快路径</b>（{@code durableLsn >= lsn}）：直接返回，0 系统调用、0 分配。
     * 这是"同一批内多个 ACK 共用一个持久点"的命中路径，撮合类场景下命中率极高。
     *
     * @param partitionId 分区 ID
     * @param lsn 期望持久到的 LSN
     * @param timeoutMillis 超时
     * @throws WalException.IoException 刷盘失败
     * @throws WalException.TimeoutException 超时
     * @throws WalException.ClosedException 协调器已停止
     */
    public void awaitDurable(int partitionId, long lsn, long timeoutMillis) {
        PartitionImpl p = registry.get(partitionId);
        if (p == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        if (p.durableLsn() >= lsn) {
            return; // 快路径
        }
        if (!running) {
            throw new WalException.ClosedException("组提交已停止，无法等待 LSN=" + lsn + " 落盘");
        }
        Health h = health;
        if (h == Health.FAILED || h == Health.DEGRADED) {
            // 降级期间<b>不排队等待</b>：数据确实没落盘，排队只会让调用方白等满一个超时
            // 之后拿到一个注定失败的结论。立刻失败既省时间，也避免"以为在等、其实没人刷"。
            throw new WalException.IoException(
                    "组提交处于 " + h + " 状态，拒绝假确认 LSN=" + lsn,
                    lastFailure == null ? new IOException("group commit degraded") : lastFailure);
        }
        ConcurrentLinkedQueue<FlushTicket> queue = queueOf(partitionId);
        FlushTicket ticket = FlushTicket.acquire(partitionId, lsn);
        try {
            queue.add(ticket);
            // 入队后必须再查一次：flusher 可能在"读 durableLsn"与"入队"之间就刷完了，
            // 不再查就会白等到超时（丢失唤醒）
            if (p.durableLsn() >= lsn && queue.remove(ticket)) {
                ticket.signal();
            }
            ticket.await(timeoutMillis);
        } finally {
            // 幂等摘除：flusher 已取走时返回 false，此时它一定已经 signal 过
            queue.remove(ticket);
            ticket.close();
        }
    }

    /**
     * 取得（或创建）某分区的等待队列。
     *
     * @param partitionId 分区 ID
     * @return 队列
     */
    private ConcurrentLinkedQueue<FlushTicket> queueOf(int partitionId) {
        return waiters.computeIfAbsent(partitionId, pid -> new ConcurrentLinkedQueue<>());
    }

    /**
     * 唤醒所有已在持久水位之下的等待者。
     *
     * @param partitionId 分区 ID
     * @param durableLsn 该分区新的持久水位
     */
    private void signalWaiters(int partitionId, long durableLsn) {
        ConcurrentLinkedQueue<FlushTicket> queue = waiters.get(partitionId);
        if (queue == null || queue.isEmpty()) {
            return;
        }
        for (FlushTicket t : queue) {
            if (t.targetLsn() <= durableLsn && queue.remove(t)) {
                t.signal();
            }
        }
    }

    /**
     * 让某分区的<b>所有</b>等待者失败（用于"整批/整体不可用"的场景，如关闭期排空）。
     *
     * @param partitionId 分区 ID
     * @param cause 原因
     */
    private void failWaiters(int partitionId, Throwable cause) {
        failWaiters(partitionId, Long.MAX_VALUE, cause);
    }

    /**
     * 让某分区"目标 LSN 不超过 {@code maxTargetLsn}"的等待者失败，其余保持排队。
     *
     * <p>为什么要上界：批次是<b>按 {@code begin} 时刻的水位</b>捕获目标的，批次执行
     * 期间新追加的记录属于下一批。把 {@code targetLsn > maxTargetLsn} 的等待者也
     * 一并失败，等于"把还没尝试过的数据报成失败"，调用方重试就会产生重复事件。
     *
     * @param partitionId 分区 ID
     * @param maxTargetLsn 本批覆盖到的最大目标 LSN（含）
     * @param cause 原因
     */
    private void failWaiters(int partitionId, long maxTargetLsn, Throwable cause) {
        ConcurrentLinkedQueue<FlushTicket> queue = waiters.get(partitionId);
        if (queue == null) {
            return;
        }
        for (FlushTicket t : queue) {
            if (t.targetLsn() <= maxTargetLsn && queue.remove(t)) {
                t.fail(cause);
            }
        }
    }

    // ------------------------------------------------------------------
    // FlushBatch.Listener
    // ------------------------------------------------------------------

    @Override
    public void onBatchCompleted(FlushBatch batch) {
        batchCount.incrementAndGet();
        for (int word = 0; word < batch.upperWord(); word++) {
            long bits = batch.maskWord(word);
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                bits &= bits - 1L;
                int pid = (word << 6) + bit;
                PartitionImpl p = registry.get(pid);
                if (p != null) {
                    signalWaiters(pid, p.durableLsn());
                }
            }
        }
        maxBatchRecords.accumulateAndGet(batch.records(), Math::max);
        maxBatchBytes.accumulateAndGet(batch.bytes(), Math::max);
        if (metrics != null) {
            metrics.add(MetricsRegistry.BATCH_RECORDS, batch.records());
        }
    }

    @Override
    public void onBatchFailed(FlushBatch batch, int partitionId, Throwable cause) {
        failedBatches.incrementAndGet();
        lastFailure = cause;
        health = Health.DEGRADED;
        if (metrics != null) {
            // fsync 失败是"设备/文件系统写入失败"，不是"日志内容损坏"。
            // 记到 CORRUPTION_COUNT 会让运维把磁盘故障误判为数据损坏
            // （进而去跑恢复/修复流程），两者处置路径完全不同。
            metrics.increment(MetricsRegistry.FSYNC_FAILURE);
        }
        if (partitionId >= 0) {
            settleAfterFailure(batch, cause);
        } else {
            for (Integer pid : waiters.keySet()) {
                failWaiters(pid, cause);
            }
        }
    }

    /**
     * 批次失败后的等待者结算：<b>以 {@code durableLsn} 这个真值为准，而不是以
     * "处理到第几个分区"为准</b>。
     *
     * <p>旧实现只对失败的那一个分区 {@code failWaiters}，对本批<b>已经 force 成功</b>
     * 的分区一个字都不做——而 {@link #signalWaiters} 只在 {@link #onBatchCompleted}
     * 里调用，批次一旦走失败分支就永远不会执行。结果：数据明明已经落盘、等待者却
     * 睡满整个超时，{@code flushAndWait} 返回 {@code false}（假阴性）；调用方若据此
     * 判定"没落盘"并重试，还会产生重复业务事件（WAL 不做幂等去重）。
     *
     * <p>结算规则（逐个分区，{@code durableLsn} 是唯一判据）：
     * <ul>
     *   <li>{@code durableLsn >= 本批水位目标} → 已达成，{@link #signalWaiters} 唤醒；</li>
     *   <li>否则 → 只对"本批覆盖到的那部分"等待者（{@code targetLsn <= 水位目标}）
     *       报 {@link WalException.IoException}；批次执行期间新追加的等待者保持排队，
     *       等重试批次处理。</li>
     * </ul>
     *
     * <p>幂等性：{@link #signalWaiters} 与 {@link #failWaiters} 都先 {@code remove}
     * 再通知，同一张票不会被通知两次。
     *
     * @param batch 失败的批次（掩码仍是本批的完整快照）
     * @param cause 失败原因
     */
    private void settleAfterFailure(FlushBatch batch, Throwable cause) {
        for (int word = 0; word < batch.upperWord(); word++) {
            long bits = batch.maskWord(word);
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                bits &= bits - 1L;
                int pid = (word << 6) + bit;
                PartitionImpl p = registry.get(pid);
                if (p == null) {
                    continue;
                }
                long durable = p.durableLsn();
                long target = batch.maxLsnOf(pid);
                signalWaiters(pid, durable);
                if (durable < target) {
                    failWaiters(pid, target, cause);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 单次 force
    // ------------------------------------------------------------------

    /**
     * 对单个分区做一次真 fsync 并推进水位。
     *
     * <p>顺序不可变：<b>先 force，后 publish</b>，且 publish 的是<b>force 之前</b>捕获的
     * {@code targetLsn}。反过来的话会出现"水位已推进但数据未落盘"（假持久性），
     * 或者在 force 之后才读水位从而把"force 期间新写入的数据"也算作已持久。
     *
     * @param partition 分区
     * @param targetLsn force 之前捕获的水位目标
     * @return 成功返回 true；分区无活跃段（空分区）返回 false
     * @throws IOException force 失败
     */
    private boolean forcePartition(PartitionImpl partition, long targetLsn) throws IOException {
        SegmentHandle handle = partition.activeSegment() == null
                ? null : partition.activeSegment().handle();
        if (handle == null) {
            return false;
        }
        long started = System.nanoTime();
        handle.force(false);
        if (metrics != null) {
            metrics.increment(MetricsRegistry.FSYNC_COUNT);
            metrics.recordLatency(MetricsRegistry.FSYNC_LATENCY, System.nanoTime() - started);
        }
        partition.publishDurableLsn(targetLsn);
        tracker.clearPendingRecords(partition.id());
        forcedPartitions.incrementAndGet();
        return true;
    }

    // ------------------------------------------------------------------
    // flusher
    // ------------------------------------------------------------------

    /**
     * 单个 flusher 线程：按 {@code pid % flusherCount} 亲和只负责自己那份分区。
     *
     * <p>多 flusher 共享一张位图的技巧：每个 flusher 取走<b>整张</b>位图，只保留自己负责的位，
     * 其余位用 {@link DirtyTracker#markWordDirty} 原子 {@code OR} 回去。这样既不需要
     * "按 flusher 分片位图"，也不会丢并发标记。
     */
    private final class Flusher implements Runnable {

        private final int index;
        private final FlushBatch batch;
        /** 本 flusher 负责的位掩码（预计算，每轮只做一次 {@code get} 判断是否有活）。 */
        private final long[] myMask;
        private volatile Thread thread;
        private volatile long nextRetryNanos;
        /** 下次丢位扫描时刻：只有 0 号 flusher 读它，且只由本线程写，故不需要 volatile。 */
        private long nextSweepNanos;
        private volatile int restartCount;

        Flusher(int index) {
            this.index = index;
            this.batch = new FlushBatch(registry, tracker, GroupCommitCoordinator.this);
            this.myMask = new long[tracker.wordCount()];
            for (int w = 0; w < myMask.length; w++) {
                myMask[w] = DirtyTracker.bitsForWord(w, index, flusherCount,
                        registry.maxPartitions());
            }
        }

        void start() {
            Thread t = new NamedThreadFactory("wal-groupcommit-" + index, true)
                    .newThread(this);
            t.setPriority(Thread.NORM_PRIORITY + 1);
            this.thread = t;
            t.start();
        }

        void join(long timeoutMillis) throws InterruptedException {
            Thread t = this.thread;
            if (t != null) {
                t.join(timeoutMillis);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    tickLoop();
                    return; // 正常停止
                } catch (Throwable e) {
                    lastFailure = e;
                    int n = ++restartCount;
                    restarts.accumulateAndGet(n, Math::max);
                    if (n > MAX_RESTARTS) {
                        health = Health.FAILED;
                        running = false;
                        return;
                    }
                    health = Health.DEGRADED;
                }
            }
        }

        /**
         * 主循环：park（时间窗）→ 只取走自己的脏位 → force。
         *
         * <p>只取自己的位（{@link DirtyTracker#drainMine}）而不是"取全部再放回别人的"，
         * 是为了避免多 flusher 之间互相抢位导致的饥饿（详见 {@code drainMine} 的注释）。
         */
        private void tickLoop() {
            while (running) {
                long waitNanos = policy == WalConfig.FsyncPolicy.PERIODIC
                        ? flushIntervalMillis * 1_000_000L
                        : Math.max(1, batchIntervalMicros) * 1000L;
                LockSupport.parkNanos(this, waitNanos);
                if (!running) {
                    return;
                }
                if (System.nanoTime() < nextRetryNanos) {
                    continue; // 退避窗口内（上次 force 失败），不去撞系统调用
                }
                long drainedBytes = 0L;
                if (index == 0) {
                    // 全局计数只由 0 号 flusher 取走，避免 N 个线程把字节数切碎。
                    // F8：条数计数也归 DirtyTracker（LongAdder），取走即清零；
                    // 清零瞬间并发写入的增量可能被一并清掉（漏记不多记），
                    // 该计数只用于"是否提前唤醒"的建议判断，见 DirtyTracker 字段说明。
                    tracker.drainPendingRecords();
                    drainedBytes = tracker.drainBytes();
                    // 丢位兜底（每秒一次）：必须在 drainMine 之前，补的位当轮即可被取走。
                    sweepStaleDirtyBits();
                }
                int upper = tracker.drainMine(batch.maskBuffer(), myMask);
                if (upper == 0) {
                    continue;
                }
                runBatch(upper, drainedBytes);
            }
        }

        /**
         * 丢位兜底扫描：把"已有发布数据、但脏位不在位图上"的活跃分区重新置脏。
         *
         * <p><b>正常路径一条都不会补</b>——每次 append 都会 {@code markDirty}，
         * 批次失败也会按分区 {@code redirty}。这条扫描防的是"位图丢位"这一类
         * <b>静默</b>缺陷：漏置位、异常路径提前返回、将来重构把 drain/redirty 的
         * 配对改坏。这类缺陷一旦发生，数据会无限期停在 page cache 且没有任何报错
         * （健康度照样 HEALTHY），是 WAL 最难定位的失效形态。每秒一次 O(分区数)
         * 只读扫描，把最坏后果从"无界"压到"最多一个扫描周期"。
         *
         * <p>只对 {@code activeSegment() != null} 的分区置脏：没有活跃段的分区
         * （已封存/已移除）在 {@link #forcePartition} 里直接返回 {@code false}、
         * {@code durableLsn} 永远追不上 {@code publishedLsn}，对它们置脏会退化成
         * 每秒一次的空转批次。
         *
         * <p>只有 0 号 flusher 调用；字段 {@code nextSweepNanos} 也因此不需要同步。
         */
        private void sweepStaleDirtyBits() {
            long now = System.nanoTime();
            if (now < nextSweepNanos) {
                return;
            }
            nextSweepNanos = now + SWEEP_INTERVAL_NANOS;
            int max = registry.maxPartitions();
            for (int pid = 0; pid < max; pid++) {
                PartitionImpl p = registry.get(pid);
                if (p == null || p.activeSegment() == null) {
                    continue;
                }
                if (p.publishedLsn() > p.durableLsn()) {
                    tracker.redirty(pid);
                }
            }
        }

        /**
         * 对一个批次执行 force。
         *
         * <p><b>失败语义</b>：单个分区失败<b>不中断批次</b>。原因见类注释"批次内失败
         * 为什么不能就地返回"——脏位在本方法调用前就已经从全局位图取走，中途
         * {@code return} 会让同批剩余分区永久丢位。这里逐个分区 {@code redirty}
         * 失败者、继续处理后来者，最后再整体上报失败（供等待者结算与健康度降级）。
         *
         * @param upper 掩码有效上界
         * @param drainedBytes 本轮脏字节
         */
        private void runBatch(int upper, long drainedBytes) {
            batch.begin(drainedBytes, upper);
            int failedPid = -1;
            IOException firstFailure = null;
            for (int w = 0; w < upper; w++) {
                long bits = batch.maskWord(w);
                while (bits != 0L) {
                    int bit = Long.numberOfTrailingZeros(bits);
                    bits &= bits - 1L;
                    int pid = (w << 6) + bit;
                    PartitionImpl p = registry.get(pid);
                    if (p == null) {
                        continue;
                    }
                    try {
                        forcePartition(p, batch.maxLsnOf(pid));
                        batch.onPartitionForced(pid);
                    } catch (IOException e) {
                        // 失败：位图放回去等重试（水位绝不推进，等待者收异常）。
                        // 继续处理剩余分区——能成功的立刻落盘，失败的各记各的位。
                        tracker.redirty(pid);
                        if (failedPid < 0) {
                            failedPid = pid;
                            firstFailure = e;
                        }
                    }
                }
            }
            if (failedPid >= 0) {
                // 退避只推迟"重试失败分区"的时机；同批其余分区的 force 已经完成，
                // 其数据已落盘（等待者由 settleAfterFailure 按水位真值结算）。
                nextRetryNanos = System.nanoTime() + forceRetryMillis * 1_000_000L;
                batch.fail(failedPid, firstFailure);
                return;
            }
            if (health == Health.DEGRADED) {
                health = Health.HEALTHY; // 有一次成功 force 就恢复
            }
            batch.complete();
        }
    }

    // ------------------------------------------------------------------
    // 观测
    // ------------------------------------------------------------------

    /**
     * 当前健康度。
     *
     * @return 健康度
     */
    public Health health() {
        return health;
    }

    /**
     * 是否运行中。
     *
     * @return 运行返回 true
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * 最近一次失败原因。
     *
     * @return 原因；从未失败返回 null
     */
    public Throwable lastFailure() {
        return lastFailure;
    }

    /**
     * 刷盘统计快照。
     *
     * @return 快照
     */
    public FlushStats stats() {
        return new FlushStats(batchCount.get(), forcedPartitions.get(), failedBatches.get(),
                maxBatchRecords.get(), maxBatchBytes.get(), restarts.get(), flusherCount);
    }

    /**
     * 刷盘统计。
     *
     * @param batches 批次数
     * @param forcedPartitions 累计 force 的分区次数
     * @param failedBatches 失败批次数
     * @param maxBatchRecords 单批最大记录数
     * @param maxBatchBytes 单批最大字节数
     * @param restarts flusher 重启次数
     * @param flushers flusher 线程数
     */
    public record FlushStats(long batches, long forcedPartitions, long failedBatches,
                             long maxBatchRecords, long maxBatchBytes, long restarts,
                             int flushers) {
    }

    /**
     * flusher 线程数。
     *
     * @return 数量
     */
    public int flusherCount() {
        return flusherCount;
    }

    /**
     * fsync 策略。
     *
     * @return 策略
     */
    public WalConfig.FsyncPolicy policy() {
        return policy;
    }

    /**
     * 某分区当前的等待者数量（诊断用）。
     *
     * @param partitionId 分区 ID
     * @return 数量
     */
    public int waiterCount(int partitionId) {
        ConcurrentLinkedQueue<FlushTicket> q = waiters.get(partitionId);
        return q == null ? 0 : q.size();
    }

    @Override
    public String toString() {
        return "GroupCommitCoordinator{policy=" + policy + ", flushers=" + flusherCount
                + ", health=" + health + ", batches=" + batchCount.get()
                + ", forced=" + forcedPartitions.get() + '}';
    }
}
