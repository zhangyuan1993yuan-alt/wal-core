package com.walcore.internal.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.Health;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.groupcommit.GroupCommitCoordinator;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 写入管线主流程（ARCH §13.1）：<b>锁 → 段就绪 → 分配 LSN → 编码 → 写帧 → 发布水位 → 标脏</b>。
 *
 * <h2>顺序为什么是这个顺序（改一行就会破坏不变量）</h2>
 * <ol>
 *   <li><b>先拿分区写锁</b>：同分区的写入必须串行，否则段内字节顺序无法与 LSN 顺序一致；</li>
 *   <li><b>再确认段有空间</b>：空间不足先轮转，保证后面的 {@code appendFrame} 不会因为
 *       "写不下"而中途失败（失败会留下半帧）；</li>
 *   <li><b>然后在锁内分配 LSN，且紧邻写帧</b>：这是 P0-01 ① 的根因级要求。若在锁外分配，
 *       "拿到 LSN 小的线程先被换出、拿到 LSN 大的线程先写"，文件里就会出现 LSN 逆序；</li>
 *   <li><b>编码到 staging 区，编码成功后才写段</b>：这是"插件故障不损坏日志"的核心机制
 *       （ARCH §10.4.4）——codec 抛异常时偏移根本没动，段里不会留半帧；</li>
 *   <li><b>{@code publishLsn} 用 release 语义</b>：读者 {@code getAcquire} 之后能看到
 *       完整的段内字节，才能安全地读 {@code [0, publishedWritePos)}。</li>
 * </ol>
 *
 * <h2>锁外做的事</h2>
 * {@code coordinator.onAppend(pid, bytes)}（标脏 + 可能唤醒刷盘线程）放在锁外：
 * 它只碰原子量，不需要段与 LSN 的顺序保证；放在锁内会白白拉长临界区。
 *
 * <h2>零对象分配（P0-01 ③）</h2>
 * 稳态下本类在 {@link #append} 上的分配为 0：
 * <ul>
 *   <li>编码缓冲是 {@code StagingBuffer}（每分区一个，惰性创建）或线程本地 scratch，
 *       二者都只在<b>首次使用或帧变大</b>时分配；</li>
 *   <li>不走 {@code Stream}/{@code Optional}/装箱/字符串拼接；</li>
 *   <li>异常只在真正的错误分支上创建。</li>
 * </ul>
 */
public final class AppendPipeline implements AutoCloseable {

    /** 单帧超过 staging 容量时改用线程本地 scratch 的阈值计数指标名。 */
    public static final String METRIC_OVERSIZE = "wal.append.oversizeFrames";

    /**
     * 线程本地的大帧 scratch 上限：超过此值的帧用完即弃，避免单个线程长期驻留 MB 级堆外内存。
     */
    private static final int SCRATCH_RETAIN_LIMIT = 1 << 20;

    private static final ThreadLocal<ByteBuffer> LARGE_SCRATCH = new ThreadLocal<>();

    private final PartitionRegistry registry;
    private final LsnAllocator lsnAllocator;
    private final FrameEncoder encoder;
    private final DirtyTracker dirtyTracker;
    private final GroupCommitCoordinator coordinator;
    private final SegmentManager segmentManager;
    private final WalConfig config;
    private final MetricsRegistry metrics;

    /** 每分区一个 staging 区（惰性创建）；下标即分区 ID。 */
    private final StagingBuffer[] arenas;
    /**
     * 每分区一个"提交视图"：与 arena 共享底层字节，但拥有独立的 position/limit。
     *
     * <p>为什么需要它：{@code SegmentHandle.append} 只认 {@code [position, limit)}，
     * 而 {@code encodeFrame} 用的是<b>绝对下标</b>写入、按 {@code limit()} 校验容量。
     * 若直接改 arena 主缓冲的 limit，下一次编码就会因为 limit 偏小抛
     * {@code BufferOverflowException}。用独立视图既能零分配，又互不干扰。
     */
    private final ByteBuffer[] arenaViews;
    private final AtomicLong oversizeFrames = new AtomicLong();

    /**
     * append 延迟直方图的抽样掩码：{@code (lsn & MASK) == 0} 时采一个样本，即 1/64。
     *
     * <h2>为什么用 LSN 取模抽样，而不是"每 N 次计数一次"</h2>
     * <ul>
     *   <li>LSN 本来就在锁内分配好了，{@code & MASK} 是一条寄存器指令，<b>零额外内存访问</b>；
     *       用计数器就要再加一个跨线程原子量，等于给热路径加一次 CAS；</li>
     *   <li>LSN 全局单调，取模后样本在时间上均匀散布，不会"集中落在同一个分区的同一批写入里"；</li>
     *   <li>抽样比例可解释：1/64 下 100 万条/秒的日志每秒仍有 1.5 万个样本，
     *       p999 的统计误差可以忽略。</li>
     * </ul>
     */
    private static final long APPEND_LATENCY_SAMPLE_MASK = 63L;

    /**
     * 预取出来的指标句柄（构造期一次性取得，热路径上<b>不再查 Map</b>）。
     *
     * <p>这三个字段就是把"埋点开销 &lt; 1%"落到实处的关键：{@code metrics.add(name, 1)}
     * 每次都要在 {@code ConcurrentHashMap} 上做一次 {@code get}（约 5~8ns，且在高并发下
     * 会命中同一个 cache line），而直接持有 {@code LongAdder} 之后就只剩一次
     * {@code add}（无竞争时约 2~4ns）。{@code metricsEnabled} 为 false 时三者都是 null，
     * 热路径上只有一个 boolean 判断。
     */
    private final java.util.concurrent.atomic.LongAdder mAppendRecords;
    private final java.util.concurrent.atomic.LongAdder mAppendBytes;
    private final com.walcore.internal.metrics.LatencyHistogram mAppendLatency;
    private final boolean metricsEnabled;

    private volatile boolean closed;

    /**
     * 构造写入管线。
     *
     * @param registry 分区注册表
     * @param lsnAllocator 全局 LSN 分配器
     * @param encoder 帧编码器
     * @param dirtyTracker 脏分区位图
     * @param coordinator 组提交协调器，可为 null（仅测试"纯写入"链路）
     * @param segmentManager 段管理器
     * @param config 配置
     * @param metrics 指标注册中心，可为 null
     */
    public AppendPipeline(PartitionRegistry registry, LsnAllocator lsnAllocator,
                          FrameEncoder encoder, DirtyTracker dirtyTracker,
                          GroupCommitCoordinator coordinator, SegmentManager segmentManager,
                          WalConfig config, MetricsRegistry metrics) {
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.lsnAllocator = Preconditions.checkNotNull(lsnAllocator, "lsnAllocator");
        this.encoder = Preconditions.checkNotNull(encoder, "encoder");
        this.dirtyTracker = Preconditions.checkNotNull(dirtyTracker, "dirtyTracker");
        this.coordinator = coordinator;
        this.segmentManager = Preconditions.checkNotNull(segmentManager, "segmentManager");
        this.config = Preconditions.checkNotNull(config, "config");
        this.metrics = metrics;
        this.arenas = new StagingBuffer[config.maxPartitions()];
        this.arenaViews = new ByteBuffer[config.maxPartitions()];
        if (metrics != null && metrics.enabled()) {
            this.metricsEnabled = true;
            this.mAppendRecords = metrics.counter(MetricsRegistry.APPEND_RECORDS);
            this.mAppendBytes = metrics.counter(MetricsRegistry.APPEND_BYTES);
            this.mAppendLatency = metrics.histogram(MetricsRegistry.APPEND_LATENCY);
        } else {
            this.metricsEnabled = false;
            this.mAppendRecords = null;
            this.mAppendBytes = null;
            this.mAppendLatency = null;
        }
    }

    // ==================================================================
    // 写入
    // ==================================================================

    /**
     * 追加一条记录（纯内存路径，不 fsync）。
     *
     * @param partitionId 分区 ID
     * @param payload 业务字节，读取其 {@code [position, limit)}
     * @param options 追加选项，不得为 null
     * @return 全局唯一、严格递增的 LSN
     * @throws WalException.ClosedException 已关闭
     * @throws WalException.PartitionException 分区不存在或不可写
     * @throws WalException.BackpressureException 背压超时 / 单帧超过段容量
     */
    public long append(int partitionId, ByteBuffer payload, AppendOptions options) {
        Preconditions.checkNotNull(payload, "payload");
        Preconditions.checkNotNull(options, "options");
        if (closed) {
            throw new WalException.ClosedException("WAL 已关闭，拒绝写入（partitionId="
                    + partitionId + "）");
        }
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        if (!partition.isWritable()) {
            throw new WalException.PartitionException(
                    "分区不可写（state=" + partition.state() + "）: " + partitionId);
        }

        final int payloadLen = payload.remaining();
        if (payloadLen > config.maxPayloadBytes()) {
            throw new WalException.PartitionException(
                    "单条 payload 超过配置上限 maxPayloadBytes=" + config.maxPayloadBytes()
                            + "，实际 " + payloadLen + " 字节（partitionId=" + partitionId + "）");
        }
        final int frameLen = encoder.requiredFrameLen(payloadLen);

        long lsn;
        long latencyNanos = -1L;
        partition.lock();
        try {
            // ---- F2：锁内复核可写状态 ----
            // removePartition/close 的状态迁移都在同一个分区写锁内完成
            // （见 DefaultWal.freezePartition / close）。锁外的 isWritable 预检存在
            // TOCTOU 窗口：一个"预检通过后才被冻结"的分区若不在锁内复核，仍会被
            // ensureWritableSegment 复活出新段。复核只是一次 volatile 读。
            if (!partition.isWritable()) {
                throw new WalException.PartitionException(
                        "分区不可写（state=" + partition.state() + "）: " + partitionId);
            }
            Segment segment = ensureWritableSegment(partitionId, frameLen);
            // ---- 关键：LSN 在锁内分配，且紧邻写帧 ----
            lsn = lsnAllocator.allocate();
            // 埋点抽样：只在被抽中时才读一次时钟（nanoTime 在 Windows 上约 25ns，
            // 逐条读会吃掉 append 预算的 5% 以上，抽样后摊薄到 0.04%）
            boolean sample = metricsEnabled && (lsn & APPEND_LATENCY_SAMPLE_MASK) == 0L;
            long started = sample ? System.nanoTime() : 0L;
            ByteBuffer frame = encodeInto(partitionId, frameLen, lsn, payload, options);
            try {
                segment.appendFrame(frame, lsn);
            } catch (IOException e) {
                // ---- F1：写失败时 LSN 已经消耗，刻意【不回退】分配器 ----
                // 分区写锁只串行化本分区：其他分区的写线程此刻可能已经把
                // <b>更大的</b> LSN 写进了文件。这里 rewind 会让后续 allocate
                // 重新发出与已落盘记录相同的 LSN——重复 LSN 会破坏恢复期的全局
                // 归并重放，<b>重复比空洞危害更大</b>。失败的 append 因此定义为
                // "该 LSN 从未存在"，留下的空洞由恢复期的 max(lastLsn)+1 语义吸收。
                throw new WalException.IoException(
                        "写入段失败: pid=" + partitionId + ", lsn=" + lsn, e);
            }
            partition.publishLsn(lsn);
            if (sample) {
                latencyNanos = System.nanoTime() - started;
            }
        } finally {
            partition.unlock();
        }

        if (metricsEnabled) {
            // ---- F8：计数只走 LongAdder（条带化，高并发下无 cache line 争用）----
            // 此前这里还有 appendCount/appendBytes 两个全局 AtomicLong 与指标重复
            // 记账，多写线程时是纯粹的全局争用点，已删除。
            mAppendRecords.increment();
            mAppendBytes.add(frameLen);
            if (latencyNanos >= 0L) {
                mAppendLatency.record(latencyNanos);
            }
        }
        if (coordinator != null) {
            coordinator.onAppend(partitionId, frameLen);
        }
        return lsn;
    }

    /**
     * 追加并等待持久化。
     *
     * @param partitionId 分区 ID
     * @param payload 业务字节
     * @param options 追加选项
     * @return 已持久化的 LSN
     */
    public long appendSync(int partitionId, ByteBuffer payload, AppendOptions options) {
        long lsn = append(partitionId, payload, options);
        if (coordinator == null) {
            return lsn;
        }
        coordinator.awaitDurable(partitionId, lsn, config.flushAndWaitTimeoutMillis());
        return lsn;
    }

    /**
     * 批量追加到同一分区。
     *
     * <p>整批在<b>同一个分区写锁临界区</b>内完成，因此返回的 LSN 数组天然连续
     * （{@code result[i+1] == result[i] + 1}），这是"无空洞"不变量的直接体现，
     * 也是上层做批量确认的可靠依据。
     *
     * @param partitionId 分区 ID
     * @param payloads 业务字节列表
     * @return LSN 数组，与入参一一对应
     */
    public long[] appendBatch(int partitionId, List<ByteBuffer> payloads) {
        Preconditions.checkNotNull(payloads, "payloads");
        if (payloads.isEmpty()) {
            return new long[0];
        }
        if (closed) {
            throw new WalException.ClosedException("WAL 已关闭，拒绝写入（partitionId="
                    + partitionId + "）");
        }
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        if (!partition.isWritable()) {
            throw new WalException.PartitionException(
                    "分区不可写（state=" + partition.state() + "）: " + partitionId);
        }
        long[] lsns = new long[payloads.size()];
        long bytes = 0L;
        partition.lock();
        try {
            // F2：锁内复核可写状态（理由同 append()，见那里的注释）
            if (!partition.isWritable()) {
                throw new WalException.PartitionException(
                        "分区不可写（state=" + partition.state() + "）: " + partitionId);
            }
            for (int i = 0; i < payloads.size(); i++) {
                ByteBuffer payload = payloads.get(i);
                Preconditions.checkNotNull(payload, "payloads[" + i + "]");
                int payloadLen = payload.remaining();
                if (payloadLen > config.maxPayloadBytes()) {
                    throw new WalException.PartitionException(
                            "payloads[" + i + "] 超过 maxPayloadBytes=" + config.maxPayloadBytes());
                }
                int frameLen = encoder.requiredFrameLen(payloadLen);
                Segment segment = ensureWritableSegment(partitionId, frameLen);
                long lsn = lsnAllocator.allocate();
                ByteBuffer frame = encodeInto(partitionId, frameLen, lsn, payload,
                        AppendOptions.DEFAULTS);
                try {
                    segment.appendFrame(frame, lsn);
                } catch (IOException e) {
                    // F1：同 append()——LSN 已消耗，不回退（重复比空洞危害更大）
                    throw new WalException.IoException(
                            "批量写入段失败: pid=" + partitionId + ", index=" + i, e);
                }
                partition.publishLsn(lsn);
                lsns[i] = lsn;
                bytes += frameLen;
            }
        } finally {
            partition.unlock();
        }
        if (metricsEnabled) {
            mAppendRecords.add(lsns.length);
            mAppendBytes.add(bytes);
        }
        if (coordinator != null) {
            // 一批只标一次脏，但字节与条数都要如实上报
            coordinator.onAppend(partitionId, (int) Math.min(Integer.MAX_VALUE, bytes));
        }
        return lsns;
    }

    // ==================================================================
    // 状态
    // ==================================================================

    /**
     * 只做可写性检查（供定时任务/管理接口在真正写入前预检）。
     *
     * @param partitionId 分区 ID
     * @throws WalException.ClosedException 已关闭
     * @throws WalException.PartitionException 分区不存在或不可写
     */
    public void checkWritable(int partitionId) {
        if (closed) {
            throw new WalException.ClosedException("WAL 已关闭");
        }
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        if (!partition.isWritable()) {
            throw new WalException.PartitionException(
                    "分区不可写（state=" + partition.state() + "）: " + partitionId);
        }
    }

    /**
     * 停止接收新写入，并释放所有 staging 区的堆外内存。
     */
    @Override
    public void close() {
        close(Collections.emptySet());
    }

    /**
     * 停止接收新写入；{@code deferArenaClose} 里的分区<b>跳过</b> staging 区的立即 unmap。
     *
     * <p><b>为什么需要这个参数（F2）</b>：调用方（{@code DefaultWal.close}）在关闭前会
     * 先按分区拿写锁把 in-flight 写排干。拿不到锁的分区意味着仍有写线程在该分区的
     * 临界区内（正在往 arena 里编码帧）——立刻 unmap 是 use-after-unmap（native 崩溃）。
     * 这些 arena 只能延迟到实例 GC 时由 DirectBuffer cleaner 释放，调用方必须在
     * {@code CloseReport} 里如实报告这一事实。
     *
     * @param deferArenaClose 跳过立即释放的分区 ID 集合；空集合等价于 {@link #close()}
     */
    public void close(Set<Integer> deferArenaClose) {
        this.closed = true;
        Set<Integer> defer = deferArenaClose == null ? Collections.emptySet() : deferArenaClose;
        for (int pid = 0; pid < arenas.length; pid++) {
            StagingBuffer arena = arenas[pid];
            if (arena != null && !defer.contains(pid)) {
                arena.close();
            }
        }
    }

    /**
     * 是否已关闭。
     *
     * @return 已关闭返回 true
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * 因超过 staging 容量而改用线程本地 scratch 的帧数。
     *
     * <p>非零只表示"有过大帧"，不是错误；但它意味着该分区的堆外占用会临时升高，
     * 值得在 {@code wal.stats()} 里观察。
     *
     * @return 帧数
     */
    public long oversizeFrames() {
        return oversizeFrames.get();
    }

    /**
     * 取某分区的 staging 区（诊断/测试用，可能为 null）。
     *
     * @param partitionId 分区 ID
     * @return staging 区
     */
    public StagingBuffer stagingArena(int partitionId) {
        if (partitionId < 0 || partitionId >= arenas.length) {
            return null;
        }
        return arenas[partitionId];
    }

    // ==================================================================
    // 内部实现
    // ==================================================================

    /**
     * 保证活跃段能容纳 {@code frameLen} 字节，必要时轮转。
     *
     * <p><b>调用方必须已持有分区写锁</b>。
     */
    private Segment ensureWritableSegment(int partitionId, int frameLen) {
        // 背压场景②：脏字节超过高水位时不再继续灌数据（除非策略是 ALWAYS，那种情况下
        // 每条都同步刷，永远不会积压到高水位）
        if (dirtyTracker != null && dirtyTracker.overHighWatermark()) {
            applyBackpressure(partitionId, "脏字节超过高水位 maxDirtyBytes="
                    + config.maxDirtyBytes() + "，当前 " + dirtyTracker.pendingBytes());
        }

        Segment segment = segmentManager.activeSegment(partitionId);
        if (segment == null) {
            try {
                // 走到这里有两种情况：分区首次写入（还没有任何段），或恢复后旧段全部
                // 只读（clearActive 清空了活跃指针）。后者必须用 createNextSegment
                // 发号，绝不能按 peek() 命名——那会撞上既有段名。
                segment = segmentManager.createNextSegment(partitionId);
            } catch (IOException e) {
                throw new WalException.IoException(
                        "分区 " + partitionId + " 无活跃段且创建失败", e);
            }
        }
        if (segment.hasRoom(frameLen)) {
            return segment;
        }
        // 背压场景③：段未就绪 / 单帧大于段容量
        if (frameLen > segment.capacityBytes()) {
            throw new WalException.BackpressureException(
                    "单帧 " + frameLen + " 字节超过段容量 " + segment.capacityBytes()
                            + "（partitionId=" + partitionId
                            + "，请调大 segmentSizeBytes）");
        }
        try {
            Segment next = segmentManager.rotateLocked(partitionId);
            if (!next.hasRoom(frameLen)) {
                throw new WalException.BackpressureException(
                        "轮转后新段仍无法容纳单帧 " + frameLen + " 字节（partitionId="
                                + partitionId + "）");
            }
            return next;
        } catch (IOException e) {
            throw new WalException.IoException("分区 " + partitionId + " 轮转失败", e);
        }
    }

    /**
     * 按 {@code backpressurePolicy} 处理"写入过快"。
     *
     * <p>{@code BLOCK}：park 到脏字节降下来或超时；{@code FAIL}：立即抛。
     */
    private void applyBackpressure(int partitionId, String reason) {
        if (config.backpressurePolicy() == WalConfig.BackpressurePolicy.FAIL) {
            throw new WalException.BackpressureException(
                    "背压（FAIL 策略）: " + reason + "（partitionId=" + partitionId + "）");
        }
        long deadline = System.nanoTime() + config.backpressureTimeoutMillis() * 1_000_000L;
        while (dirtyTracker.overHighWatermark()) {
            if (System.nanoTime() > deadline) {
                throw new WalException.BackpressureException(
                        "背压超时（" + config.backpressureTimeoutMillis() + "ms）: " + reason
                                + "（partitionId=" + partitionId + "）");
            }
            if (coordinator != null) {
                // 主动催一次刷盘，避免"等脏字节降下来"却没人去刷的死等
                coordinator.requestFlush();
            }
            Thread.yield();
        }
    }

    /**
     * 把整帧编码进可复用的缓冲区，返回"position=0、limit=frameLen"的视图。
     *
     * <p>返回值是<b>本线程独占</b>的缓冲区（staging 区按分区独占，大帧 scratch 按线程独占），
     * 因此调用方可以直接把它交给 {@code SegmentHandle.append}，无需再复制。
     */
    private ByteBuffer encodeInto(int partitionId, int frameLen, long lsn,
                                  ByteBuffer payload, AppendOptions options) {
        StagingBuffer arena = arenaOf(partitionId);
        if (arena != null && frameLen <= arena.capacity()) {
            int offset = arena.reserve(frameLen, config.backpressureTimeoutMillis());
            ByteBuffer buffer = arena.buffer();
            int encoded = encoder.encodeFrame(buffer, offset, frameLen, lsn, partitionId,
                    payload, options);
            if (encoded != frameLen) {
                // 容量已按 requiredFrameLen 预留，走到这里说明 codec 的 maxStoredSize 契约被破坏
                throw new WalException.CodecException(
                        "codec 实际帧长 " + encoded + " 与预估 " + frameLen + " 不一致"
                                + "（codecId=" + encoder.codecId() + "），请检查 maxStoredSize 实现");
            }
            arena.commit(frameLen);
            // 独立视图圈出 [offset, offset+frameLen)：零分配，且不动 arena 主缓冲的 limit
            ByteBuffer view = arenaViews[partitionId];
            view.limit(offset + frameLen);
            view.position(offset);
            // 本帧马上被同步拷进段文件，staging 不累积，可以立即回收空间
            arena.flushedTo(offset + frameLen);
            return view;
        }
        // 大帧 / 未启用 staging：用线程本地 scratch
        oversizeFrames.incrementAndGet();
        if (metrics != null) {
            metrics.increment(METRIC_OVERSIZE);
        }
        ByteBuffer scratch = LARGE_SCRATCH.get();
        if (scratch == null || scratch.capacity() < frameLen) {
            int capacity = Math.max(frameLen, 1 << 16);
            scratch = ByteBuffer.allocateDirect(capacity);
            if (capacity <= SCRATCH_RETAIN_LIMIT) {
                LARGE_SCRATCH.set(scratch);
            }
        }
        int encoded = encoder.encodeFrame(scratch, 0, frameLen, lsn, partitionId, payload, options);
        if (encoded != frameLen) {
            throw new WalException.CodecException(
                    "codec 实际帧长 " + encoded + " 与预估 " + frameLen + " 不一致"
                            + "（codecId=" + encoder.codecId() + "）");
        }
        scratch.limit(frameLen);
        scratch.position(0);
        return scratch;
    }

    private StagingBuffer arenaOf(int partitionId) {
        if (partitionId < 0 || partitionId >= arenas.length) {
            return null;
        }
        StagingBuffer arena = arenas[partitionId];
        if (arena != null) {
            return arena;
        }
        long configured = config.stagingBufferBytes();
        if (configured <= 0L) {
            return null;
        }
        int capacity = (int) Math.min(Integer.MAX_VALUE, configured);
        StagingBuffer created = new StagingBuffer(capacity);
        ByteBuffer view = created.buffer().duplicate();
        arenas[partitionId] = created;
        arenaViews[partitionId] = view;
        return created;
    }

    /**
     * 当前健康度（供 {@code wal.health()} 的写入侧判断）。
     *
     * @return 健康度
     */
    public Health health() {
        return coordinator == null ? Health.HEALTHY : coordinator.health();
    }

    @Override
    public String toString() {
        return "AppendPipeline{oversize=" + oversizeFrames.get()
                + ", closed=" + closed + '}';
    }
}
