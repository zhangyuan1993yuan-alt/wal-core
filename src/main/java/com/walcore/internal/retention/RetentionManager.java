package com.walcore.internal.retention;

import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.util.NamedThreadFactory;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.RetentionPolicy;
import com.walcore.spi.SegmentInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 保留策略的执行者：<b>装配事实 → 问策略 → 做安全检查 → 真删除</b>（T14）。
 *
 * <h2>为什么"问策略"和"删文件"要拆开</h2>
 * 策略是第三方可替换的（{@code wal.spi.retention}）。如果策略能直接删文件，那么
 * "活跃段被删"、"仍有读者的段被删"、"Windows 上没 unmap 就删"这三类事故就会由
 * 插件作者的水平决定。把删除权收回框架后，第三方策略最坏只是"判断得不好"，
 * 不会造成数据损坏——这是可扩展系统里非常重要的一条边界。
 *
 * <h2>不可删除边界（{@code minPinnedLsn}）怎么来的</h2>
 * 三者取最小：
 * <ol>
 *   <li><b>显式 pin</b>：{@link #pin(long)} 登记的位置（游标/tailer 通过它声明"我还在读这里"）；</li>
 *   <li><b>外部 pin 源</b>：由装配方注入，通常返回"最近快照的 {@code uptoLsn}"（T15 联动）；</li>
 *   <li><b>仍有读者的最旧段的首帧 LSN</b>：兜底，防止"忘记 pin 的读者"被误伤。</li>
 * </ol>
 *
 * <h2>调度</h2>
 * 单 daemon 线程、每 {@code retentionIntervalMillis}（默认 30s）跑一轮；
 * 测试可以调 {@link #runOnce()} 立刻跑一轮并拿到明细。
 */
public final class RetentionManager implements AutoCloseable {

    /** 关闭时等待线程退出的上限（毫秒）。 */
    private static final long STOP_JOIN_MILLIS = 5_000L;

    /** 单轮评估失败的计数名（不复用通用计数器，便于按组件定位）。 */
    private static final String RETENTION_FAILED = "wal.retention.failed";

    /**
     * "删除越过不可删除边界"的计数名。
     *
     * <p>这个指标<b>正常环境下恒为 0</b>。它一旦非 0，就说明框架内部的保留围栏被破坏
     * （读者需要的数据被删），属于必须上报的缺陷，而不是可调优的噪声。
     */
    private static final String RETENTION_FENCE_VIOLATION = "wal.retention.fence_violation";

    /**
     * 外部 pin 源（快照位点的注入点）。
     */
    public interface PinSource {

        /**
         * 返回该分区不可删除的最小 LSN。
         *
         * @param partitionId 分区 ID
         * @return LSN；{@code <= 0} 表示没有额外约束
         */
        long minPinnedLsn(int partitionId);
    }

    private final SegmentManager segmentManager;
    private final PartitionRegistry registry;
    private final RetentionPolicy policy;
    private final WalConfig config;
    private final MetricsRegistry metrics;
    private final PinSource pinSource;

    /** 显式 pin：LSN -&gt; 引用计数（多个消费者可能 pin 同一位置）。 */
    private final ConcurrentSkipListMap<Long, AtomicInteger> pins = new ConcurrentSkipListMap<>();

    private final AtomicLong deletedSegments = new AtomicLong();
    private final AtomicLong skippedSegments = new AtomicLong();
    private final AtomicLong runs = new AtomicLong();

    private volatile boolean running = true;
    private volatile Thread thread;
    private volatile List<RunResult> lastRun = List.of();

    /**
     * 构造管理器（不启动线程）。
     *
     * @param segmentManager 段管理器
     * @param registry 分区注册表
     * @param policy 保留策略
     * @param config 配置
     * @param metrics 指标，可为 null
     * @param pinSource 外部 pin 源，可为 null
     */
    public RetentionManager(SegmentManager segmentManager, PartitionRegistry registry,
                            RetentionPolicy policy, WalConfig config, MetricsRegistry metrics,
                            PinSource pinSource) {
        this.segmentManager = Preconditions.checkNotNull(segmentManager, "segmentManager");
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.policy = Preconditions.checkNotNull(policy, "policy");
        this.config = Preconditions.checkNotNull(config, "config");
        this.metrics = metrics;
        this.pinSource = pinSource;
    }

    /**
     * 启动后台保留线程（幂等）。
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
            Thread t = new NamedThreadFactory("wal-retention", true)
                    .newThread(this::runLoop);
            t.setPriority(Thread.MIN_PRIORITY);
            this.thread = t;
            t.start();
        }
    }

    /**
     * 后台循环。
     */
    private void runLoop() {
        long interval = Math.max(50L, config.retentionIntervalMillis());
        while (running) {
            try {
                runOnce();
            } catch (RuntimeException e) {
                // 单轮失败不能让线程死掉：保留策略是"尽力而为"的后台清理
                if (metrics != null) {
                    metrics.increment(RETENTION_FAILED);
                }
            }
            long slept = 0L;
            while (running && slept < interval) {
                try {
                    Thread.sleep(Math.min(100L, interval - slept));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                slept += 100L;
            }
        }
    }

    /**
     * 立刻跑一轮（全部存活分区）。
     *
     * @return 每个分区的结果
     */
    public List<RunResult> runOnce() {
        return runOnce(false);
    }

    /**
     * 立刻跑一轮。
     *
     * @param force {@code true} 时忽略 {@code minPinnedLsn} 保护（会记 WARN 指标），仅供运维
     * @return 每个分区的结果（按分区 ID 升序）
     */
    public List<RunResult> runOnce(boolean force) {
        if (!running) {
            return List.of();
        }
        runs.incrementAndGet();
        if (force && metrics != null) {
            metrics.increment("wal.retention.forced");
        }
        List<RunResult> results = new ArrayList<>();
        for (int pid : registry.activeIdsCopy()) {
            try {
                results.add(runPartition(pid, force));
            } catch (WalException e) {
                results.add(new RunResult(pid, 0, 0, 0, "error: " + e.getMessage(),
                        List.of()));
            }
        }
        this.lastRun = Collections.unmodifiableList(results);
        return this.lastRun;
    }

    /**
     * 对单个分区跑一轮。
     *
     * @param partitionId 分区 ID
     * @param force 是否忽略 pin 保护
     * @return 结果
     */
    public RunResult runPartition(int partitionId, boolean force) {
        List<Segment> segments = segmentManager.segments(partitionId);
        if (segments.isEmpty()) {
            return new RunResult(partitionId, 0, 0, 0, "no-segments", List.of());
        }
        PartitionImpl partition = registry.get(partitionId);
        if (partition != null && partition.state() == com.walcore.api.PartitionInfo.State.PURGED) {
            return new RunResult(partitionId, 0, 0, 0, "purged", List.of());
        }

        List<SegmentInfo> infos = new ArrayList<>(segments.size());
        long totalBytes = 0L;
        for (Segment seg : segments) {
            SegmentInfo info = toInfo(seg);
            infos.add(info);
            totalBytes += info.sizeBytes();
        }
        long pinned = force ? 0L : minPinnedLsn(partitionId);
        long durable = partition == null ? 0L : partition.durableLsn();
        RetentionContext context = new RetentionContext(partitionId, infos,
                System.currentTimeMillis(), totalBytes, pinned, durable);

        RetentionDecision decision = policy.evaluate(context);
        if (!decision.hasWork()) {
            return new RunResult(partitionId, 0, 0, infos.size(), decision.reason(), List.of());
        }

        int deleted = 0;
        int skippedReaders = 0;
        int skippedPinned = 0;
        long maxDeletedLastLsn = 0L;
        List<Long> deletedLsns = new ArrayList<>();
        for (SegmentInfo info : decision.toDelete()) {
            // 每个段删除前<b>重新采样一次围栏</b>，而不是整轮只采样一次。
            //
            // 实测过的失败形态（T14 回归的第三种签名，探针可稳定复现）：一轮删除在 t0
            // 采样到 minPinnedLsn=0（此刻确实没有读者），随后逐个 unmap+删文件要花十几毫秒；
            // 就在这段时间里一个游标被创建并登记了围栏，于是它"刚开始读就发现自己要读的
            // 段已经不见了"，续读直接跳号（pos=180 之后下一批读到 289）。
            // 整轮只采样一次 = 把"决策"和"执行"之间的整个批处理时间都变成了竞态窗口。
            long fence = effectiveFence(partitionId, pinned, force);
            if (!force && isPinned(info, fence)) {
                skippedPinned++;
                continue;
            }
            boolean ok;
            try {
                // 围栏还要传进去，在 SegmentManager 的段序列临界区内、紧邻真正的摘除
                // 之前再复核一次：这样"看起来可删"与"真的摘除"之间的窗口只剩几十纳秒。
                ok = segmentManager.deleteSegment(partitionId, info.firstLsn(), fence);
            } catch (WalException e) {
                ok = false;
            }
            if (!ok) {
                // 三种原因都归到"本轮跳过"：活跃段、仍有读者、已被围栏保护/已不存在
                skippedReaders++;
                continue;
            }
            deleted++;
            maxDeletedLastLsn = Math.max(maxDeletedLastLsn, info.lastLsn());
            deletedLsns.add(info.firstLsn());
            try {
                policy.onSegmentDeleted(info);
            } catch (RuntimeException ignored) {
                // 回调失败不影响已完成的删除
            }
        }

        // ---- 防御性断言：本轮的删除不得越过"不可删除边界" ----
        //
        // 这里校验的是<b>结构性不变量</b>而不是"某次具体故障的补丁"：
        // 每一条被真正删掉的段，其段尾 LSN 必须严格小于本轮生效的 pin 地板。
        // 一旦有人（未来重构、插件、或本类自己的顺序调整）把 isPinned 检查移到
        // deleteSegment 之后、或把 force 语义用错，这条断言会立刻把"静默丢区间"
        // 变成一次<b>响亮的失败</b>（异常 + 指标），而不是让下游重放悄悄缺数据。
        //
        // 注意：这里<b>不</b>在删除前做同样的校验——那样只是重复一次 isPinned，
        // 真正的价值在于"删除动作发生之后"再核对一次事实。
        if (!force && deleted > 0 && pinned > 0L && maxDeletedLastLsn >= pinned) {
            if (metrics != null) {
                metrics.increment(RETENTION_FENCE_VIOLATION);
            }
            throw new IllegalStateException(
                    "保留策略越过了不可删除边界（内部不变量被破坏）: partition=" + partitionId
                            + ", pinnedLsn=" + pinned
                            + ", 被删段的最大段尾 LSN=" + maxDeletedLastLsn
                            + ", 被删段=" + deletedLsns
                            + "。这说明有读者需要的数据被删除，下游重放会出现缺区间；"
                            + "请保留现场并上报（这是框架缺陷，不是配置问题）");
        }

        if (deleted > 0) {
            deletedSegments.addAndGet(deleted);
            if (metrics != null) {
                metrics.add(MetricsRegistry.RETENTION_DELETED_SEGMENTS, deleted);
            }
        }
        int skipped = skippedReaders + skippedPinned;
        if (skipped > 0) {
            skippedSegments.addAndGet(skipped);
            if (metrics != null) {
                metrics.add(MetricsRegistry.RETENTION_SKIPPED_SEGMENTS, skipped);
            }
        }
        return new RunResult(partitionId, deleted, skippedReaders + skippedPinned,
                infos.size(), decision.reason(), deletedLsns);
    }

    /**
     * 判断段是否被 pin 保护。
     *
     * @param info 段
     * @param pinned 边界 LSN
     * @return 受保护返回 true
     */
    private static boolean isPinned(SegmentInfo info, long pinned) {
        if (pinned <= 0L) {
            return false;
        }
        return info.lastLsn() >= pinned;
    }

    /**
     * 段 → 描述信息（逻辑占用口径见 {@link SegmentInfo} 的注释）。
     *
     * @param seg 段
     * @return 描述
     */
    private static SegmentInfo toInfo(Segment seg) {
        long size;
        if (seg.isActive()) {
            size = seg.publishedWritePos();
        } else {
            long sealed = seg.finalLength();
            size = sealed >= 0L ? sealed : seg.publishedWritePos();
        }
        return new SegmentInfo(seg.partitionId(), seg.firstLsn(), seg.lastLsn(), size,
                seg.createdMillis(), seg.handle().path());
    }

    // ==================================================================
    // pin 管理
    // ==================================================================

    /**
     * 登记一个不可删除位置（引用计数）。
     *
     * @param lsn LSN，{@code <= 0} 时忽略
     */
    public void pin(long lsn) {
        if (lsn <= 0L) {
            return;
        }
        pins.compute(lsn, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            }
            v.incrementAndGet();
            return v;
        });
    }

    /**
     * 解除登记（引用计数归零后移除）。
     *
     * @param lsn LSN
     */
    public void unpin(long lsn) {
        if (lsn <= 0L) {
            return;
        }
        pins.computeIfPresent(lsn, (k, v) -> v.decrementAndGet() <= 0 ? null : v);
    }

    /**
     * 当前不可删除的最小 LSN（0 表示无约束）。
     *
     * <p>三者取最小：显式 pin、外部 pin 源（快照位点）、仍有读者的最旧段的首帧 LSN。
     * 第三项是"忘记 pin 的读者"的兜底；正常路径（游标 / tailer）会显式 pin，
     * 因此它通常不生效——这也让本方法的 O(段数) 扫描不至于落在热路径上。
     *
     * @param partitionId 分区 ID
     * @return LSN
     */
    public long minPinnedLsn(int partitionId) {
        long min = minExplicitPinnedLsn(partitionId);
        for (Segment seg : segmentManager.segments(partitionId)) {
            if (seg.readerCount() > 0) {
                long segFirst = seg.firstLsn();
                if (min <= 0L || segFirst < min) {
                    min = segFirst;
                }
                break;
            }
        }
        return min;
    }

    /**
     * 只算"显式登记"的围栏（显式 pin + 外部 pin 源），<b>不扫段序列</b>。
     *
     * <p>删除循环里每删一个段都要重新采样围栏，因此这里必须是 O(log n)（跳表首项）而不是
     * O(段数)。"仍有读者的段"这一维度不需要在这里重复计算——它由
     * {@link SegmentManager#deleteSegment(int, long, long)} 内的
     * {@code beginDelete()} 与读者登记互斥地保证，粒度更细、更准。
     *
     * @param partitionId 分区 ID
     * @return LSN；无约束返回 0
     */
    private long minExplicitPinnedLsn(int partitionId) {
        long min = 0L;
        Map.Entry<Long, AtomicInteger> first = pins.firstEntry();
        if (first != null) {
            min = first.getKey();
        }
        if (pinSource != null) {
            long external = pinSource.minPinnedLsn(partitionId);
            if (external > 0L && (min <= 0L || external < min)) {
                min = external;
            }
        }
        return min;
    }

    /**
     * 本次删除实际生效的围栏 = {@code min(轮首围栏, 当前围栏)}（两者都取正值）。
     *
     * <p>取较小值是因为"围栏越小 = 保护越多"：轮首采样与"现在"之间可能新登记了
     * 更小的 pin（新读者从更早的位置开始读），必须按更严格的那一侧执行。
     * 反向（pin 被撤销、围栏变大）不影响本轮的待删集合——那些段本来就在轮首围栏之下。
     *
     * @param partitionId 分区 ID
     * @param roundPinned 轮首采样的围栏
     * @param force 是否忽略保护
     * @return 生效围栏；{@code <= 0} 表示无保护
     */
    private long effectiveFence(int partitionId, long roundPinned, boolean force) {
        if (force) {
            return 0L;
        }
        long now = minExplicitPinnedLsn(partitionId);
        if (roundPinned <= 0L) {
            return now;
        }
        if (now <= 0L) {
            return roundPinned;
        }
        return Math.min(roundPinned, now);
    }

    /**
     * 归档请求：立刻对该分区跑一轮评估（{@code ARCHIVE} 模式用）。
     *
     * <p><b>诚实说明</b>：本方法只负责"立刻触发本地清理评估"。真正的"上传到对象存储"
     * 是外部系统的职责，由 {@link RetentionPolicy} 实现通过 {@code onSegmentDeleted}
     * 回调或自有流程完成——框架不内置任何网络/归档实现。
     *
     * @param partitionId 分区 ID
     * @return 本轮结果
     */
    public RunResult notifyArchive(int partitionId) {
        if (metrics != null) {
            metrics.increment("wal.retention.archive.requested");
        }
        return runPartition(partitionId, false);
    }

    // ==================================================================
    // 观测 / 关闭
    // ==================================================================

    /**
     * 生效的策略。
     *
     * @return 策略
     */
    public RetentionPolicy policy() {
        return policy;
    }

    /**
     * 累计删除的段数。
     *
     * @return 数量
     */
    public long deletedSegmentCount() {
        return deletedSegments.get();
    }

    /**
     * 累计跳过的段数（仍有读者 / 被 pin 保护）。
     *
     * @return 数量
     */
    public long skippedSegmentCount() {
        return skippedSegments.get();
    }

    /**
     * 累计评估轮次。
     *
     * @return 轮次
     */
    public long runCount() {
        return runs.get();
    }

    /**
     * 最近一轮的明细。
     *
     * @return 不可变列表
     */
    public List<RunResult> lastRun() {
        return lastRun;
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        Thread t = this.thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(STOP_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String toString() {
        return "RetentionManager{policy=" + policy.id()
                + ", runs=" + runs.get()
                + ", deleted=" + deletedSegments.get()
                + ", skipped=" + skippedSegments.get() + '}';
    }

    /**
     * 单个分区的评估结果。
     */
    public static final class RunResult {

        private final int partitionId;
        private final int deleted;
        private final int skipped;
        private final int remainingSegments;
        private final String reason;
        private final List<Long> deletedFirstLsns;

        /**
         * 构造结果。
         *
         * @param partitionId 分区 ID
         * @param deleted 删除的段数
         * @param skipped 跳过的段数
         * @param remainingSegments 本轮结束后仍存在的段数
         * @param reason 策略给出的原因
         * @param deletedFirstLsns 被删段的名字 LSN 列表
         */
        public RunResult(int partitionId, int deleted, int skipped, int remainingSegments,
                         String reason, List<Long> deletedFirstLsns) {
            this.partitionId = partitionId;
            this.deleted = deleted;
            this.skipped = skipped;
            this.remainingSegments = remainingSegments;
            this.reason = reason;
            this.deletedFirstLsns = List.copyOf(deletedFirstLsns);
        }

        /**
         * 分区 ID。
         *
         * @return 分区 ID
         */
        public int partitionId() {
            return partitionId;
        }

        /**
         * 删除的段数。
         *
         * @return 数量
         */
        public int deleted() {
            return deleted;
        }

        /**
         * 跳过的段数。
         *
         * @return 数量
         */
        public int skipped() {
            return skipped;
        }

        /**
         * 本轮结束后仍存在的段数。
         *
         * @return 数量
         */
        public int remainingSegments() {
            return remainingSegments;
        }

        /**
         * 策略给出的原因。
         *
         * @return 原因
         */
        public String reason() {
            return reason;
        }

        /**
         * 被删段的名字 LSN 列表。
         *
         * @return 不可变列表
         */
        public List<Long> deletedFirstLsns() {
            return deletedFirstLsns;
        }

        @Override
        public String toString() {
            return "RetentionRun{p" + partitionId
                    + ", deleted=" + deleted
                    + ", skipped=" + skipped
                    + ", remaining=" + remainingSegments
                    + ", reason=" + reason + '}';
        }
    }
}
