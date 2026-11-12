package com.walcore.internal.read;

import com.walcore.api.CursorOptions;
import com.walcore.api.TailHandler;
import com.walcore.api.TailerOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.api.WalTailer;
import com.walcore.internal.io.MappedWindowCache;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.util.NamedThreadFactory;
import com.walcore.internal.util.Preconditions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link WalTailer} 的实现：<b>单线程顺序读 + 有界落后判定 + 三档溢出策略</b>。
 *
 * <h2>为什么"追历史"和"追实时"共用一个游标就能做到无空隙</h2>
 * 朴素做法是"先用游标追到 {@code publishedLsn}，再换成订阅式推送"，但两步之间
 * 写入可能又前进了，切换点极易丢记录或多吐记录（典型 bug：{@code publishedLsn} 读早了）。
 * 本实现完全不切换：始终用同一个 {@link CursorImpl} 往前读，它本身会随轮转<b>增长段列表</b>
 * （见 {@code CursorImpl#refreshSegments}），读到段尾再读一次就能看到新写入的数据。
 * 于是"历史阶段"与"实时阶段"只是同一个循环里 {@link #lagRecords()} 是否为 0 的差别，
 * 结构上不可能出现空隙或重复。
 *
 * <h2>为什么消费在自己的线程上</h2>
 * 回调里做 IO / 睡眠是订阅方的自由，但<b>绝不能</b>让写入线程陪着它等。
 * 因此 {@code append()} 完全不感知 tailer 的存在（连 {@code unpark} 都没有），
 * 代价是最坏 {@value #POLL_NANOS}ns 的轮询延迟，换来的是"慢订阅者 → 写入吞吐为 0 影响"
 * 这一硬保证（PRD P1-01 ④）。
 *
 * <h2>三档溢出策略的诚实说明</h2>
 * <ul>
 *   <li>{@code DISCONNECT}：回调 {@link TailHandler#onOverflow} 后自关，语义最严格；</li>
 *   <li>{@code DROP_TO_LATEST}：{@code seekTo(publishedLsn)} 跳到最新，丢弃量计入
 *       {@code wal.tailer.dropped}，并在 {@link #droppedRecords()} 可见；</li>
 *   <li>{@code BLOCK}：<b>本实现选择"不丢"而不是"反压写入方"</b>——WAL 是写优先系统，
 *       让订阅者反压写入方等于把"日志组件"变成"消息队列"，会破坏 {@code append()} 的
 *       p99 承诺。因此 BLOCK 的语义是"订阅方自己承担无限落后的内存/延迟代价"，
 *       与 ARCH §3.5 的"背压选项"意图一致，但不提供跨组件的写阻塞。</li>
 * </ul>
 */
public final class TailerImpl implements WalTailer {

    /** 轮询间隔（纳秒）。50µs 对"实时"场景足够，CPU 占用可忽略。 */
    private static final long POLL_NANOS = 50_000L;

    /** 关闭时等待消费线程退出的上限（毫秒）。 */
    private static final long STOP_JOIN_MILLIS = 2_000L;

    private final int partitionId;
    private final TailHandler handler;
    private final WalConfig.TailerOverflowPolicy overflowPolicy;
    private final int bufferSize;
    private final long subscribedFromLsn;
    private final PartitionRegistry registry;
    private final MetricsRegistry metrics;
    private final WalCursor cursor;
    private final Thread thread;

    /** 收尾是否已经开始（防重入守卫，允许在中途被观察）。 */
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);

    /** 是否<b>已完成</b>收尾（{@code onClose} 已回调）。对外语义以此为准。 */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong lastDeliveredLsn = new AtomicLong(0L);

    private volatile boolean running = true;
    private volatile boolean live;
    private volatile Throwable failure;

    /**
     * 构造并<b>立即启动</b>订阅线程。
     *
     * @param partitionId 分区 ID
     * @param options 选项
     * @param segmentManager 段管理器
     * @param decoder 帧解码器
     * @param checkpointStore checkpoint 存储（可为 null，订阅不使用 checkpoint）
     * @param windows 映射窗口缓存，可为 null
     * @param registry 分区注册表（读取 {@code publishedLsn}）
     * @param metrics 指标，可为 null
     * @param threadNamePrefix 线程名前缀
     * @param retentionPin 保留策略 pin 出口（可为 null）：tailer 内部的游标是长活读者，
     *                     必须让它把"当前读位置"登记给保留策略，否则保留策略会在它
     *                     还没读到的段上"合法地"删数据
     */
    public TailerImpl(int partitionId, TailerOptions options, SegmentManager segmentManager,
                      FrameDecoder decoder, CursorCheckpointStore checkpointStore,
                      MappedWindowCache windows, PartitionRegistry registry,
                      MetricsRegistry metrics, String threadNamePrefix,
                      RetentionPin retentionPin) {
        Preconditions.checkNotNull(options, "options");
        this.partitionId = partitionId;
        this.handler = options.handler();
        this.overflowPolicy = options.overflowPolicy();
        this.bufferSize = options.bufferSize();
        this.subscribedFromLsn = options.fromLsn();
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.metrics = metrics;
        this.lastDeliveredLsn.set(subscribedFromLsn - 1L);

        CursorOptions cursorOptions = CursorOptions.builder()
                .fromLsn(subscribedFromLsn)
                .resumeFromCheckpoint(false)
                .verifyOnRead(false)
                .build();
        this.cursor = new CursorImpl(partitionId, segmentManager, decoder, cursorOptions,
                checkpointStore, windows, retentionPin);

        String prefix = (threadNamePrefix == null || threadNamePrefix.isEmpty())
                ? "wal" : threadNamePrefix;
        this.thread = new NamedThreadFactory(prefix + "-tailer-p" + partitionId, true)
                .newThread(this::runLoop);
        this.thread.start();
    }

    // ==================================================================
    // 主循环
    // ==================================================================

    /**
     * 消费循环：读一条 → 交付 → 无数据时判溢出 → 小睡。
     */
    private void runLoop() {
        try {
            while (running) {
                WalRecord record;
                try {
                    record = cursor.next();
                } catch (WalException.ClosedException e) {
                    break;
                }
                if (record != null) {
                    deliver(record);
                    if (!running) {
                        break;
                    }
                    // 交付之后<b>必须</b>再判一次溢出。只在"读不到数据"时判溢出是错的：
                    // 订阅者很慢但数据一直有，cursor.next() 每次都返回记录，
                    // 那条"无数据分支"永远走不到，落后就会无限增长而策略永不生效
                    // （实测表现：DISCONNECT 不触发、DROP_TO_LATEST 永不丢弃）。
                    if (!checkOverflow()) {
                        break;
                    }
                    continue;
                }
                if (!handleIdle()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            failure = e;
        } finally {
            shutdown();
        }
    }

    /**
     * 交付一条记录。
     *
     * @param record 记录（可复用实例）
     */
    private void deliver(WalRecord record) {
        long lsn = record.lsn();
        long latency = System.nanoTime() - record.timestampNanos();
        lastDeliveredLsn.set(lsn);
        delivered.incrementAndGet();
        if (metrics != null) {
            metrics.recordLatency(MetricsRegistry.TAILER_LATENCY, latency);
            metrics.add(MetricsRegistry.TAILER_DELIVERED, 1L);
        }
        try {
            handler.onRecord(record);
        } catch (RuntimeException e) {
            failure = e;
            running = false;
            try {
                handler.onError(e);
            } catch (RuntimeException ignored) {
                // onError 自身抛异常：只记录，不再递归上报
            }
        }
    }

    /**
     * 无数据可读时的处理：判断是否追平、是否溢出、然后小睡。
     *
     * @return 继续消费返回 true；应当退出循环返回 false
     */
    private boolean handleIdle() {
        long published = publishedLsn();
        long lag = published - lastDeliveredLsn.get();
        if (lag <= 0L) {
            // 已追平：进入实时阶段（此后只要不溢出就一直是 live）
            live = true;
            LockSupport.parkNanos(this, POLL_NANOS);
            return running;
        }
        if (!checkOverflow()) {
            return false;
        }
        LockSupport.parkNanos(this, POLL_NANOS);
        return running;
    }

    /**
     * 判定并处理溢出（读一次 {@code publishedLsn}，代价是一次 acquire 读）。
     *
     * @return 继续消费返回 true
     */
    private boolean checkOverflow() {
        long published = publishedLsn();
        long lag = published - lastDeliveredLsn.get();
        if (lag > bufferSize) {
            return handleOverflow(lag, published);
        }
        if (lag <= 0L) {
            live = true;
        }
        return running;
    }

    /**
     * 按策略处理溢出。
     *
     * @param lag 落后条数
     * @param published 当前已发布的最大 LSN
     * @return 继续消费返回 true（DROP_TO_LATEST / BLOCK）；应当断开返回 false
     */
    private boolean handleOverflow(long lag, long published) {
        if (metrics != null) {
            metrics.increment(MetricsRegistry.TAILER_OVERFLOW);
        }
        switch (overflowPolicy) {
            case DROP_TO_LATEST: {
                long target = published;
                long before = lastDeliveredLsn.get();
                if (target <= before) {
                    return running;
                }
                boolean moved = cursor.seekTo(target);
                if (!moved) {
                    // 目标 LSN 所在段已被保留策略删掉：无法跳转，退化为继续顺序读，
                    // 由下一轮的 lag 判定再次尝试（不会静默丢数据，只是慢）
                    LockSupport.parkNanos(this, POLL_NANOS);
                    return running;
                }
                long skipped = target - before - 1L;
                if (skipped > 0L) {
                    dropped.addAndGet(skipped);
                    lastDeliveredLsn.set(target - 1L);
                    if (metrics != null) {
                        metrics.add(MetricsRegistry.TAILER_DROPPED, skipped);
                    }
                }
                return running;
            }
            case BLOCK:
                // 不丢：继续按顺序读。落后代价由订阅方承担（见类注释的诚实说明）
                LockSupport.parkNanos(this, POLL_NANOS);
                return running;
            case DISCONNECT:
            default: {
                WalException.TailerOverflowException overflow =
                        new WalException.TailerOverflowException(
                                "订阅者落后 " + lag + " 条，超过 bufferSize=" + bufferSize
                                        + "（partition=" + partitionId
                                        + ", policy=" + overflowPolicy + "），按策略断开订阅");
                try {
                    handler.onOverflow(overflow);
                } catch (RuntimeException e) {
                    failure = e;
                }
                running = false;
                return false;
            }
        }
    }

    /**
     * 关闭游标并回调 {@code onClose}（至多一次）。
     */
    private void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }
        running = false;
        try {
            cursor.close();
        } catch (RuntimeException ignored) {
            // 游标关闭失败不改变"订阅已结束"这一事实
        }
        try {
            handler.onClose();
        } catch (RuntimeException ignored) {
            // onClose 抛异常不影响线程退出
        }
        // 最后才置"已完成"：这样 isClosed()==true 就严格蕴含"onClose 已回调过"，
        // 使用方可以放心用 isClosed() 作为"资源已释放"的判据（避免一个易错的时序坑）
        closed.set(true);
    }

    /**
     * 当前已发布的 LSN；分区已被 PURGE 时返回 0。
     *
     * @return LSN
     */
    private long publishedLsn() {
        PartitionImpl partition = registry.get(partitionId);
        return partition == null ? 0L : partition.publishedLsn();
    }

    // ==================================================================
    // WalTailer
    // ==================================================================

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public long subscribedFromLsn() {
        return subscribedFromLsn;
    }

    @Override
    public long lagRecords() {
        long lag = publishedLsn() - lastDeliveredLsn.get();
        return lag < 0L ? 0L : lag;
    }

    @Override
    public boolean isLive() {
        return live;
    }

    @Override
    public long deliveredRecords() {
        return delivered.get();
    }

    @Override
    public long droppedRecords() {
        return dropped.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * 关闭订阅。
     *
     * <p>若从 tailer 自己的回调线程里调用（handler 内部调 {@code close()}），
     * <b>不会</b>自我 join，只置标志位，由 {@code runLoop} 的 {@code finally} 完成收尾。
     */
    @Override
    public void close() {
        if (closed.get() || shutdownStarted.get()) {
            return;
        }
        running = false;
        if (Thread.currentThread() == thread) {
            // 回调线程内自关：不能 join 自己，交由 runLoop 退出时收尾
            return;
        }
        LockSupport.unpark(thread);
        try {
            thread.join(STOP_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        shutdown();
    }

    /**
     * 消费线程名（测试用于断言"close 后线程已退出"）。
     *
     * @return 线程名
     */
    public String threadName() {
        return thread.getName();
    }

    /**
     * 订阅线程是否仍存活。
     *
     * @return 存活返回 true
     */
    public boolean isThreadAlive() {
        return thread.isAlive();
    }

    /**
     * 消费循环因异常终止时的根因。
     *
     * @return 异常；正常关闭返回 null
     */
    public Throwable failure() {
        return failure;
    }

    @Override
    public String toString() {
        return "TailerImpl{partition=" + partitionId
                + ", from=" + subscribedFromLsn
                + ", delivered=" + delivered.get()
                + ", dropped=" + dropped.get()
                + ", lag=" + lagRecords()
                + ", live=" + live
                + ", closed=" + closed.get() + '}';
    }
}
