package com.walcore.internal.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 计数器与延迟直方图的注册中心（零第三方依赖）。
 *
 * <p>覆盖 PRD P1-05 要求的六项指标：
 * <ol>
 *   <li>写入吞吐：{@code wal.append.records}（条）与 {@code wal.append.bytes}（字节），
 *       由采样线程按 {@code metricsIntervalMillis} 差分出 条/s 与 字节/s；</li>
 *   <li>{@code append} 与 {@code flushAndWait} 的 p50/p99/p999：
 *       {@code wal.append.latency} 与 {@code wal.flush.latency} 两个直方图；</li>
 *   <li>组提交批大小分布：{@code wal.groupcommit.batch.records} 直方图；</li>
 *   <li>日志总大小：{@code wal.log.bytes} 标量；</li>
 *   <li>fsync 耗时：{@code wal.fsync.latency} 直方图；</li>
 *   <li>健康度：{@code wal.health} 标量（0=HEALTHY 1=DEGRADED 2=FAILED）。</li>
 * </ol>
 *
 * <p><b>读路径无锁</b>：计数器是 {@link LongAdder}，直方图桶是原子数组；
 * 采样线程持锁遍历时写入方完全不受影响。
 */
public final class MetricsRegistry {

    /** {@code append} 延迟直方图名称。 */
    public static final String APPEND_LATENCY = "wal.append.latency";
    /** {@code flushAndWait} 延迟直方图名称。 */
    public static final String FLUSH_LATENCY = "wal.flush.latency";
    /** fsync 耗时直方图名称。 */
    public static final String FSYNC_LATENCY = "wal.fsync.latency";
    /** 组提交批大小（条数）直方图名称。 */
    public static final String BATCH_RECORDS = "wal.groupcommit.batch.records";
    /** 累计写入记录数。 */
    public static final String APPEND_RECORDS = "wal.append.records";
    /** 累计写入字节数。 */
    public static final String APPEND_BYTES = "wal.append.bytes";
    /** 累计 fsync 次数。 */
    public static final String FSYNC_COUNT = "wal.fsync.count";
    /**
     * 累计 fsync 失败次数（R4）。
     *
     * <p>口径：{@code force()} 实际抛出 {@code IOException} 的次数——失败的 force
     * 不计入 {@link #FSYNC_COUNT}，两个指标的差值才是"发起且成功"的次数。
     */
    public static final String FSYNC_FAILURE = "wal.fsync.failure";
    /** 累计损坏事件数。 */
    public static final String CORRUPTION_COUNT = "wal.corruption.count";
    /** 累计轮转降级次数。 */
    public static final String ROTATE_FALLBACK = "wal.rotate.fallback";
    /** 健康度标量。 */
    public static final String HEALTH = "wal.health";

    /**
     * Tailer 交付延迟直方图名称（T13，P1-01）。
     *
     * <p>口径：{@code 回调发生的 now - 记录自身 timestampNanos}，单位纳秒。
     * 它<b>包含</b>排队与轮询等待，因此是"端到端订阅延迟"，不是"回调执行耗时"。
     */
    public static final String TAILER_LATENCY = "wal.tailer.latency";

    /** Tailer 累计交付条数。 */
    public static final String TAILER_DELIVERED = "wal.tailer.delivered";

    /** Tailer 因 {@code DROP_TO_LATEST} 累计丢弃条数（TASKS T13 ③ 明确要求）。 */
    public static final String TAILER_DROPPED = "wal.tailer.dropped";

    /** Tailer 累计溢出事件次数。 */
    public static final String TAILER_OVERFLOW = "wal.tailer.overflow";

    /** 保留策略累计删除的段数（T14）。 */
    public static final String RETENTION_DELETED_SEGMENTS = "wal.retention.deleted.segments";

    /** 保留策略因"仍有读者"而跳过删除的段数（T14 ④）。 */
    public static final String RETENTION_SKIPPED_SEGMENTS = "wal.retention.skipped.segments";

    /** 快照写入次数（T15）。 */
    public static final String SNAPSHOT_WRITES = "wal.snapshot.writes";

    /** 日志总字节数标量（T17 ④，gauge，不是累计计数器）。 */
    public static final String LOG_BYTES = "wal.log.bytes";

    /** 存活分区数标量（gauge）。 */
    public static final String PARTITIONS = "wal.partitions";

    /** 现存段数标量（gauge）。 */
    public static final String SEGMENTS = "wal.segments";

    private final boolean enabled;
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, LatencyHistogram> histograms = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<MeterSink> sinks = new CopyOnWriteArrayList<>();
    private final LongAdder sinkErrors = new LongAdder();

    /**
     * 构造启用的注册中心。
     */
    public MetricsRegistry() {
        this(true);
    }

    /**
     * 构造注册中心。
     *
     * @param enabled 为 false 时所有写入调用直接返回（仍可安全调用，便于上层不写分支）
     */
    public MetricsRegistry(boolean enabled) {
        this.enabled = enabled;
        // 预注册高频指标，避免运行期第一次写入时在 ConcurrentHashMap 上做 computeIfAbsent
        counter(APPEND_RECORDS);
        counter(APPEND_BYTES);
        counter(FSYNC_COUNT);
        counter(FSYNC_FAILURE);
        counter(CORRUPTION_COUNT);
        counter(ROTATE_FALLBACK);
        counter(TAILER_DELIVERED);
        counter(TAILER_DROPPED);
        counter(TAILER_OVERFLOW);
        counter(RETENTION_DELETED_SEGMENTS);
        counter(RETENTION_SKIPPED_SEGMENTS);
        counter(SNAPSHOT_WRITES);
        histogram(APPEND_LATENCY);
        histogram(FLUSH_LATENCY);
        histogram(FSYNC_LATENCY);
        histogram(BATCH_RECORDS);
        histogram(TAILER_LATENCY);
        gauge(HEALTH);
        gauge(LOG_BYTES);
        gauge(PARTITIONS);
        gauge(SEGMENTS);
    }

    /**
     * 取得（必要时创建）一个标量 gauge。
     *
     * <p>gauge 与 counter 的区别是语义而不是实现：counter 只增不减，采样线程取差分算速率；
     * gauge 是"最后一次观测到的瞬时值"，直接透传（例如日志总大小、存活分区数、健康度）。
     *
     * @param name 指标名
     * @return gauge 句柄
     */
    public AtomicLong gauge(String name) {
        AtomicLong g = gauges.get(name);
        if (g == null) {
            AtomicLong created = new AtomicLong();
            AtomicLong prev = gauges.putIfAbsent(name, created);
            g = prev != null ? prev : created;
        }
        return g;
    }

    /**
     * 设置 gauge 值。
     *
     * @param name 指标名
     * @param value 值
     */
    public void setGauge(String name, long value) {
        if (enabled) {
            gauge(name).set(value);
        }
    }

    /**
     * 全部 gauge 快照（不可变）。
     *
     * @return 名称 -&gt; 值
     */
    public Map<String, Long> gaugeSnapshot() {
        Map<String, Long> out = new LinkedHashMap<>();
        List<String> names = new ArrayList<>(gauges.keySet());
        Collections.sort(names);
        for (String n : names) {
            out.put(n, gauges.get(n).get());
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * 已注册的 sink 数量（诊断用）。
     *
     * @return 数量
     */
    public int sinkCount() {
        return sinks.size();
    }

    /**
     * 指标采集是否启用。
     *
     * @return 启用返回 true
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * 取得（必要时创建）一个计数器。
     *
     * @param name 指标名
     * @return 计数器
     */
    public LongAdder counter(String name) {
        LongAdder c = counters.get(name);
        if (c == null) {
            LongAdder created = new LongAdder();
            LongAdder prev = counters.putIfAbsent(name, created);
            c = prev != null ? prev : created;
        }
        return c;
    }

    /**
     * 计数器加一。
     *
     * @param name 指标名
     */
    public void increment(String name) {
        if (enabled) {
            counter(name).increment();
        }
    }

    /**
     * 计数器累加。
     *
     * @param name 指标名
     * @param delta 增量
     */
    public void add(String name, long delta) {
        if (enabled) {
            counter(name).add(delta);
        }
    }

    /**
     * 取得（必要时创建）一个使用默认桶边界的延迟直方图。
     *
     * @param name 指标名
     * @return 直方图
     */
    public LatencyHistogram histogram(String name) {
        return histogram(name, LatencyHistogram.DEFAULT_BOUNDS_NANOS);
    }

    /**
     * 取得（必要时创建）一个使用自定义桶边界的延迟直方图。
     *
     * @param name 指标名
     * @param boundsNanos 桶上界（纳秒）
     * @return 直方图
     */
    public LatencyHistogram histogram(String name, long[] boundsNanos) {
        LatencyHistogram h = histograms.get(name);
        if (h == null) {
            LatencyHistogram created = new LatencyHistogram(boundsNanos);
            LatencyHistogram prev = histograms.putIfAbsent(name, created);
            h = prev != null ? prev : created;
        }
        return h;
    }

    /**
     * 记录一个延迟样本。
     *
     * @param name 指标名
     * @param nanos 延迟（纳秒）
     */
    public void recordLatency(String name, long nanos) {
        if (enabled) {
            histogram(name).record(nanos);
        }
    }

    /**
     * 注册一个外抛 sink。
     *
     * @param sink sink，非 null
     */
    public void registerSink(MeterSink sink) {
        if (sink != null && sink != NoopMeterSink.INSTANCE) {
            sinks.addIfAbsent(sink);
        }
    }

    /**
     * 注销一个外抛 sink。
     *
     * @param sink sink
     */
    public void unregisterSink(MeterSink sink) {
        sinks.remove(sink);
    }

    /**
     * 全部计数器快照（不可变）。
     *
     * @return 名称 -&gt; 累计值
     */
    public Map<String, Long> counterSnapshot() {
        Map<String, Long> out = new LinkedHashMap<>();
        List<String> names = new ArrayList<>(counters.keySet());
        Collections.sort(names);
        for (String n : names) {
            out.put(n, counters.get(n).sum());
        }
        return Collections.unmodifiableMap(out);
    }

    /**
     * 全部直方图快照（不可变）。
     *
     * @return 名称 -&gt; 直方图实例
     */
    public Map<String, LatencyHistogram> histogramSnapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(histograms));
    }

    /**
     * 把当前快照推送给全部已注册 sink。
     *
     * <p>由采样线程周期性调用。单个 sink 抛异常<b>不影响</b>其它 sink。
     */
    public void publish() {
        if (sinks.isEmpty()) {
            return;
        }
        Map<String, String> tags = Collections.emptyMap();
        for (Map.Entry<String, LongAdder> e : counters.entrySet()) {
            long v = e.getValue().sum();
            for (MeterSink sink : sinks) {
                try {
                    sink.onCounter(e.getKey(), v, tags);
                } catch (RuntimeException ex) {
                    sinkErrors.increment();
                }
            }
        }
        for (Map.Entry<String, LatencyHistogram> e : histograms.entrySet()) {
            LatencyHistogram h = e.getValue();
            long[] buckets = h.bucketCounts();
            long[] bounds = h.boundsNanos();
            for (MeterSink sink : sinks) {
                try {
                    sink.onHistogram(e.getKey(), buckets, bounds, tags);
                } catch (RuntimeException ex) {
                    sinkErrors.increment();
                }
            }
        }
        for (Map.Entry<String, AtomicLong> e : gauges.entrySet()) {
            long v = e.getValue().get();
            for (MeterSink sink : sinks) {
                try {
                    sink.onGauge(e.getKey(), v, tags);
                } catch (RuntimeException ex) {
                    sinkErrors.increment();
                }
            }
        }
    }

    /**
     * sink 抛出异常的累计次数，用于自诊断。
     *
     * @return 次数
     */
    public long sinkErrorCount() {
        return sinkErrors.sum();
    }

    /**
     * 清空全部样本（保留注册项），用于对比实验。
     */
    public void reset() {
        for (LongAdder c : counters.values()) {
            c.reset();
        }
        for (LatencyHistogram h : histograms.values()) {
            h.reset();
        }
        for (AtomicLong g : gauges.values()) {
            g.set(0L);
        }
        sinkErrors.reset();
    }
}
