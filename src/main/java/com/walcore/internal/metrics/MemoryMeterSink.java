package com.walcore.internal.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存型 {@link MeterSink}：把最近一次推送的快照留在 Map 里，供测试与排障直接读取。
 *
 * <h2>为什么框架自带一个内存 sink</h2>
 * 桥接外部监控（Micrometer / Prometheus）是使用方的事，但"<b>验证指标真的被采集到了</b>"
 * 是框架自己必须能做的事。没有这个类，每个想断言指标的测试都得自己写一遍几十行 sink，
 * 而且很容易写出"只在 onCounter 里存值、忘了 onHistogram"这类假绿测试。
 *
 * <h2>线程安全</h2>
 * {@link #onCounter}/{@link #onHistogram}/{@link #onGauge} 由采样线程调用，
 * {@link #counter}/{@link #histogram} 由断言线程读取，两侧都用 {@link ConcurrentHashMap}，
 * 单次读写不会看到半更新状态。<b>不要</b>在多线程里同时依赖"三次读彼此一致"——
 * 那不是这个类的职责。
 */
public final class MemoryMeterSink implements MeterSink {

    private final Map<String, Long> counters = new ConcurrentHashMap<>();
    private final Map<String, long[]> histogramBuckets = new ConcurrentHashMap<>();
    private final Map<String, long[]> histogramBounds = new ConcurrentHashMap<>();
    private final Map<String, Long> gauges = new ConcurrentHashMap<>();

    /** 收到的回调总次数，用来验证"框架确实在周期性推送"。 */
    private volatile long callbacks;

    @Override
    public void onCounter(String name, long value, Map<String, String> tags) {
        counters.put(name, value);
        callbacks++;
    }

    @Override
    public void onHistogram(String name, long[] buckets, long[] boundsNanos,
                            Map<String, String> tags) {
        // 必须拷贝：框架复用同一个数组实例填充，直接存引用会被下一次推送覆盖
        histogramBuckets.put(name, buckets.clone());
        histogramBounds.put(name, boundsNanos.clone());
        callbacks++;
    }

    @Override
    public void onGauge(String name, long value, Map<String, String> tags) {
        gauges.put(name, value);
        callbacks++;
    }

    /**
     * 读某个计数器的最近值。
     *
     * @param name 指标名
     * @return 值；从未收到返回 null
     */
    public Long counter(String name) {
        return counters.get(name);
    }

    /**
     * 读某个 gauge 的最近值。
     *
     * @param name 指标名
     * @return 值；从未收到返回 null
     */
    public Long gauge(String name) {
        return gauges.get(name);
    }

    /**
     * 读某个直方图的桶计数副本。
     *
     * @param name 指标名
     * @return 桶计数；从未收到返回 null
     */
    public long[] histogram(String name) {
        long[] b = histogramBuckets.get(name);
        return b == null ? null : b.clone();
    }

    /**
     * 读某个直方图的桶边界副本。
     *
     * @param name 指标名
     * @return 桶边界；从未收到返回 null
     */
    public long[] bounds(String name) {
        long[] b = histogramBounds.get(name);
        return b == null ? null : b.clone();
    }

    /**
     * 收到的回调次数。
     *
     * @return 次数
     */
    public long callbacks() {
        return callbacks;
    }

    /**
     * 已收到的计数器名称集合（诊断用）。
     *
     * @return 名称 -&gt; 值（副本）
     */
    public Map<String, Long> counters() {
        return new LinkedHashMap<>(counters);
    }

    /**
     * 已收到的 gauge 名称集合（诊断用）。
     *
     * @return 名称 -&gt; 值（副本）
     */
    public Map<String, Long> gauges() {
        return new LinkedHashMap<>(gauges);
    }

    /**
     * 已收到的直方图名称集合（诊断用）。
     *
     * @return 名称集合（副本）
     */
    public java.util.Set<String> histogramNames() {
        return new java.util.LinkedHashSet<>(histogramBuckets.keySet());
    }

    /**
     * 清空全部已收到的数据（保留注册关系），用于两轮对比。
     */
    public void clear() {
        counters.clear();
        histogramBuckets.clear();
        histogramBounds.clear();
        gauges.clear();
        callbacks = 0L;
    }

    @Override
    public String toString() {
        return "MemoryMeterSink{counters=" + counters.size()
                + ", histograms=" + histogramBuckets.size()
                + ", gauges=" + gauges.size()
                + ", callbacks=" + callbacks + '}';
    }
}
