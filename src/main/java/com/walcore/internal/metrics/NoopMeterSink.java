package com.walcore.internal.metrics;

import java.util.Map;

/**
 * 默认的空 {@link MeterSink}：什么都不做。
 *
 * <p>存在的意义是让"未接入外部监控"成为<b>零分支</b>的默认路径：
 * {@code MetricsRegistry} 持有一个不可为 null 的 sink 引用，采样时无脑调用，
 * 不需要每次判断"是否有 sink"。
 */
public final class NoopMeterSink implements MeterSink {

    /** 单例。 */
    public static final NoopMeterSink INSTANCE = new NoopMeterSink();

    /**
     * 构造空实现。通常直接使用 {@link #INSTANCE}。
     */
    public NoopMeterSink() {
        // 无状态，无资源
    }

    @Override
    public void onCounter(String name, long value, Map<String, String> tags) {
        // 故意为空
    }

    @Override
    public void onHistogram(String name, long[] buckets, long[] boundsNanos, Map<String, String> tags) {
        // 故意为空
    }

    @Override
    public void onGauge(String name, long value, Map<String, String> tags) {
        // 故意为空
    }
}
