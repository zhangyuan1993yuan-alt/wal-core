package com.walcore.internal.metrics;

import java.util.Map;

/**
 * 指标外抛接口：把零依赖的内部指标桥接到外部监控体系（Micrometer / Prometheus / 日志）。
 *
 * <p>之所以用"外抛"而不是直接依赖 Micrometer：核心库的硬约束是<b>零第三方运行时依赖</b>。
 * 想接入 Micrometer 的使用方只需自己实现本接口（约 20 行），把回调转成
 * {@code Counter}/{@code Timer} 即可，而框架代码一行都不用改。
 *
 * <p><b>实现约束</b>：本接口的方法在采样线程（每 {@code metricsIntervalMillis} 一次）上调用，
 * 允许阻塞到毫秒级；但<b>绝不得</b>抛出异常——抛出会被框架吞掉并累加
 * {@code wal.metrics.sink.error}，长期抛异常会导致指标静默缺失。
 */
public interface MeterSink {

    /**
     * 收到一个计数器的最新值（累计值，非增量）。
     *
     * @param name 指标名，形如 {@code wal.append.records}
     * @param value 累计值
     * @param tags 维度标签（只读），如 {@code {"partition":"3"}}
     */
    void onCounter(String name, long value, Map<String, String> tags);

    /**
     * 收到一个延迟直方图的桶计数快照。
     *
     * @param name 指标名，形如 {@code wal.append.latency}
     * @param buckets 非累计的桶计数快照，长度 = 桶边界数 + 1；
     *                {@code buckets[i]} 表示落在第 i 个桶内的样本数，末位为溢出桶
     * @param boundsNanos 桶上界（纳秒），升序；{@code buckets.length == boundsNanos.length + 1}
     * @param tags 维度标签（只读）
     */
    void onHistogram(String name, long[] buckets, long[] boundsNanos, Map<String, String> tags);

    /**
     * 收到一个标量快照（如日志总字节数、健康度枚举序号）。
     *
     * @param name 指标名
     * @param value 值
     * @param tags 维度标签（只读）
     */
    default void onGauge(String name, long value, Map<String, String> tags) {
        // 默认忽略：只要计数器和直方图的实现可以不关心 gauge
    }
}
