package com.walcore.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 运行期统计快照（一次性取全部指标，避免使用方多次调用产生"跨时刻不一致"的读数）。
 *
 * <p>本类只做<b>数据容器</b>，指标采集在 {@code internal.metrics.MetricsRegistry} 中完成。
 * 所有字段都是采样瞬间的值，字段之间不保证严格处于同一纳秒。
 */
public final class WalStats {

    /**
     * 一个延迟直方图的分位快照（T17：把"分位数"变成 {@code stats()} 的一等公民）。
     *
     * <p>为什么在 {@code api} 包里另立一个视图类、而不是直接暴露内部的
     * {@code LatencyHistogram}：{@code api} 包是使用方唯一的编译依赖面，
     * 里面出现 {@code com.walcore.internal.*} 会让"内部实现可自由重构"这条承诺失效。
     * 这里只搬运三个数字与样本量，副作用是分位口径被固定在文档里，不容易被悄悄改掉。
     *
     * <p>单位一律是<b>纳秒</b>（与桶边界一致）；转毫秒由使用方自己做，避免出现
     * "有的指标是微秒、有的是纳秒"的经典事故。
     */
    public static final class LatencyView {

        private final String name;
        private final long count;
        private final long p50Nanos;
        private final long p99Nanos;
        private final long p999Nanos;

        /**
         * 构造视图。
         *
         * @param name 指标名
         * @param count 样本总数
         * @param p50Nanos p50（纳秒）
         * @param p99Nanos p99（纳秒）
         * @param p999Nanos p999（纳秒）
         */
        public LatencyView(String name, long count, long p50Nanos, long p99Nanos,
                           long p999Nanos) {
            this.name = name == null ? "" : name;
            this.count = count;
            this.p50Nanos = p50Nanos;
            this.p99Nanos = p99Nanos;
            this.p999Nanos = p999Nanos;
        }

        /**
         * 指标名。
         *
         * @return 名称
         */
        public String name() {
            return name;
        }

        /**
         * 样本总数。
         *
         * @return 条数
         */
        public long count() {
            return count;
        }

        /**
         * p50 延迟。
         *
         * @return 纳秒
         */
        public long p50Nanos() {
            return p50Nanos;
        }

        /**
         * p99 延迟。
         *
         * @return 纳秒
         */
        public long p99Nanos() {
            return p99Nanos;
        }

        /**
         * p999 延迟。
         *
         * @return 纳秒
         */
        public long p999Nanos() {
            return p999Nanos;
        }

        @Override
        public String toString() {
            return name + "{n=" + count
                    + ", p50=" + p50Nanos + "ns, p99=" + p99Nanos + "ns, p999="
                    + p999Nanos + "ns}";
        }
    }

    private final Health health;
    private final long totalLogBytes;
    private final long totalRecords;
    private final long publishedLsnMax;
    private final long durableLsnMax;
    private final long offHeapBytes;
    private final long indexHeapBytes;
    private final long corruptionCount;
    private final long rotateFallbackCount;
    private final Map<String, String> activePlugins;
    private final int activePartitions;
    private final long segmentCount;
    private final long rotateCount;
    private final long flushBatches;
    private final long forcedPartitions;
    private final long failedFlushes;
    private final Throwable lastFlushFailure;
    private final Map<String, Long> counters;
    private final Map<String, LatencyView> latencies;
    private final Map<String, Long> gauges;

    /**
     * 构造统计快照。
     *
     * @param health 健康度
     * @param totalLogBytes 日志总字节数（各分区各段之和）
     * @param totalRecords 记录总数
     * @param publishedLsnMax 全分区已发布的最大 LSN
     * @param durableLsnMax 全分区已持久化水位中的最大值
     * @param offHeapBytes 堆外内存占用（映射窗口 + staging）
     * @param indexHeapBytes 索引堆内占用
     * @param corruptionCount 累计损坏事件数
     * @param rotateFallbackCount 累计轮转降级（内联创建段）次数
     * @param activePlugins 当前生效的插件：SPI 类型 -> 插件 id，无则空表
     * @param activePartitions 存活分区数
     */
    public WalStats(Health health, long totalLogBytes, long totalRecords,
                    long publishedLsnMax, long durableLsnMax,
                    long offHeapBytes, long indexHeapBytes,
                    long corruptionCount, long rotateFallbackCount,
                    Map<String, String> activePlugins, int activePartitions) {
        this(health, totalLogBytes, totalRecords, publishedLsnMax, durableLsnMax,
                offHeapBytes, indexHeapBytes, corruptionCount, rotateFallbackCount,
                activePlugins, activePartitions, 0L, 0L, 0L, 0L, 0L, null, null);
    }

    /**
     * 构造<b>完整</b>统计快照（T08 补全：新增段数、轮转次数、组提交计数与原始计数器）。
     *
     * @param health 健康度
     * @param totalLogBytes 日志总字节数
     * @param totalRecords 记录总数
     * @param publishedLsnMax 全分区已发布的最大 LSN
     * @param durableLsnMax 全分区已持久化水位中的最大值
     * @param offHeapBytes 堆外内存占用
     * @param indexHeapBytes 索引堆内占用
     * @param corruptionCount 累计损坏事件数
     * @param rotateFallbackCount 累计轮转降级次数
     * @param activePlugins 当前生效的插件
     * @param activePartitions 存活分区数
     * @param segmentCount 现存段总数
     * @param rotateCount 累计轮转次数
     * @param flushBatches 累计刷盘批次数
     * @param forcedPartitions 累计被刷盘的分区次数
     * @param failedFlushes 累计失败刷盘次数
     * @param lastFlushFailure 最近一次刷盘失败的根因，无则 null
     * @param counters 指标注册中心的原始计数器快照，可为 null
     */
    public WalStats(Health health, long totalLogBytes, long totalRecords,
                    long publishedLsnMax, long durableLsnMax,
                    long offHeapBytes, long indexHeapBytes,
                    long corruptionCount, long rotateFallbackCount,
                    Map<String, String> activePlugins, int activePartitions,
                    long segmentCount, long rotateCount,
                    long flushBatches, long forcedPartitions, long failedFlushes,
                    Throwable lastFlushFailure, Map<String, Long> counters) {
        this(health, totalLogBytes, totalRecords, publishedLsnMax, durableLsnMax,
                offHeapBytes, indexHeapBytes, corruptionCount, rotateFallbackCount,
                activePlugins, activePartitions, segmentCount, rotateCount,
                flushBatches, forcedPartitions, failedFlushes, lastFlushFailure,
                counters, null, null);
    }

    /**
     * 构造<b>全量</b>统计快照（T17：在 T08 的基础上补齐延迟分位与 gauge）。
     *
     * @param health 健康度
     * @param totalLogBytes 日志总字节数
     * @param totalRecords 记录总数
     * @param publishedLsnMax 全分区已发布的最大 LSN
     * @param durableLsnMax 全分区已持久化水位中的最大值
     * @param offHeapBytes 堆外内存占用
     * @param indexHeapBytes 索引堆内占用
     * @param corruptionCount 累计损坏事件数
     * @param rotateFallbackCount 累计轮转降级次数
     * @param activePlugins 当前生效的插件
     * @param activePartitions 存活分区数
     * @param segmentCount 现存段总数
     * @param rotateCount 累计轮转次数
     * @param flushBatches 累计刷盘批次数
     * @param forcedPartitions 累计被刷盘的分区次数
     * @param failedFlushes 累计失败刷盘次数
     * @param lastFlushFailure 最近一次刷盘失败的根因
     * @param counters 原始计数器快照
     * @param latencies 延迟分位快照：指标名 -&gt; 分位视图
     * @param gauges 瞬时标量快照：指标名 -&gt; 值
     */
    public WalStats(Health health, long totalLogBytes, long totalRecords,
                    long publishedLsnMax, long durableLsnMax,
                    long offHeapBytes, long indexHeapBytes,
                    long corruptionCount, long rotateFallbackCount,
                    Map<String, String> activePlugins, int activePartitions,
                    long segmentCount, long rotateCount,
                    long flushBatches, long forcedPartitions, long failedFlushes,
                    Throwable lastFlushFailure, Map<String, Long> counters,
                    Map<String, LatencyView> latencies, Map<String, Long> gauges) {
        this.health = health == null ? Health.HEALTHY : health;
        this.totalLogBytes = totalLogBytes;
        this.totalRecords = totalRecords;
        this.publishedLsnMax = publishedLsnMax;
        this.durableLsnMax = durableLsnMax;
        this.offHeapBytes = offHeapBytes;
        this.indexHeapBytes = indexHeapBytes;
        this.corruptionCount = corruptionCount;
        this.rotateFallbackCount = rotateFallbackCount;
        this.activePlugins = activePlugins == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(activePlugins));
        this.activePartitions = activePartitions;
        this.segmentCount = segmentCount;
        this.rotateCount = rotateCount;
        this.flushBatches = flushBatches;
        this.forcedPartitions = forcedPartitions;
        this.failedFlushes = failedFlushes;
        this.lastFlushFailure = lastFlushFailure;
        this.counters = counters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(counters));
        this.latencies = latencies == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(latencies));
        this.gauges = gauges == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(gauges));
    }

    /**
     * 一个"刚打开、尚未写入"的空闲快照，便于测试与默认值初始化。
     *
     * @return 快照
     */
    public static WalStats empty() {
        return new WalStats(Health.HEALTHY, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null, 0);
    }

    /**
     * 健康度。运维据此决定是否摘流量。
     *
     * @return 健康度
     */
    public Health health() {
        return health;
    }

    /**
     * 日志总字节数（**已写入的数据字节**之和，<b>不是</b>段的配置容量）。
     *
     * <h2>口径（重要，避免和 {@code capacityBytes} 混淆）</h2>
     * 每个段按"真实数据长度"计：
     * <ul>
     *   <li>活跃段 = {@code publishedWritePos()}（已提交的写指针位置）；</li>
     *   <li>封存段 = {@code finalLength()}。</li>
     * </ul>
     * 这与 {@code RetentionManager} 做"按大小保留"判定时用的 {@code SegmentInfo.sizeBytes}
     * 是<b>同一个口径</b>，因此"看板上的日志总量"与"保留策略何时开始删段"永远一致。
     *
     * <p>反例说明为什么不能用容量：{@code segmentSizeBytes=256MB} 的一个活跃段里可能只有
     * 320 字节数据，按容量累加会让本指标虚高 6 个数量级；
     * 且段文件若是预分配的，{@code physicalLength} 同样等于容量，也<b>不能</b>直接用作口径。
     *
     * @return 数据字节数
     */
    public long totalLogBytes() {
        return totalLogBytes;
    }

    /**
     * 记录总数。
     *
     * @return 条数
     */
    public long totalRecords() {
        return totalRecords;
    }

    /**
     * 全分区已发布的最大 LSN。
     *
     * @return LSN
     */
    public long publishedLsnMax() {
        return publishedLsnMax;
    }

    /**
     * 全分区已持久化水位中的最大值。
     *
     * @return LSN
     */
    public long durableLsnMax() {
        return durableLsnMax;
    }

    /**
     * 堆外内存占用（只读段映射窗口 + staging 缓冲）。
     *
     * <p><b>注意</b>：活跃段的 mmap 计入虚拟地址空间与内核 page cache，
     * 不计入本值、不计入 Java 堆、也不计入 {@code MaxDirectMemorySize}（ARCH §16.3）。
     *
     * @return 字节数
     */
    public long offHeapBytes() {
        return offHeapBytes;
    }

    /**
     * 索引堆内占用（{@code long[]} 二分数组之和）。
     *
     * @return 字节数
     */
    public long indexHeapBytes() {
        return indexHeapBytes;
    }

    /**
     * 累计损坏事件数。
     *
     * @return 次数
     */
    public long corruptionCount() {
        return corruptionCount;
    }

    /**
     * 累计轮转降级次数（{@code next == null} 时的内联创建段）。
     *
     * @return 次数
     */
    public long rotateFallbackCount() {
        return rotateFallbackCount;
    }

    /**
     * 当前生效的插件映射：SPI 类型名 -&gt; 插件 id。
     *
     * @return 不可变映射
     */
    public Map<String, String> activePlugins() {
        return activePlugins;
    }

    /**
     * 存活分区数。
     *
     * @return 分区数
     */
    public int activePartitions() {
        return activePartitions;
    }

    /**
     * 现存段总数（全分区求和）。
     *
     * @return 段数
     */
    public long segmentCount() {
        return segmentCount;
    }

    /**
     * 累计段轮转次数。
     *
     * @return 次数
     */
    public long rotateCount() {
        return rotateCount;
    }

    /**
     * 累计组提交刷盘批次数。
     *
     * @return 批次数
     */
    public long flushBatches() {
        return flushBatches;
    }

    /**
     * 累计被刷盘覆盖的分区次数（一次批可覆盖多个分区）。
     *
     * @return 次数
     */
    public long forcedPartitions() {
        return forcedPartitions;
    }

    /**
     * 累计失败刷盘批次数（{@code force()} 抛异常）。
     *
     * @return 次数
     */
    public long failedFlushes() {
        return failedFlushes;
    }

    /**
     * 最近一次刷盘失败的根因（用于快速定位 ENOSPC / EIO），无则 {@code null}。
     *
     * @return 异常，可能为 null
     */
    public Throwable lastFlushFailure() {
        return lastFlushFailure;
    }

    /**
     * 指标注册中心的原始计数器快照（名称 -&gt; 值）。
     *
     * <p>这里是 P1-05 指标的<b>统一出口</b>：直方图分位数需要按需抽样，
     * 因此不在本快照里展开，使用方可通过 {@code MeterSink} 桥接出去。
     *
     * @return 不可变映射
     */
    public Map<String, Long> counters() {
        return counters;
    }

    /**
     * 全部延迟直方图的分位快照：指标名 -&gt; 分位视图。
     *
     * <p>至少包含 {@code wal.append.latency}、{@code wal.flush.latency}、
     * {@code wal.fsync.latency}、{@code wal.groupcommit.batch.records}、
     * {@code wal.tailer.latency}（后两者是"条数"分布，不是时间）。
     *
     * @return 不可变映射
     */
    public Map<String, LatencyView> latencies() {
        return latencies;
    }

    /**
     * 取单个延迟指标的分位视图。
     *
     * @param name 指标名
     * @return 视图；未采集到该指标返回 null
     */
    public LatencyView latency(String name) {
        return name == null ? null : latencies.get(name);
    }

    /**
     * 瞬时标量快照：指标名 -&gt; 值（{@code wal.log.bytes}、{@code wal.health} 等）。
     *
     * <p>与 {@link #counters()} 的区别：counter 只增不减，采样线程取差分算速率；
     * gauge 是"最后一次观测到的值"，直接换算。
     *
     * @return 不可变映射
     */
    public Map<String, Long> gauges() {
        return gauges;
    }

    @Override
    public String toString() {
        return "WalStats{health=" + health
                + ", partitions=" + activePartitions
                + ", records=" + totalRecords
                + ", logBytes=" + totalLogBytes
                + ", publishedLsnMax=" + publishedLsnMax
                + ", durableLsnMax=" + durableLsnMax
                + ", offHeapBytes=" + offHeapBytes
                + ", indexHeapBytes=" + indexHeapBytes
                + ", corruptions=" + corruptionCount
                + ", rotateFallback=" + rotateFallbackCount
                + ", plugins=" + activePlugins
                + '}';
    }
}
