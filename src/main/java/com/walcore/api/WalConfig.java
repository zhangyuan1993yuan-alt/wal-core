package com.walcore.api;

import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.util.Preconditions;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * WAL 实例的<b>不可变</b>配置值对象。
 *
 * <p>所有字段在构造时一次性校验并冻结；运行期只读，因此可以被任意线程无锁共享，
 * 也可以安全地暴露给 SPI 插件（{@code PluginContext.config()}）。
 *
 * <p>本类按 TASKS-wal.md「并行冲突预警」的要求，在 T01 阶段就把 ARCH §3.5 / §6.1 / §7.2 /
 * §9.3 / §16.3 中出现的<b>全部</b>配置项一次写全（含尚未被任何模块读取的字段），
 * 后续任务只读不写，避免多任务反复改同一个类。
 *
 * <h2>三个最容易被误用的默认值（务必先读）</h2>
 * <ul>
 *   <li>{@code batchIntervalMicros = 200}：决定 {@code flushAndWait} 的 p99 主力。
 *       降到 20µs 能把持久确认压到百 µs 级，但吞吐会掉到 15~40 万 TPS。</li>
 *   <li>{@code groupCommitThreads = 2}：分区多且要求低持久确认延迟时增大；分区少且追求
 *       极致吞吐时设为 1。</li>
 *   <li>{@code fsyncPolicy = BATCH}：{@code ALWAYS} 只用于"必须逐条持久确认"的极端场景，
 *       吞吐会掉 2 个数量级。</li>
 * </ul>
 */
public final class WalConfig {

    // ==================================================================
    // 枚举（与 ARCH §17.2 #3 一致，全部内置于本文件）
    // ==================================================================

    /** 刷盘（fsync）策略。 */
    public enum FsyncPolicy {
        /**
         * 每条记录都必须落盘后才返回。语义最强、吞吐最低（每条约一次 fdatasync）。
         *
         * <p>这是 PRD WAL-P0-04 ② "写入线程永不直接调用 fsync" 的<b>唯一豁免</b>：
         * 语义要求同步，因此 {@code append()} 内部会等待 {@code FlushTicket}。
         */
        ALWAYS,
        /**
         * 攒批定时刷（默认）。三重触发：时间窗 {@code batchIntervalMicros}、
         * 条数 {@code batchMaxRecords}、字节数 {@code batchMaxBytes}，任一先到即刷。
         */
        BATCH,
        /**
         * 仅按时间间隔 {@code flushIntervalMillis} 刷盘，忽略条数与字节阈值。
         *
         * <p>适用于"允许崩溃时丢失最后若干毫秒"的审计类日志。
         */
        PERIODIC
    }

    /** 尾部损坏处理策略。 */
    public enum CorruptionPolicy {
        /**
         * 只在<b>最后一个段</b>截断到最近一条完整帧（默认）。
         *
         * <p>若非最后段出现 CRC 错误 → 抛 {@link WalException.CorruptedException}。
         * 理由：中间段损坏意味着历史已确认数据丢失，静默截断会造成 LSN 空洞。
         */
        TAIL_TRUNCATE,
        /** 任何 CRC 不一致立即抛异常，<b>不修改任何文件</b>。用于审计/合规只读场景。 */
        FAIL_FAST,
        /**
         * 在 {@code resyncWindowBytes} 窗口内逐字节重同步，每次跳过都产出
         * {@code CorruptionEvent} 并累加指标。用于容灾抢修。
         */
        SKIP_RECORD
    }

    /** 背压策略。 */
    public enum BackpressurePolicy {
        /** 写入线程 park 等待 flush 完成，最长 {@code backpressureTimeoutMillis}，超时抛异常（默认）。 */
        BLOCK,
        /** 立即抛 {@link WalException.BackpressureException}，不等。 */
        FAIL
    }

    /** Tailer 订阅者消费落后超过缓冲上限时的策略。 */
    public enum TailerOverflowPolicy {
        /** 回调 {@code onOverflow} 并关闭订阅（默认）。 */
        DISCONNECT,
        /** 丢弃积压、直接跳到最新 LSN，记指标 {@code wal.tailer.dropped}。 */
        DROP_TO_LATEST,
        /** 反向背压到写入方（写入吞吐会受慢订阅者影响）。 */
        BLOCK
    }

    /** 恢复期的校验强度。 */
    public enum RecoveryVerifyMode {
        /** 只做帧边界校验，不算 CRC。最快的重启方式。 */
        NONE,
        /** 只对最后一个段逐帧校验 CRC（默认）。RTO 与绝对正确性的折中。 */
        LAST_SEGMENT,
        /** 全部段逐帧校验 CRC。最慢但最彻底。 */
        FULL
    }

    /** 某类 SPI 完全没有可用实现时的行为。 */
    public enum PluginFailurePolicy {
        /** 用内置默认实现并记 WARN + 指标（默认）。 */
        FALLBACK_DEFAULT,
        /** 启动直接失败，错误消息必须含接口全名/已尝试 ClassLoader/provider 文件名/已发现 id。 */
        FAIL_FAST
    }

    /** 段预分配模式（ARCH §6.3）。 */
    public enum PreallocateMode {
        /** {@code setLength} 后直接 mmap，由文件系统按需分配块（默认，创建 < 1ms）。 */
        SPARSE,
        /** {@code setLength} 后用后台线程以 8MB 为单位填 0，消除写入期块分配延迟。 */
        FILL,
        /** 不预分配（不建议：会引入 remap 逻辑）。 */
        NONE
    }

    // ==================================================================
    // 字段
    // ==================================================================

    private final Path baseDir;
    private final int partitionCount;
    private final int maxPartitions;
    private final long segmentSizeBytes;
    private final int segmentMaxRecords;
    private final long segmentMaxAgeMillis;
    private final boolean timeBasedRotation;
    private final int maxPayloadBytes;

    private final FsyncPolicy fsyncPolicy;
    private final int batchIntervalMicros;
    private final int batchMaxRecords;
    private final int batchMaxBytes;
    private final int flushIntervalMillis;
    private final int groupCommitThreads;

    private final int indexIntervalRecords;
    private final long maxIndexHeapBytes;

    private final CorruptionPolicy corruptionPolicy;
    private final RecoveryVerifyMode recoveryVerifyMode;
    private final int resyncWindowBytes;
    private final boolean fastRecoveryOnCleanShutdown;

    private final BackpressurePolicy backpressurePolicy;
    private final long backpressureTimeoutMillis;
    private final long rotationTimeoutMillis;
    private final long maxDirtyBytes;
    private final long flushAndWaitTimeoutMillis;
    private final long forceTimeoutMillis;
    private final long purgeTimeoutMillis;

    private final TailerOverflowPolicy tailerOverflowPolicy;
    private final int tailerBufferSize;
    private final long cursorCheckpointIntervalMillis;

    private final boolean retentionEnabled;
    private final long retentionIntervalMillis;
    private final long retentionMaxAgeMillis;
    private final long retentionMaxTotalBytes;
    private final long minFreeBytes;
    private final boolean allowPartitionIdReuse;

    private final boolean allowForwardCompatibleRead;
    private final long offHeapBudgetBytes;
    private final long stagingBufferBytes;
    private final long mappedWindowBytes;
    private final int mappedWindowCount;
    private final boolean forceParentDirOnCreate;
    private final PreallocateMode preallocateMode;

    private final boolean metricsEnabled;
    private final long metricsIntervalMillis;

    private final PluginFailurePolicy onPluginLoadFailure;
    private final ClassLoader pluginClassLoader;
    private final Path pluginDir;

    private final String codecId;
    private final String storageBackendId;
    private final String indexStoreId;
    private final String retentionPolicyId;
    private final String replicationSinkId;

    // ==================================================================
    // 构造
    // ==================================================================

    /**
     * 从构建器拷贝并校验，产出不可变配置。
     *
     * @param b 已填充的构建器，非 null
     */
    WalConfig(WalBuilder b) {
        Preconditions.checkNotNull(b, "builder");
        this.baseDir = Preconditions.checkNotNull(b.baseDirValue(), "baseDir");
        this.maxPartitions = Preconditions.checkInRange(b.maxPartitionsValue(), 1, 65_536, "maxPartitions");
        this.partitionCount = Preconditions.checkInRange(
                b.partitionCountValue(), 1, this.maxPartitions, "partitionCount");

        // 段容量下界取 1KB（8 字节对齐的 128 倍）而不是"两个最大帧"（33.5MB）：
        // 33.5MB 这个下界会让所有单元测试都无法使用小段（每个段要占 32MB 文件），
        // 而小段对"帧不跨段""轮转边界"这类逻辑的验证是必需的。
        // 下界放宽不会破坏正确性——若段容量小于单帧长度，append 会因容量不足直接失败
        // （错误消息明确），不存在写入半帧的可能。生产默认值仍是 256MB。
        this.segmentSizeBytes = Preconditions.checkInRange(
                b.segmentSizeBytesValue(), 1024L,
                1L << 40, "segmentSizeBytes");
        this.segmentMaxRecords = Preconditions.checkPositive(b.segmentMaxRecordsValue(), "segmentMaxRecords");
        this.segmentMaxAgeMillis = Preconditions.checkPositive(b.segmentMaxAgeMillisValue(), "segmentMaxAgeMillis");
        this.timeBasedRotation = b.timeBasedRotationValue();
        this.maxPayloadBytes = Preconditions.checkInRange(
                b.maxPayloadBytesValue(), FrameLayout.MIN_PAYLOAD_LEN, FrameLayout.MAX_PAYLOAD_LEN,
                "maxPayloadBytes");

        this.fsyncPolicy = Preconditions.checkNotNull(b.fsyncPolicyValue(), "fsyncPolicy");
        this.batchIntervalMicros = Preconditions.checkPositive(b.batchIntervalMicrosValue(), "batchIntervalMicros");
        this.batchMaxRecords = Preconditions.checkPositive(b.batchMaxRecordsValue(), "batchMaxRecords");
        this.batchMaxBytes = Preconditions.checkPositive(b.batchMaxBytesValue(), "batchMaxBytes");
        this.flushIntervalMillis = Preconditions.checkPositive(b.flushIntervalMillisValue(), "flushIntervalMillis");
        this.groupCommitThreads = Preconditions.checkInRange(
                b.groupCommitThreadsValue(), 1, 8, "groupCommitThreads");

        this.indexIntervalRecords = Preconditions.checkPositive(
                b.indexIntervalRecordsValue(), "indexIntervalRecords");
        this.maxIndexHeapBytes = Preconditions.checkPositive(b.maxIndexHeapBytesValue(), "maxIndexHeapBytes");

        this.corruptionPolicy = Preconditions.checkNotNull(b.corruptionPolicyValue(), "corruptionPolicy");
        this.recoveryVerifyMode = Preconditions.checkNotNull(b.recoveryVerifyModeValue(), "recoveryVerifyMode");
        this.resyncWindowBytes = Preconditions.checkInRange(
                b.resyncWindowBytesValue(), FrameLayout.MIN_FRAME_LEN, 1 << 22, "resyncWindowBytes");
        this.fastRecoveryOnCleanShutdown = b.fastRecoveryOnCleanShutdownValue();

        this.backpressurePolicy = Preconditions.checkNotNull(b.backpressurePolicyValue(), "backpressurePolicy");
        this.backpressureTimeoutMillis = Preconditions.checkPositive(
                b.backpressureTimeoutMillisValue(), "backpressureTimeoutMillis");
        this.rotationTimeoutMillis = Preconditions.checkPositive(
                b.rotationTimeoutMillisValue(), "rotationTimeoutMillis");
        this.maxDirtyBytes = Preconditions.checkPositive(b.maxDirtyBytesValue(), "maxDirtyBytes");
        this.flushAndWaitTimeoutMillis = Preconditions.checkPositive(
                b.flushAndWaitTimeoutMillisValue(), "flushAndWaitTimeoutMillis");
        this.forceTimeoutMillis = Preconditions.checkPositive(b.forceTimeoutMillisValue(), "forceTimeoutMillis");
        this.purgeTimeoutMillis = Preconditions.checkPositive(b.purgeTimeoutMillisValue(), "purgeTimeoutMillis");

        this.tailerOverflowPolicy = Preconditions.checkNotNull(
                b.tailerOverflowPolicyValue(), "tailerOverflowPolicy");
        this.tailerBufferSize = Preconditions.checkPositive(b.tailerBufferSizeValue(), "tailerBufferSize");
        this.cursorCheckpointIntervalMillis = Preconditions.checkPositive(
                b.cursorCheckpointIntervalMillisValue(), "cursorCheckpointIntervalMillis");

        this.retentionEnabled = b.retentionEnabledValue();
        this.retentionIntervalMillis = Preconditions.checkPositive(
                b.retentionIntervalMillisValue(), "retentionIntervalMillis");
        this.retentionMaxAgeMillis = Preconditions.checkPositive(
                b.retentionMaxAgeMillisValue(), "retentionMaxAgeMillis");
        this.retentionMaxTotalBytes = Preconditions.checkPositive(
                b.retentionMaxTotalBytesValue(), "retentionMaxTotalBytes");
        this.minFreeBytes = Preconditions.checkPositive(b.minFreeBytesValue(), "minFreeBytes");
        this.allowPartitionIdReuse = b.allowPartitionIdReuseValue();

        this.allowForwardCompatibleRead = b.allowForwardCompatibleReadValue();
        this.offHeapBudgetBytes = Preconditions.checkPositive(b.offHeapBudgetBytesValue(), "offHeapBudgetBytes");
        this.stagingBufferBytes = Preconditions.checkPositive(b.stagingBufferBytesValue(), "stagingBufferBytes");
        this.mappedWindowBytes = Preconditions.checkPositive(b.mappedWindowBytesValue(), "mappedWindowBytes");
        this.mappedWindowCount = Preconditions.checkInRange(b.mappedWindowCountValue(), 1, 1024, "mappedWindowCount");
        this.forceParentDirOnCreate = b.forceParentDirOnCreateValue();
        this.preallocateMode = Preconditions.checkNotNull(b.preallocateModeValue(), "preallocateMode");

        this.metricsEnabled = b.metricsEnabledValue();
        this.metricsIntervalMillis = Preconditions.checkPositive(
                b.metricsIntervalMillisValue(), "metricsIntervalMillis");

        this.onPluginLoadFailure = Preconditions.checkNotNull(
                b.onPluginLoadFailureValue(), "onPluginLoadFailure");
        this.pluginClassLoader = b.pluginClassLoaderValue();
        this.pluginDir = b.pluginDirValue() != null
                ? b.pluginDirValue()
                : this.baseDir.resolve("plugin");

        this.codecId = b.codecIdValue();
        this.storageBackendId = b.storageBackendIdValue();
        this.indexStoreId = b.indexStoreIdValue();
        this.retentionPolicyId = b.retentionPolicyIdValue();
        this.replicationSinkId = b.replicationSinkIdValue();
    }

    // ==================================================================
    // 只读访问器（顺序与 ARCH §12 类图一致，其后为工程实现所需补充）
    // ==================================================================

    /**
     * 日志根目录。
     *
     * @return 根目录
     */
    public Path baseDir() {
        return baseDir;
    }

    /**
     * 启动时创建的分区数。
     *
     * @return 分区数
     */
    public int partitionCount() {
        return partitionCount;
    }

    /**
     * 分区 ID 上限（{@code 0 .. maxPartitions-1}）。
     *
     * @return 上限
     */
    public int maxPartitions() {
        return maxPartitions;
    }

    /**
     * 单段目标大小（字节）。
     *
     * @return 段大小
     */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    /**
     * 单段最大记录数，防止超小记录把索引粒度做得极粗。
     *
     * @return 记录数上限
     */
    public int segmentMaxRecords() {
        return segmentMaxRecords;
    }

    /**
     * 单段最大存活时长（毫秒），仅 {@link #timeBasedRotation()} 为 true 时生效。
     *
     * @return 毫秒
     */
    public long segmentMaxAgeMillis() {
        return segmentMaxAgeMillis;
    }

    /**
     * 是否启用按时长轮转。
     *
     * @return 启用返回 true
     */
    public boolean timeBasedRotation() {
        return timeBasedRotation;
    }

    /**
     * 单条 payload 上限（字节）。
     *
     * @return 上限
     */
    public int maxPayloadBytes() {
        return maxPayloadBytes;
    }

    /**
     * 刷盘策略。
     *
     * @return 策略
     */
    public FsyncPolicy fsyncPolicy() {
        return fsyncPolicy;
    }

    /**
     * 攒批时间窗（微秒）。
     *
     * @return 微秒
     */
    public int batchIntervalMicros() {
        return batchIntervalMicros;
    }

    /**
     * 攒批条数阈值。
     *
     * @return 条数
     */
    public int batchMaxRecords() {
        return batchMaxRecords;
    }

    /**
     * 攒批字节阈值。
     *
     * @return 字节数
     */
    public int batchMaxBytes() {
        return batchMaxBytes;
    }

    /**
     * {@link FsyncPolicy#PERIODIC} 下的固定刷盘间隔（毫秒）。
     *
     * @return 毫秒
     */
    public int flushIntervalMillis() {
        return flushIntervalMillis;
    }

    /**
     * 组提交线程数（1..8）。
     *
     * @return 线程数
     */
    public int groupCommitThreads() {
        return groupCommitThreads;
    }

    /**
     * 稀疏索引粒度：每多少条记录写一条索引项。
     *
     * @return 条数
     */
    public int indexIntervalRecords() {
        return indexIntervalRecords;
    }

    /**
     * 索引堆内占用上限，超限后索引降级为纯文件二分而非 OOM。
     *
     * @return 字节数
     */
    public long maxIndexHeapBytes() {
        return maxIndexHeapBytes;
    }

    /**
     * 损坏处理策略。
     *
     * @return 策略
     */
    public CorruptionPolicy corruptionPolicy() {
        return corruptionPolicy;
    }

    /**
     * 恢复期校验强度。
     *
     * @return 模式
     */
    public RecoveryVerifyMode recoveryVerifyMode() {
        return recoveryVerifyMode;
    }

    /**
     * {@link CorruptionPolicy#SKIP_RECORD} 下的重同步窗口大小（字节）。
     *
     * @return 字节数
     */
    public int resyncWindowBytes() {
        return resyncWindowBytes;
    }

    /**
     * {@code cleanShutdown=true} 时是否跳过最后段逐帧 CRC。
     *
     * @return 跳过返回 true
     */
    public boolean fastRecoveryOnCleanShutdown() {
        return fastRecoveryOnCleanShutdown;
    }

    /**
     * 背压策略。
     *
     * @return 策略
     */
    public BackpressurePolicy backpressurePolicy() {
        return backpressurePolicy;
    }

    /**
     * 背压最长阻塞时长（毫秒）。
     *
     * @return 毫秒
     */
    public long backpressureTimeoutMillis() {
        return backpressureTimeoutMillis;
    }

    /**
     * 下一段未就绪时内联创建的最长等待（毫秒）。
     *
     * @return 毫秒
     */
    public long rotationTimeoutMillis() {
        return rotationTimeoutMillis;
    }

    /**
     * 全局脏字节高水位，超过后触发背压。
     *
     * @return 字节数
     */
    public long maxDirtyBytes() {
        return maxDirtyBytes;
    }

    /**
     * {@code flushAndWait} 的默认超时（毫秒）。
     *
     * @return 毫秒
     */
    public long flushAndWaitTimeoutMillis() {
        return flushAndWaitTimeoutMillis;
    }

    /**
     * 单次 {@code force()} 的 watchdog 超时（毫秒）。
     *
     * @return 毫秒
     */
    public long forceTimeoutMillis() {
        return forceTimeoutMillis;
    }

    /**
     * {@code PURGE} 等待读者退出的最长时长（毫秒）。
     *
     * @return 毫秒
     */
    public long purgeTimeoutMillis() {
        return purgeTimeoutMillis;
    }

    /**
     * Tailer 溢出策略。
     *
     * @return 策略
     */
    public TailerOverflowPolicy tailerOverflowPolicy() {
        return tailerOverflowPolicy;
    }

    /**
     * Tailer 有界环形缓冲容量（条数）。
     *
     * @return 条数
     */
    public int tailerBufferSize() {
        return tailerBufferSize;
    }

    /**
     * 游标 checkpoint 落盘间隔（毫秒）。
     *
     * @return 毫秒
     */
    public long cursorCheckpointIntervalMillis() {
        return cursorCheckpointIntervalMillis;
    }

    /**
     * 是否启用后台保留策略线程。
     *
     * @return 启用返回 true
     */
    public boolean retentionEnabled() {
        return retentionEnabled;
    }

    /**
     * 保留策略评估周期（毫秒）。
     *
     * @return 毫秒
     */
    public long retentionIntervalMillis() {
        return retentionIntervalMillis;
    }

    /**
     * 保留时长（毫秒）。
     *
     * @return 毫秒
     */
    public long retentionMaxAgeMillis() {
        return retentionMaxAgeMillis;
    }

    /**
     * 保留总大小上限（字节）。
     *
     * @return 字节数
     */
    public long retentionMaxTotalBytes() {
        return retentionMaxTotalBytes;
    }

    /**
     * 磁盘最低可用空间告警阈值（字节）。
     *
     * @return 字节数
     */
    public long minFreeBytes() {
        return minFreeBytes;
    }

    /**
     * 是否允许重用已 PURGED 的分区 ID。
     *
     * @return 允许返回 true
     */
    public boolean allowPartitionIdReuse() {
        return allowPartitionIdReuse;
    }

    /**
     * 是否允许读取 version 更高的帧（前向兼容读）。
     *
     * @return 允许返回 true
     */
    public boolean allowForwardCompatibleRead() {
        return allowForwardCompatibleRead;
    }

    /**
     * 堆外内存预算上限（字节）。
     *
     * @return 字节数
     */
    public long offHeapBudgetBytes() {
        return offHeapBudgetBytes;
    }

    /**
     * 单分区 staging 缓冲大小（仅非零拷贝后端使用）。
     *
     * @return 字节数
     */
    public long stagingBufferBytes() {
        return stagingBufferBytes;
    }

    /**
     * 只读段映射窗口大小（字节）。
     *
     * @return 字节数
     */
    public long mappedWindowBytes() {
        return mappedWindowBytes;
    }

    /**
     * 只读段映射窗口数量（同时打开的句柄上界）。
     *
     * @return 窗口数
     */
    public int mappedWindowCount() {
        return mappedWindowCount;
    }

    /**
     * 创建/重命名段后是否对父目录 {@code force(true)}（Windows 上自动降级为 no-op）。
     *
     * @return 需要返回 true
     */
    public boolean forceParentDirOnCreate() {
        return forceParentDirOnCreate;
    }

    /**
     * 段预分配模式。
     *
     * @return 模式
     */
    public PreallocateMode preallocateMode() {
        return preallocateMode;
    }

    /**
     * 是否启用指标采样。
     *
     * @return 启用返回 true
     */
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /**
     * 指标采样周期（毫秒）。
     *
     * @return 毫秒
     */
    public long metricsIntervalMillis() {
        return metricsIntervalMillis;
    }

    /**
     * 某类 SPI 无可用实现时的行为。
     *
     * @return 策略
     */
    public PluginFailurePolicy onPluginLoadFailure() {
        return onPluginLoadFailure;
    }

    /**
     * 用户显式指定的插件 ClassLoader（最高优先级），可为 null。
     *
     * @return ClassLoader 或 null
     */
    public ClassLoader pluginClassLoader() {
        return pluginClassLoader;
    }

    /**
     * 外部插件 JAR 目录，默认 {@code <baseDir>/plugin}。
     *
     * @return 目录
     */
    public Path pluginDir() {
        return pluginDir;
    }

    /**
     * 显式指定的 codec 插件 id（对应 {@code wal.spi.codec}），null 表示按优先级仲裁。
     *
     * @return 插件 id 或 null
     */
    public String codecId() {
        return codecId;
    }

    /**
     * 显式指定的存储后端插件 id（对应 {@code wal.spi.storage}）。
     *
     * @return 插件 id 或 null
     */
    public String storageBackendId() {
        return storageBackendId;
    }

    /**
     * 显式指定的索引插件 id（对应 {@code wal.spi.index}）。
     *
     * @return 插件 id 或 null
     */
    public String indexStoreId() {
        return indexStoreId;
    }

    /**
     * 显式指定的保留策略插件 id（对应 {@code wal.spi.retention}）。
     *
     * @return 插件 id 或 null
     */
    public String retentionPolicyId() {
        return retentionPolicyId;
    }

    /**
     * 显式指定的复制插件 id（对应 {@code wal.spi.replication}）。
     *
     * @return 插件 id 或 null
     */
    public String replicationSinkId() {
        return replicationSinkId;
    }

    // ==================================================================
    // 构建入口
    // ==================================================================

    /**
     * 创建一个全新的构建器（默认值见 {@link WalBuilder} 字段初始化器）。
     *
     * @return 构建器
     */
    public static WalBuilder builder() {
        return new WalBuilder();
    }

    /**
     * 以本配置为起点派生一个构建器，用于做局部覆盖。
     *
     * @return 预填好本配置全部字段的构建器
     */
    public WalBuilder toBuilder() {
        return new WalBuilder(this);
    }

    /**
     * 默认根目录（{@code ./wal-data}）。
     *
     * @return 默认根目录
     */
    public static Path defaultBaseDir() {
        return Paths.get("wal-data").toAbsolutePath().normalize();
    }

    @Override
    public String toString() {
        return "WalConfig{baseDir=" + baseDir
                + ", partitionCount=" + partitionCount
                + ", maxPartitions=" + maxPartitions
                + ", segmentSizeBytes=" + segmentSizeBytes
                + ", fsyncPolicy=" + fsyncPolicy
                + ", batchIntervalMicros=" + batchIntervalMicros
                + ", batchMaxRecords=" + batchMaxRecords
                + ", batchMaxBytes=" + batchMaxBytes
                + ", groupCommitThreads=" + groupCommitThreads
                + ", indexIntervalRecords=" + indexIntervalRecords
                + ", corruptionPolicy=" + corruptionPolicy
                + ", recoveryVerifyMode=" + recoveryVerifyMode
                + ", backpressurePolicy=" + backpressurePolicy
                + ", tailerOverflowPolicy=" + tailerOverflowPolicy
                + ", onPluginLoadFailure=" + onPluginLoadFailure
                + '}';
    }
}
