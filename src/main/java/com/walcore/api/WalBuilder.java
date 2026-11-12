package com.walcore.api;

import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.util.Preconditions;

import java.nio.file.Path;
import java.time.Duration;

/**
 * {@link WalConfig} 的流式构建器，同时是 {@link Wal} 实例的开办入口。
 *
 * <p>设计约束：本类持有<b>可变</b>状态（{@code fields}），因此<b>不是线程安全</b>的；
 * 一次构建完成后请丢弃构建器，共享使用 {@link #buildConfig()} 产出的不可变 {@link WalConfig}。
 *
 * <p>典型用法：
 * <pre>{@code
 * Wal wal = WalConfig.builder()
 *         .baseDir(Paths.get("/data/wal"))
 *         .partitions(16)
 *         .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
 *         .batchIntervalMicros(200)
 *         .groupCommitThreads(2)
 *         .build();
 * }</pre>
 *
 * <p><b>实现进度说明（诚实标注）</b>：{@link #build()}/{@link #open()} 依赖
 * {@code internal.DefaultWal}（生命周期编排 + 恢复 + 写入管线集成），属于 T08/T12 的交付范围。
 * 在 T01~T07 阶段调用这两个方法会抛 {@link UnsupportedOperationException} 并提示当前进度；
 * 配置构建（{@link #buildConfig()}）已完全可用，后续任务不会修改本类的字段集合。
 */
public final class WalBuilder {

    // ---- 目录与分区 ----
    private Path baseDir = WalConfig.defaultBaseDir();
    private int partitionCount = 16;
    private int maxPartitions = 4096;

    // ---- 段 ----
    private long segmentSizeBytes = 268_435_456L;          // 256MB
    private int segmentMaxRecords = 10_000_000;
    private long segmentMaxAgeMillis = 3_600_000L;         // 1h
    private boolean timeBasedRotation = false;
    private WalConfig.PreallocateMode preallocateMode = WalConfig.PreallocateMode.SPARSE;

    // ---- 帧 ----
    private int maxPayloadBytes = FrameLayout.MAX_PAYLOAD_LEN;

    // ---- 组提交 ----
    private WalConfig.FsyncPolicy fsyncPolicy = WalConfig.FsyncPolicy.BATCH;
    private int batchIntervalMicros = 200;
    private int batchMaxRecords = 4096;
    private int batchMaxBytes = 1_048_576;                 // 1MB
    private int flushIntervalMillis = 50;
    private int groupCommitThreads = 2;

    // ---- 索引 ----
    private int indexIntervalRecords = 1000;
    private long maxIndexHeapBytes = 192L * 1024 * 1024;   // 192MB

    // ---- 恢复 ----
    private WalConfig.CorruptionPolicy corruptionPolicy = WalConfig.CorruptionPolicy.TAIL_TRUNCATE;
    private WalConfig.RecoveryVerifyMode recoveryVerifyMode = WalConfig.RecoveryVerifyMode.LAST_SEGMENT;
    private int resyncWindowBytes = 4096;
    private boolean fastRecoveryOnCleanShutdown = true;

    // ---- 背压与超时 ----
    private WalConfig.BackpressurePolicy backpressurePolicy = WalConfig.BackpressurePolicy.BLOCK;
    private long backpressureTimeoutMillis = 50L;
    private long rotationTimeoutMillis = 1000L;
    private long maxDirtyBytes = 33_554_432L;              // 32MB
    private long flushAndWaitTimeoutMillis = 5000L;
    private long forceTimeoutMillis = 10_000L;
    private long purgeTimeoutMillis = 30_000L;

    // ---- Tailer / 游标 ----
    private WalConfig.TailerOverflowPolicy tailerOverflowPolicy =
            WalConfig.TailerOverflowPolicy.DISCONNECT;
    private int tailerBufferSize = 65_536;
    private long cursorCheckpointIntervalMillis = 1000L;

    // ---- 保留策略 ----
    private boolean retentionEnabled = true;
    private long retentionIntervalMillis = 30_000L;
    private long retentionMaxAgeMillis = 7L * 24 * 3600 * 1000;      // 7 天
    private long retentionMaxTotalBytes = 500L * 1024 * 1024 * 1024; // 500GB
    private long minFreeBytes = 1024L * 1024 * 1024;                 // 1GB
    private boolean allowPartitionIdReuse = false;

    // ---- 兼容性与内存预算 ----
    private boolean allowForwardCompatibleRead = true;
    private long offHeapBudgetBytes = 512L * 1024 * 1024;  // 512MB
    private long stagingBufferBytes = 4L * 1024 * 1024;    // 4MB
    private long mappedWindowBytes = 32L * 1024 * 1024;    // 32MB
    private int mappedWindowCount = 8;
    private boolean forceParentDirOnCreate = true;

    // ---- 指标 ----
    private boolean metricsEnabled = true;
    private long metricsIntervalMillis = 1000L;

    // ---- SPI ----
    private WalConfig.PluginFailurePolicy onPluginLoadFailure =
            WalConfig.PluginFailurePolicy.FALLBACK_DEFAULT;
    private ClassLoader pluginClassLoader;
    private Path pluginDir;
    private String codecId;
    private String storageBackendId;
    private String indexStoreId;
    private String retentionPolicyId;
    private String replicationSinkId;

    /**
     * 使用全部默认值创建构建器。
     */
    public WalBuilder() {
        // 默认值全部写在字段初始化器中，便于集中审阅
    }

    /**
     * 从既有配置派生构建器（做局部覆盖用）。
     *
     * @param config 源配置，非 null
     */
    public WalBuilder(WalConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.baseDir = config.baseDir();
        this.partitionCount = config.partitionCount();
        this.maxPartitions = config.maxPartitions();
        this.segmentSizeBytes = config.segmentSizeBytes();
        this.segmentMaxRecords = config.segmentMaxRecords();
        this.segmentMaxAgeMillis = config.segmentMaxAgeMillis();
        this.timeBasedRotation = config.timeBasedRotation();
        this.preallocateMode = config.preallocateMode();
        this.maxPayloadBytes = config.maxPayloadBytes();
        this.fsyncPolicy = config.fsyncPolicy();
        this.batchIntervalMicros = config.batchIntervalMicros();
        this.batchMaxRecords = config.batchMaxRecords();
        this.batchMaxBytes = config.batchMaxBytes();
        this.flushIntervalMillis = config.flushIntervalMillis();
        this.groupCommitThreads = config.groupCommitThreads();
        this.indexIntervalRecords = config.indexIntervalRecords();
        this.maxIndexHeapBytes = config.maxIndexHeapBytes();
        this.corruptionPolicy = config.corruptionPolicy();
        this.recoveryVerifyMode = config.recoveryVerifyMode();
        this.resyncWindowBytes = config.resyncWindowBytes();
        this.fastRecoveryOnCleanShutdown = config.fastRecoveryOnCleanShutdown();
        this.backpressurePolicy = config.backpressurePolicy();
        this.backpressureTimeoutMillis = config.backpressureTimeoutMillis();
        this.rotationTimeoutMillis = config.rotationTimeoutMillis();
        this.maxDirtyBytes = config.maxDirtyBytes();
        this.flushAndWaitTimeoutMillis = config.flushAndWaitTimeoutMillis();
        this.forceTimeoutMillis = config.forceTimeoutMillis();
        this.purgeTimeoutMillis = config.purgeTimeoutMillis();
        this.tailerOverflowPolicy = config.tailerOverflowPolicy();
        this.tailerBufferSize = config.tailerBufferSize();
        this.cursorCheckpointIntervalMillis = config.cursorCheckpointIntervalMillis();
        this.retentionEnabled = config.retentionEnabled();
        this.retentionIntervalMillis = config.retentionIntervalMillis();
        this.retentionMaxAgeMillis = config.retentionMaxAgeMillis();
        this.retentionMaxTotalBytes = config.retentionMaxTotalBytes();
        this.minFreeBytes = config.minFreeBytes();
        this.allowPartitionIdReuse = config.allowPartitionIdReuse();
        this.allowForwardCompatibleRead = config.allowForwardCompatibleRead();
        this.offHeapBudgetBytes = config.offHeapBudgetBytes();
        this.stagingBufferBytes = config.stagingBufferBytes();
        this.mappedWindowBytes = config.mappedWindowBytes();
        this.mappedWindowCount = config.mappedWindowCount();
        this.forceParentDirOnCreate = config.forceParentDirOnCreate();
        this.metricsEnabled = config.metricsEnabled();
        this.metricsIntervalMillis = config.metricsIntervalMillis();
        this.onPluginLoadFailure = config.onPluginLoadFailure();
        this.pluginClassLoader = config.pluginClassLoader();
        this.pluginDir = config.pluginDir();
        this.codecId = config.codecId();
        this.storageBackendId = config.storageBackendId();
        this.indexStoreId = config.indexStoreId();
        this.retentionPolicyId = config.retentionPolicyId();
        this.replicationSinkId = config.replicationSinkId();
    }

    // ==================================================================
    // 流式 setter（ARCH §12 WalBuilder 类图列出的方法 + 补齐全部配置项）
    // ==================================================================

    /**
     * 设置日志根目录。
     *
     * @param dir 根目录，非 null
     * @return this
     */
    public WalBuilder baseDir(Path dir) {
        this.baseDir = Preconditions.checkNotNull(dir, "baseDir");
        return this;
    }

    /**
     * 设置启动时创建的分区数。
     *
     * @param n 分区数，1..maxPartitions
     * @return this
     */
    public WalBuilder partitions(int n) {
        this.partitionCount = n;
        return this;
    }

    /**
     * 设置分区 ID 上限。
     *
     * @param n 上限，1..65536
     * @return this
     */
    public WalBuilder maxPartitions(int n) {
        this.maxPartitions = n;
        return this;
    }

    /**
     * 设置单段目标大小。
     *
     * @param n 字节数
     * @return this
     */
    public WalBuilder segmentSizeBytes(long n) {
        this.segmentSizeBytes = n;
        return this;
    }

    /**
     * 设置单段最大记录数。
     *
     * @param n 条数
     * @return this
     */
    public WalBuilder segmentMaxRecords(int n) {
        this.segmentMaxRecords = n;
        return this;
    }

    /**
     * 设置单段最大存活时长。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder segmentMaxAgeMillis(long millis) {
        this.segmentMaxAgeMillis = millis;
        return this;
    }

    /**
     * 开启/关闭按时长轮转。
     *
     * @param enabled 是否启用
     * @return this
     */
    public WalBuilder timeBasedRotation(boolean enabled) {
        this.timeBasedRotation = enabled;
        return this;
    }

    /**
     * 设置段预分配模式。
     *
     * @param mode 模式
     * @return this
     */
    public WalBuilder preallocateMode(WalConfig.PreallocateMode mode) {
        this.preallocateMode = mode;
        return this;
    }

    /**
     * 设置单条 payload 上限。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder maxPayloadBytes(int bytes) {
        this.maxPayloadBytes = bytes;
        return this;
    }

    /**
     * 设置刷盘策略。
     *
     * @param p 策略
     * @return this
     */
    public WalBuilder fsyncPolicy(WalConfig.FsyncPolicy p) {
        this.fsyncPolicy = p;
        return this;
    }

    /**
     * 设置攒批时间窗（微秒）。
     *
     * @param micros 微秒
     * @return this
     */
    public WalBuilder batchIntervalMicros(int micros) {
        this.batchIntervalMicros = micros;
        return this;
    }

    /**
     * 设置攒批条数阈值。
     *
     * @param records 条数
     * @return this
     */
    public WalBuilder batchMaxRecords(int records) {
        this.batchMaxRecords = records;
        return this;
    }

    /**
     * 设置攒批字节阈值。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder batchMaxBytes(int bytes) {
        this.batchMaxBytes = bytes;
        return this;
    }

    /**
     * 设置 {@link WalConfig.FsyncPolicy#PERIODIC} 的固定刷盘间隔。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder flushIntervalMillis(int millis) {
        this.flushIntervalMillis = millis;
        return this;
    }

    /**
     * 设置组提交线程数。
     *
     * @param n 1..8
     * @return this
     */
    public WalBuilder groupCommitThreads(int n) {
        this.groupCommitThreads = n;
        return this;
    }

    /**
     * 设置稀疏索引粒度。
     *
     * @param records 每多少条一个索引项
     * @return this
     */
    public WalBuilder indexIntervalRecords(int records) {
        this.indexIntervalRecords = records;
        return this;
    }

    /**
     * 设置索引堆内占用上限。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder maxIndexHeapBytes(long bytes) {
        this.maxIndexHeapBytes = bytes;
        return this;
    }

    /**
     * 设置损坏处理策略。
     *
     * @param p 策略
     * @return this
     */
    public WalBuilder corruptionPolicy(WalConfig.CorruptionPolicy p) {
        this.corruptionPolicy = p;
        return this;
    }

    /**
     * 设置恢复期校验强度。
     *
     * @param m 模式
     * @return this
     */
    public WalBuilder recoveryVerifyMode(WalConfig.RecoveryVerifyMode m) {
        this.recoveryVerifyMode = m;
        return this;
    }

    /**
     * 设置重同步窗口大小。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder resyncWindowBytes(int bytes) {
        this.resyncWindowBytes = bytes;
        return this;
    }

    /**
     * 设置是否在 clean shutdown 后跳过逐帧 CRC。
     *
     * @param enabled 是否跳过
     * @return this
     */
    public WalBuilder fastRecoveryOnCleanShutdown(boolean enabled) {
        this.fastRecoveryOnCleanShutdown = enabled;
        return this;
    }

    /**
     * 设置背压策略。
     *
     * @param p 策略
     * @return this
     */
    public WalBuilder backpressurePolicy(WalConfig.BackpressurePolicy p) {
        this.backpressurePolicy = p;
        return this;
    }

    /**
     * 设置背压最长阻塞时长。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder backpressureTimeoutMillis(long millis) {
        this.backpressureTimeoutMillis = millis;
        return this;
    }

    /**
     * 设置轮转等待超时。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder rotationTimeoutMillis(long millis) {
        this.rotationTimeoutMillis = millis;
        return this;
    }

    /**
     * 设置全局脏字节高水位。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder maxDirtyBytes(long bytes) {
        this.maxDirtyBytes = bytes;
        return this;
    }

    /**
     * 设置 {@code flushAndWait} 默认超时。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder flushAndWaitTimeoutMillis(long millis) {
        this.flushAndWaitTimeoutMillis = millis;
        return this;
    }

    /**
     * 设置 fsync watchdog 超时。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder forceTimeoutMillis(long millis) {
        this.forceTimeoutMillis = millis;
        return this;
    }

    /**
     * 设置 PURGE 等待读者退出的最长时长。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder purgeTimeoutMillis(long millis) {
        this.purgeTimeoutMillis = millis;
        return this;
    }

    /**
     * 设置 Tailer 溢出策略。
     *
     * @param p 策略
     * @return this
     */
    public WalBuilder tailerOverflowPolicy(WalConfig.TailerOverflowPolicy p) {
        this.tailerOverflowPolicy = p;
        return this;
    }

    /**
     * 设置 Tailer 缓冲容量。
     *
     * @param records 条数
     * @return this
     */
    public WalBuilder tailerBufferSize(int records) {
        this.tailerBufferSize = records;
        return this;
    }

    /**
     * 设置游标 checkpoint 落盘间隔。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder cursorCheckpointIntervalMillis(long millis) {
        this.cursorCheckpointIntervalMillis = millis;
        return this;
    }

    /**
     * 开启/关闭后台保留策略线程。
     *
     * @param enabled 是否启用
     * @return this
     */
    public WalBuilder retentionEnabled(boolean enabled) {
        this.retentionEnabled = enabled;
        return this;
    }

    /**
     * 设置保留策略评估周期。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder retentionIntervalMillis(long millis) {
        this.retentionIntervalMillis = millis;
        return this;
    }

    /**
     * 设置保留时长。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder retentionMaxAgeMillis(long millis) {
        this.retentionMaxAgeMillis = millis;
        return this;
    }

    /**
     * 设置保留总大小上限。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder retentionMaxTotalBytes(long bytes) {
        this.retentionMaxTotalBytes = bytes;
        return this;
    }

    /**
     * 设置磁盘最低可用空间阈值。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder minFreeBytes(long bytes) {
        this.minFreeBytes = bytes;
        return this;
    }

    /**
     * 设置是否允许重用已 PURGED 的分区 ID。
     *
     * @param allowed 是否允许
     * @return this
     */
    public WalBuilder allowPartitionIdReuse(boolean allowed) {
        this.allowPartitionIdReuse = allowed;
        return this;
    }

    /**
     * 设置是否允许读取版本更高的帧。
     *
     * @param allowed 是否允许
     * @return this
     */
    public WalBuilder allowForwardCompatibleRead(boolean allowed) {
        this.allowForwardCompatibleRead = allowed;
        return this;
    }

    /**
     * 设置堆外内存预算。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder offHeapBudgetBytes(long bytes) {
        this.offHeapBudgetBytes = bytes;
        return this;
    }

    /**
     * 设置单分区 staging 缓冲大小。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder stagingBufferBytes(long bytes) {
        this.stagingBufferBytes = bytes;
        return this;
    }

    /**
     * 设置只读段映射窗口大小。
     *
     * @param bytes 字节数
     * @return this
     */
    public WalBuilder mappedWindowBytes(long bytes) {
        this.mappedWindowBytes = bytes;
        return this;
    }

    /**
     * 设置只读段映射窗口数量。
     *
     * @param n 窗口数
     * @return this
     */
    public WalBuilder mappedWindowCount(int n) {
        this.mappedWindowCount = n;
        return this;
    }

    /**
     * 设置是否在创建段后对父目录 {@code force(true)}。
     *
     * @param enabled 是否启用
     * @return this
     */
    public WalBuilder forceParentDirOnCreate(boolean enabled) {
        this.forceParentDirOnCreate = enabled;
        return this;
    }

    /**
     * 开启/关闭指标采样。
     *
     * @param enabled 是否启用
     * @return this
     */
    public WalBuilder metricsEnabled(boolean enabled) {
        this.metricsEnabled = enabled;
        return this;
    }

    /**
     * 设置指标采样周期。
     *
     * @param millis 毫秒
     * @return this
     */
    public WalBuilder metricsIntervalMillis(long millis) {
        this.metricsIntervalMillis = millis;
        return this;
    }

    /**
     * 设置 SPI 无可用实现时的行为。
     *
     * @param p 策略
     * @return this
     */
    public WalBuilder onPluginLoadFailure(WalConfig.PluginFailurePolicy p) {
        this.onPluginLoadFailure = p;
        return this;
    }

    /**
     * 指定插件 ClassLoader（最高优先级）。
     *
     * @param cl ClassLoader，可为 null
     * @return this
     */
    public WalBuilder pluginClassLoader(ClassLoader cl) {
        this.pluginClassLoader = cl;
        return this;
    }

    /**
     * 指定外部插件 JAR 目录。
     *
     * @param dir 目录，可为 null（默认 {@code <baseDir>/plugin}）
     * @return this
     */
    public WalBuilder pluginDir(Path dir) {
        this.pluginDir = dir;
        return this;
    }

    /**
     * 强制指定 codec 插件 id（对应 {@code wal.spi.codec}）。
     *
     * @param id 插件 id，null 表示按优先级仲裁
     * @return this
     */
    public WalBuilder codec(String id) {
        this.codecId = id;
        return this;
    }

    /**
     * 强制指定存储后端插件 id（对应 {@code wal.spi.storage}）。
     *
     * @param id 插件 id，null 表示按平台探测自动选择
     * @return this
     */
    public WalBuilder storageBackend(String id) {
        this.storageBackendId = id;
        return this;
    }

    /**
     * 强制指定索引插件 id（对应 {@code wal.spi.index}）。
     *
     * @param id 插件 id
     * @return this
     */
    public WalBuilder indexStore(String id) {
        this.indexStoreId = id;
        return this;
    }

    /**
     * 强制指定保留策略插件 id（对应 {@code wal.spi.retention}）。
     *
     * @param id 插件 id
     * @return this
     */
    public WalBuilder retentionPolicy(String id) {
        this.retentionPolicyId = id;
        return this;
    }

    /**
     * 强制指定复制插件 id（对应 {@code wal.spi.replication}）。
     *
     * @param id 插件 id
     * @return this
     */
    public WalBuilder replicationSink(String id) {
        this.replicationSinkId = id;
        return this;
    }

    // ==================================================================
    // 构建
    // ==================================================================

    /**
     * 构建并校验不可变配置。
     *
     * @return 配置对象
     * @throws IllegalArgumentException 任一配置项越界
     */
    public WalConfig buildConfig() {
        return new WalConfig(this);
    }

    /**
     * 构建配置并打开 WAL 实例。
     *
     * @return WAL 实例
     */
    public Wal build() {
        return open();
    }

    /**
     * 构建配置并打开 WAL 实例（{@link #build()} 的语义化别名）。
     *
     * <p>完整的开办流程见 {@code internal.DefaultWal#open}：{@code LOCK} 独占 →
     * 读 {@code wal.meta} → 插件仲裁 → 崩溃恢复（扫描/逻辑截断/索引重建）→
     * {@code nextLsn} 计算 → 补齐分区 → 启动后台线程 → 写 {@code cleanShutdown=false}。
     *
     * @return WAL 实例
     * @throws com.walcore.api.WalException.AlreadyOpenException 目录已被占用
     * @throws com.walcore.api.WalException.RecoveryFailedException 恢复失败
     */
    public Wal open() {
        return com.walcore.internal.DefaultWal.open(buildConfig());
    }

    /**
     * 构建配置并打开 WAL 实例（忽略此处的关闭超时参数，实例级别用
     * {@link Wal#close(java.time.Duration)} 单独控制）。
     *
     * @param ignoredCloseTimeout 关闭超时（仅做参数校验，不参与开办）
     * @return WAL 实例
     */
    public Wal open(Duration ignoredCloseTimeout) {
        Preconditions.checkNotNull(ignoredCloseTimeout, "closeTimeout");
        return open();
    }

    // ==================================================================
    // 包内可见的原始值访问器（供 WalConfig 拷贝与校验）
    // ==================================================================

    Path baseDirValue() {
        return baseDir;
    }

    int partitionCountValue() {
        return partitionCount;
    }

    int maxPartitionsValue() {
        return maxPartitions;
    }

    long segmentSizeBytesValue() {
        return segmentSizeBytes;
    }

    int segmentMaxRecordsValue() {
        return segmentMaxRecords;
    }

    long segmentMaxAgeMillisValue() {
        return segmentMaxAgeMillis;
    }

    boolean timeBasedRotationValue() {
        return timeBasedRotation;
    }

    WalConfig.PreallocateMode preallocateModeValue() {
        return preallocateMode;
    }

    int maxPayloadBytesValue() {
        return maxPayloadBytes;
    }

    WalConfig.FsyncPolicy fsyncPolicyValue() {
        return fsyncPolicy;
    }

    int batchIntervalMicrosValue() {
        return batchIntervalMicros;
    }

    int batchMaxRecordsValue() {
        return batchMaxRecords;
    }

    int batchMaxBytesValue() {
        return batchMaxBytes;
    }

    int flushIntervalMillisValue() {
        return flushIntervalMillis;
    }

    int groupCommitThreadsValue() {
        return groupCommitThreads;
    }

    int indexIntervalRecordsValue() {
        return indexIntervalRecords;
    }

    long maxIndexHeapBytesValue() {
        return maxIndexHeapBytes;
    }

    WalConfig.CorruptionPolicy corruptionPolicyValue() {
        return corruptionPolicy;
    }

    WalConfig.RecoveryVerifyMode recoveryVerifyModeValue() {
        return recoveryVerifyMode;
    }

    int resyncWindowBytesValue() {
        return resyncWindowBytes;
    }

    boolean fastRecoveryOnCleanShutdownValue() {
        return fastRecoveryOnCleanShutdown;
    }

    WalConfig.BackpressurePolicy backpressurePolicyValue() {
        return backpressurePolicy;
    }

    long backpressureTimeoutMillisValue() {
        return backpressureTimeoutMillis;
    }

    long rotationTimeoutMillisValue() {
        return rotationTimeoutMillis;
    }

    long maxDirtyBytesValue() {
        return maxDirtyBytes;
    }

    long flushAndWaitTimeoutMillisValue() {
        return flushAndWaitTimeoutMillis;
    }

    long forceTimeoutMillisValue() {
        return forceTimeoutMillis;
    }

    long purgeTimeoutMillisValue() {
        return purgeTimeoutMillis;
    }

    WalConfig.TailerOverflowPolicy tailerOverflowPolicyValue() {
        return tailerOverflowPolicy;
    }

    int tailerBufferSizeValue() {
        return tailerBufferSize;
    }

    long cursorCheckpointIntervalMillisValue() {
        return cursorCheckpointIntervalMillis;
    }

    boolean retentionEnabledValue() {
        return retentionEnabled;
    }

    long retentionIntervalMillisValue() {
        return retentionIntervalMillis;
    }

    long retentionMaxAgeMillisValue() {
        return retentionMaxAgeMillis;
    }

    long retentionMaxTotalBytesValue() {
        return retentionMaxTotalBytes;
    }

    long minFreeBytesValue() {
        return minFreeBytes;
    }

    boolean allowPartitionIdReuseValue() {
        return allowPartitionIdReuse;
    }

    boolean allowForwardCompatibleReadValue() {
        return allowForwardCompatibleRead;
    }

    long offHeapBudgetBytesValue() {
        return offHeapBudgetBytes;
    }

    long stagingBufferBytesValue() {
        return stagingBufferBytes;
    }

    long mappedWindowBytesValue() {
        return mappedWindowBytes;
    }

    int mappedWindowCountValue() {
        return mappedWindowCount;
    }

    boolean forceParentDirOnCreateValue() {
        return forceParentDirOnCreate;
    }

    boolean metricsEnabledValue() {
        return metricsEnabled;
    }

    long metricsIntervalMillisValue() {
        return metricsIntervalMillis;
    }

    WalConfig.PluginFailurePolicy onPluginLoadFailureValue() {
        return onPluginLoadFailure;
    }

    ClassLoader pluginClassLoaderValue() {
        return pluginClassLoader;
    }

    Path pluginDirValue() {
        return pluginDir;
    }

    String codecIdValue() {
        return codecId;
    }

    String storageBackendIdValue() {
        return storageBackendId;
    }

    String indexStoreIdValue() {
        return indexStoreId;
    }

    String retentionPolicyIdValue() {
        return retentionPolicyId;
    }

    String replicationSinkIdValue() {
        return replicationSinkId;
    }
}
