package com.walcore.internal;

import com.walcore.api.AppendOptions;
import com.walcore.api.CloseReport;
import com.walcore.api.CursorOptions;
import com.walcore.api.Health;
import com.walcore.api.PartitionInfo;
import com.walcore.api.PartitionOptions;
import com.walcore.api.ReplayListener;
import com.walcore.api.RecoveryReport;
import com.walcore.api.RemoveMode;
import com.walcore.api.TailerOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalStats;
import com.walcore.api.WalTailer;
import com.walcore.api.WalVersion;
import com.walcore.internal.append.AppendPipeline;
import com.walcore.internal.append.DirtyTracker;
import com.walcore.internal.append.FrameEncoder;
import com.walcore.internal.append.LsnAllocator;
import com.walcore.internal.groupcommit.GroupCommitCoordinator;
import com.walcore.internal.io.MappedWindowCache;
import com.walcore.internal.io.StorageBackendFactory;
import com.walcore.internal.metrics.LatencyHistogram;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.partition.PartitionStateFile;
import com.walcore.internal.plugin.PluginManager;
import com.walcore.internal.read.CursorCheckpointStore;
import com.walcore.internal.read.CursorImpl;
import com.walcore.internal.read.FrameDecoder;
import com.walcore.internal.read.RetentionPin;
import com.walcore.internal.read.TailerImpl;
import com.walcore.internal.recovery.RecoveryManager;
import com.walcore.internal.retention.RetentionManager;
import com.walcore.internal.snapshot.CheckpointManager;
import com.walcore.internal.snapshot.FileSnapshotStore;
import com.walcore.internal.spiimpl.TimeSizeRetentionPolicy;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.MetaFile;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link Wal} 的门面实现：生命周期编排 + 全部子系统的装配与委托。
 *
 * <h2>开办顺序（ARCH §9.1，顺序不可调整）</h2>
 * <ol>
 *   <li>{@code LOCK} 独占 → 双实例打开同一目录立刻抛 {@code AlreadyOpenException}；</li>
 *   <li>读 {@code wal.meta}（含 CRC 校验），拿到 {@code cleanShutdown} 与上次的 {@code nextLsn}；</li>
 *   <li>插件仲裁（codec / storage / index 三类）；</li>
 *   <li>{@link RecoveryManager} 扫描 → 校验 → <b>逻辑</b>截断 → 索引校验/重建 → 计算 {@code nextLsn}；</li>
 *   <li>{@code LsnAllocator.rewind(nextLsn)}；</li>
 *   <li>补齐配置里声明的分区（无段则建首段）并预热预建线程；</li>
 *   <li>启动组提交线程，写 {@code wal.meta(cleanShutdown=false)}。</li>
 * </ol>
 *
 * <h2>关闭顺序（ARCH §7.7，顺序不可调整）</h2>
 * <ol>
 *   <li>置 {@code closed} —— 之后所有写入抛 {@code ClosedException}；</li>
 *   <li><b>逐分区拿写锁</b>（限期重试，超时进 {@code CloseReport} 不静默）——
 *       排干 in-flight append，杜绝"close 卸映射 vs 写线程写映射"的
 *       use-after-unmap 竞争；</li>
 *   <li>关闭 pipeline（{@code removePartition} 冻结的超时分区延迟释放 staging 区）；</li>
 *   <li>排空组提交（{@code drainAllAndStop}），拿到"还有哪些分区没刷完"；</li>
 *   <li>每个活跃段 {@code force(true)}（写锁超时的分区跳过并计入 unflushed）；</li>
 *   <li>写 {@code wal.meta(cleanShutdown=true, nextLsn)}（临时文件 + ATOMIC_MOVE）；</li>
 *   <li>释放分区写锁，关闭段句柄（Mapped 后端在此 unmap，Windows 才能删文件）与存储后端；</li>
 *   <li>最后释放 {@code LOCK}。</li>
 * </ol>
 * 只有 2~4 全部成功，重启才可能走 {@code cleanShutdown=true} 的快速路径；
 * 任一步失败或超时都返回 {@code clean=false} 的报告而<b>不抛异常</b>。
 */
public final class DefaultWal implements Wal {

    /**
     * close 时每个分区写锁的等待上限（毫秒）。
     *
     * <p>写线程的临界区是亚微秒级的（锁内复核 + 编码 + memcpy + 发布），正常情况下
     * 第一次 tryLock 就能拿到；等这么久还拿不到说明有异常（例如线程在临界区内被
     * 换出/阻塞在 IO 上），此时放弃并在 {@code CloseReport} 里如实报告，而不是
     * 让 close 无限期挂住。
     */
    private static final long CLOSE_LOCK_WAIT_CAP_MILLIS = 5_000L;

    private final WalConfig config;
    private final Path baseDir;
    private final MetricsRegistry metrics;
    private final PluginManager pluginManager;
    private final StorageBackend backend;
    private final PartitionRegistry registry;
    private final LsnAllocator lsnAllocator;
    private final IndexStore indexStore;
    private final SegmentManager segmentManager;
    private final DirtyTracker dirtyTracker;
    private final GroupCommitCoordinator coordinator;
    private final AppendPipeline pipeline;
    private final FrameEncoder encoder;
    private final FrameDecoder decoder;
    private final MappedWindowCache windowCache;
    private final CursorCheckpointStore checkpointStore;
    private final RetentionManager retentionManager;
    private final CheckpointManager checkpointManager;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final RecoveryReport openReport;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 本进程内已被 {@code PURGE} 的分区 ID（tombstone）。
     *
     * <p>为什么需要它：{@code PURGE} 会把分区目录整个删掉，之后 {@code registry} 里
     * 就没有这个 ID 了。如果没有 tombstone，"删错了想重建"和"删错了被上游自动重建"
     * 就完全无法区分——而回滚的旧备份文件被写进一个本该永久消失的 ID，正是
     * {@link com.walcore.api.RemoveMode#PURGE} 想避免的事故。因此默认拒绝复用，
     * 必须显式传 {@code PartitionOptions.allowingIdReuse(true)} 才放行。
     *
     * <p>范围限制：只记在内存里。进程重启后目录已经不存在，恢复流程不会凭空复活它，
     * 因此不需要跨进程的持久 tombstone（也就避免了"tombstone 文件本身丢失"的新故障面）。
     */
    private final Set<Integer> purgedPartitionIds = ConcurrentHashMap.newKeySet();

    private DefaultWal(Builder b) {
        this.config = b.config;
        this.baseDir = b.baseDir;
        this.metrics = b.metrics;
        this.pluginManager = b.pluginManager;
        this.backend = b.backend;
        this.registry = b.registry;
        this.lsnAllocator = b.lsnAllocator;
        this.indexStore = b.indexStore;
        this.segmentManager = b.segmentManager;
        this.dirtyTracker = b.dirtyTracker;
        this.coordinator = b.coordinator;
        this.pipeline = b.pipeline;
        this.encoder = b.encoder;
        this.decoder = b.decoder;
        this.windowCache = b.windowCache;
        this.checkpointStore = b.checkpointStore;
        this.retentionManager = b.retentionManager;
        this.checkpointManager = b.checkpointManager;
        this.lockChannel = b.lockChannel;
        this.lock = b.lock;
        this.openReport = b.openReport;
    }

    // ==================================================================
    // 开办
    // ==================================================================

    /**
     * 按配置打开一个 WAL 实例。
     *
     * @param config 配置
     * @return 实例
     * @throws WalException.AlreadyOpenException 目录已被另一个实例占用
     * @throws WalException.RecoveryFailedException 恢复失败
     * @throws WalException.IoException IO 失败
     */
    public static DefaultWal open(WalConfig config) {
        Preconditions.checkNotNull(config, "config");
        Path baseDir = config.baseDir();
        try {
            Files.createDirectories(baseDir);
            Files.createDirectories(baseDir.resolve(SegmentNaming.PARTITIONS_DIR));
            if (config.pluginDir() != null) {
                Files.createDirectories(config.pluginDir());
            }
        } catch (IOException e) {
            throw new WalException.IoException("创建日志目录失败: " + baseDir, e);
        }

        FileChannel lockChannel = null;
        FileLock lock = null;
        try {
            Path lockPath = baseDir.resolve(SegmentNaming.LOCK_FILE);
            lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            lock = lockChannel.tryLock();
            if (lock == null) {
                throw new WalException.AlreadyOpenException(
                        "同一目录已被另一个 WAL 实例占用（LOCK 文件被锁定）: " + lockPath);
            }
        } catch (OverlappingFileLockException e) {
            closeQuietly(lockChannel);
            throw new WalException.AlreadyOpenException(
                    "同一 JVM 内已有一个 WAL 实例占用了该目录: " + baseDir);
        } catch (IOException e) {
            closeQuietly(lockChannel);
            throw new WalException.IoException("获取 LOCK 文件失败: " + baseDir, e);
        }

        MetricsRegistry metrics = new MetricsRegistry(config.metricsEnabled());
        PluginManager pluginManager = new PluginManager(config, baseDir, metrics);
        StorageBackend backend = null;
        SegmentManager segmentManager = null;
        GroupCommitCoordinator coordinator = null;
        MappedWindowCache windowCache = null;
        // ---- F5：失败清理链需要覆盖后创建的组件，因此声明提到 try 外 ----
        // retention 线程会访问 segmentManager 的段（删除已封存段），pipeline 持有
        // staging 区（mmap 后端是映射内存）。open 中途失败（例如 writeMeta 抛
        // IOException）时它们不会被 DefaultWal 构造函数接管，必须在 catch 里显式
        // 关闭，否则留下 wal-retention 线程与未 unmap 的映射（Windows 上目录还被
        // 占着，连删除 baseDir 都会失败）。
        RetentionManager retention = null;
        AppendPipeline pipeline = null;
        try {
            MetaFile.Meta meta = MetaFile.read(baseDir);
            boolean cleanShutdown = meta.trusted()
                    && meta.getBoolean(MetaFile.KEY_CLEAN_SHUTDOWN, false);
            // meta 里记的 nextLsn 是"上次关闭时已知的下一个 LSN"，它比"扫描出来的 max+1"
            // 更保守：分区被 PURGE、或尾部被截断时，扫描只能看到幸存数据，
            // 若直接取 max+1 就会把已经发给下游的 LSN 重新发一遍，
            // 下游按 LSN 幂等重放时会把新记录当成旧的丢掉。因此取两者的大值。
            long metaNextLsn = meta.trusted()
                    ? meta.getLong(MetaFile.KEY_NEXT_LSN, LsnAllocator.DEFAULT_FIRST_LSN)
                    : LsnAllocator.DEFAULT_FIRST_LSN;

            pluginManager.scan();
            String backendId = config.storageBackendId() != null
                    && !config.storageBackendId().isEmpty()
                    ? config.storageBackendId()
                    : pluginManager.storageBackendId();
            StorageConfig storageConfig = new StorageConfig(baseDir, config.segmentSizeBytes(),
                    config.preallocateMode() == WalConfig.PreallocateMode.FILL,
                    config.mappedWindowBytes(), config.mappedWindowCount(),
                    config.forceParentDirOnCreate());
            backend = new StorageBackendFactory().create(backendId, storageConfig,
                    contextOf(config, baseDir, metrics, pluginManager));

            PartitionRegistry registry = new PartitionRegistry(config.maxPartitions());
            LsnAllocator lsnAllocator = new LsnAllocator(LsnAllocator.DEFAULT_FIRST_LSN);
            IndexStore indexStore = pluginManager.indexStore();
            segmentManager = new SegmentManager(backend, registry, baseDir, config,
                    lsnAllocator, indexStore, metrics);

            RecoveryManager recovery = new RecoveryManager(backend, registry, segmentManager,
                    indexStore, pluginManager.codec(), config, metrics);
            RecoveryReport report = recovery.recover(cleanShutdown, null);
            lsnAllocator.rewind(Math.max(report.nextLsn(), metaNextLsn));

            // 补齐配置声明的分区（首次开办时创建 0..partitionCount-1）
            for (int pid = 0; pid < config.partitionCount(); pid++) {
                ensurePartition(pid, registry, segmentManager, lsnAllocator);
            }
            // 恢复期发现的历史分区也要保证有活跃段
            for (int pid : registry.activeIdsCopy()) {
                ensurePartition(pid, registry, segmentManager, lsnAllocator);
            }

            segmentManager.start();

            DirtyTracker dirtyTracker = new DirtyTracker(config.maxPartitions(),
                    config.maxDirtyBytes());
            coordinator = new GroupCommitCoordinator(registry, dirtyTracker, config, metrics);
            coordinator.start();

            FrameEncoder encoder = new FrameEncoder(pluginManager.codec());
            pipeline = new AppendPipeline(registry, lsnAllocator, encoder,
                    dirtyTracker, coordinator, segmentManager, config, metrics);
            FrameDecoder decoder = new FrameDecoder(pluginManager.codec(),
                    config.allowForwardCompatibleRead());
            windowCache = new MappedWindowCache(config.mappedWindowCount(),
                    Math.max(1L, config.mappedWindowBytes() / Math.max(1, config.mappedWindowCount())));

            // ---- 快照 / checkpoint（T15）----
            // 必须在 decoder 与 windowCache 之后就绪：CheckpointManager 的重放需要真游标。
            FileSnapshotStore snapshotStore = new FileSnapshotStore(baseDir);
            snapshotStore.init(pluginManager.pluginContext());
            CursorCheckpointStore cursorCheckpoints = new CursorCheckpointStore(baseDir);
            final FrameDecoder replayDecoder = decoder;
            final MappedWindowCache replayWindows = windowCache;
            final SegmentManager replaySegments = segmentManager;
            CheckpointManager checkpoints = new CheckpointManager(snapshotStore,
                    (pid, fromLsn) -> new CursorImpl(pid, replaySegments, replayDecoder,
                            CursorOptions.builder().fromLsn(fromLsn)
                                    .resumeFromCheckpoint(false).build(),
                            cursorCheckpoints, replayWindows),
                    metrics);

            // ---- 保留策略（T14）：pinSource 与快照联动，保证"快照点之后"的段不被删 ----
            com.walcore.spi.RetentionPolicy retentionPolicy = pluginManager.retentionPolicy();
            if (retentionPolicy == null) {
                // 理论不可达（scan 一定产出内置实现），保险起见兜一个
                TimeSizeRetentionPolicy builtin = new TimeSizeRetentionPolicy();
                builtin.init(pluginManager.pluginContext());
                retentionPolicy = builtin;
            }
            retention = new RetentionManager(segmentManager, registry,
                    retentionPolicy, config, metrics, checkpoints::pinnedLsn);
            if (config.retentionEnabled()) {
                retention.start();
            }

            writeMeta(false, lsnAllocator.peek(), registry.activeIdsCopy(), config);

            Builder builder = new Builder();
            builder.config = config;
            builder.baseDir = baseDir;
            builder.metrics = metrics;
            builder.pluginManager = pluginManager;
            builder.backend = backend;
            builder.registry = registry;
            builder.lsnAllocator = lsnAllocator;
            builder.indexStore = indexStore;
            builder.segmentManager = segmentManager;
            builder.dirtyTracker = dirtyTracker;
            builder.coordinator = coordinator;
            builder.pipeline = pipeline;
            builder.encoder = encoder;
            builder.decoder = decoder;
            builder.windowCache = windowCache;
            builder.checkpointStore = cursorCheckpoints;
            builder.retentionManager = retention;
            builder.checkpointManager = checkpoints;
            builder.lockChannel = lockChannel;
            builder.lock = lock;
            builder.openReport = report;
            return new DefaultWal(builder);
        } catch (WalException e) {
            closeQuietly(coordinator);
            closeQuietly(retention);
            closeQuietly(pipeline);
            closeQuietly(segmentManager);
            closeQuietly(windowCache);
            closeQuietly(backend);
            closeQuietly(pluginManager);
            releaseLock(lock, lockChannel);
            throw e;
        } catch (IOException | RuntimeException e) {
            closeQuietly(coordinator);
            closeQuietly(retention);
            closeQuietly(pipeline);
            closeQuietly(segmentManager);
            closeQuietly(windowCache);
            closeQuietly(backend);
            closeQuietly(pluginManager);
            releaseLock(lock, lockChannel);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new WalException.RecoveryFailedException("开办 WAL 失败: " + baseDir, e);
        }
    }

    private static void ensurePartition(int partitionId, PartitionRegistry registry,
                                        SegmentManager segmentManager,
                                        LsnAllocator lsnAllocator) throws IOException {
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            partition = new PartitionImpl(partitionId);
            registry.register(partition);
        }
        if (!partition.isWritable()) {
            // 冻结/归档的分区在重启后仍然不可写：不能再给它建活跃段（否则每次重启
            // 都会在磁盘上留下一个永远写不进数据的空段，`.wal` 文件数会随重启次数增长）。
            return;
        }
        if (segmentManager.activeSegment(partitionId) == null) {
            Segment first;
            if (segmentManager.segmentCount(partitionId) == 0) {
                first = segmentManager.createFirstSegment(partitionId, lsnAllocator.peek());
            } else {
                // 恢复后旧段全部只读且活跃指针已清空：新段的<b>名字</b>必须由
                // reserveNameLsn 发号，不能用 peek()——peek 很可能等于某个旧段名，
                // 覆盖仍被映射的旧文件在 Windows 上会 AccessDenied。
                first = segmentManager.createNextSegment(partitionId);
            }
            partition.setActiveSegment(first);
        }
        segmentManager.warmUp(partitionId);
    }

    private static PluginContext contextOf(WalConfig config, Path baseDir,
                                           MetricsRegistry metrics, PluginManager manager) {
        return new PluginContext() {
            @Override
            public WalConfig config() {
                return config;
            }

            @Override
            public int partitionCount() {
                return config.partitionCount();
            }

            @Override
            public MetricsRegistry metrics() {
                return metrics;
            }

            @Override
            public ClassLoader pluginClassLoader() {
                return DefaultWal.class.getClassLoader();
            }

            @Override
            public Path baseDir() {
                return baseDir;
            }

            @Override
            public void warn(String code, String message, Throwable cause) {
                if (metrics != null) {
                    metrics.increment("wal.plugin.warn." + code);
                }
            }

            @Override
            public String toString() {
                return "PluginContext{" + baseDir + '}';
            }
        };
    }

    // ==================================================================
    // Wal：配置与生命周期
    // ==================================================================

    @Override
    public WalConfig config() {
        return config;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * 开办时算出的恢复报告（供运维展示"上次是怎么恢复的"）。
     *
     * @return 报告
     */
    public RecoveryReport openReport() {
        return openReport;
    }

    @Override
    public void close() {
        close(Duration.ofSeconds(30));
    }

    @Override
    public CloseReport close(Duration timeout) {
        Preconditions.checkNotNull(timeout, "timeout");
        long started = System.nanoTime();
        if (!closed.compareAndSet(false, true)) {
            return CloseReport.clean(0L);
        }

        List<Integer> unflushed = new ArrayList<>();
        long maxUnflushedLsn = 0L;
        boolean clean = true;
        String detail = null;

        // ---- F2：先按分区拿写锁，把 in-flight append 排干，再动 pipeline/段 ----
        //
        // 此前 close() 第一步就 pipeline.close()（unmap staging 区）并冻结段：
        // 一个已经通过 closed/isWritable 预检、正在临界区内写帧的线程会与 unmap
        // 竞争（use-after-unmap，native 崩溃风险）。正确顺序是先把每个分区的写锁
        // 拿到手——拿到即证明该分区没有线程在临界区内；此后新进入的写线程会先
        // 撞上 pipeline.closed（ClosedException），再撞上锁内复核。
        //
        // 锁序约束（与写入路径一致，不得反向）：close 只拿分区写锁；
        // drainAllAndStop 只 join flusher（flusher 不取分区锁）；
        // force 只走段句柄。全程不取 ps.mutex / 段监视器。
        long lockWaitMillis = Math.max(1L,
                Math.min(timeout.toMillis(), CLOSE_LOCK_WAIT_CAP_MILLIS));
        List<PartitionImpl> lockedPartitions = new ArrayList<>();
        List<Integer> lockTimedOut = new ArrayList<>();
        for (int pid : registry.activeIdsCopy()) {
            PartitionImpl partition = registry.get(pid);
            if (partition == null) {
                continue;
            }
            if (tryLockPartition(partition, lockWaitMillis)) {
                lockedPartitions.add(partition);
            } else {
                // 不静默：进 CloseReport。该分区可能有写线程还在临界区内，
                // 其 staging 区必须延迟释放（见 pipeline.close(Set) 的说明）。
                lockTimedOut.add(pid);
            }
        }
        try {
            pipeline.close(lockTimedOut.isEmpty()
                    ? java.util.Collections.emptySet() : new java.util.HashSet<>(lockTimedOut));
            try {
                long drainMillis = Math.max(1L, timeout.toMillis());
                coordinator.drainAllAndStop(drainMillis);
                for (int pid : registry.activeIdsCopy()) {
                    PartitionImpl partition = registry.get(pid);
                    if (partition == null) {
                        continue;
                    }
                    if (partition.publishedLsn() > partition.durableLsn()) {
                        unflushed.add(pid);
                        maxUnflushedLsn = Math.max(maxUnflushedLsn, partition.publishedLsn());
                        clean = false;
                    }
                    if (lockTimedOut.contains(pid)) {
                        // 写锁没拿到 → 可能仍有线程在往该分区写，不能对它做 force：
                        // 既不安全也大概率无效。计入 unflushed，如实报告。
                        unflushed.add(pid);
                        clean = false;
                        continue;
                    }
                    forceActiveSegment(pid);
                }
            } catch (RuntimeException e) {
                clean = false;
                detail = "关闭期异常: " + e;
            }

            if (!lockTimedOut.isEmpty()) {
                clean = false;
                String msg = "分区写锁获取超时（in-flight 写未排干，staging 区延迟到 GC 释放）: "
                        + lockTimedOut;
                detail = detail == null ? msg : detail + "; " + msg;
            }

            if (clean) {
                try {
                    writeMeta(true, lsnAllocator.peek(), registry.activeIdsCopy(), config);
                } catch (IOException e) {
                    clean = false;
                    detail = "写 wal.meta 失败: " + e.getMessage();
                }
            } else {
                // 不写 cleanShutdown=true；留下 false 让下次重启走完整 CRC 恢复
                try {
                    writeMeta(false, lsnAllocator.peek(), registry.activeIdsCopy(), config);
                } catch (IOException e) {
                    detail = (detail == null ? "" : detail + "; ")
                            + "写 wal.meta 失败: " + e.getMessage();
                }
            }
        } finally {
            for (int i = lockedPartitions.size() - 1; i >= 0; i--) {
                lockedPartitions.get(i).unlock();
            }
        }

        long elapsed = (System.nanoTime() - started) / 1_000_000L;
        boolean timedOut = elapsed > timeout.toMillis();
        closeQuietly(retentionManager);
        closeQuietly(checkpointManager);
        closeQuietly(segmentManager);
        closeQuietly(windowCache);
        closeQuietly(pipeline);
        closeQuietly(coordinator);
        closeQuietly(backend);
        closeQuietly(pluginManager);
        releaseLock(lock, lockChannel);

        if (timedOut && clean) {
            // 超时但最终刷完了：仍如实报告 clean=false 与耗时，避免掩盖"关得慢"
            return new CloseReport(false, unflushed, maxUnflushedLsn, elapsed,
                    "关闭耗时 " + elapsed + "ms 超过超时 " + timeout.toMillis() + "ms");
        }
        return new CloseReport(clean, unflushed, maxUnflushedLsn, elapsed, detail);
    }

    /**
     * <b>仅供测试</b>：模拟 {@code kill -9}。
     *
     * <p>与 {@link #close()} 的区别是刻意"什么都不做对"：不排空组提交、不 {@code force}、
     * 不写 {@code wal.meta(cleanShutdown=true)}，但会在同 JVM 内释放 {@code LOCK}
     * 以便立刻重开。用它来验证 P0-10 ① "close 后立即 kill -9 也不丢数据" 里
     * <b>更严格的那一半</b>——即"连 close 都没走到"的场景。
     */
    public void crashStopForTesting() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeQuietly(pipeline);
        closeQuietly(retentionManager);
        closeQuietly(checkpointManager);
        closeQuietly(segmentManager);
        closeQuietly(windowCache);
        closeQuietly(backend);
        closeQuietly(pluginManager);
        releaseLock(lock, lockChannel);
    }

    // ==================================================================
    // Wal：写入
    // ==================================================================

    @Override
    public long append(int partitionId, ByteBuffer payload, AppendOptions options) {
        checkOpen();
        return pipeline.append(partitionId, payload, options);
    }

    @Override
    public long appendSync(int partitionId, ByteBuffer payload, AppendOptions options) {
        checkOpen();
        return pipeline.appendSync(partitionId, payload, options);
    }

    @Override
    public long[] appendBatch(int partitionId, List<ByteBuffer> payloads) {
        checkOpen();
        return pipeline.appendBatch(partitionId, payloads);
    }

    @Override
    public boolean flushAndWait(int partitionId, long lsn, long timeoutMillis) {
        checkOpen();
        long started = metrics.enabled() ? System.nanoTime() : 0L;
        try {
            coordinator.awaitDurable(partitionId, lsn, timeoutMillis);
            return true;
        } catch (WalException.TimeoutException e) {
            return false;
        } finally {
            // 这里的埋点是"每次调用记一条"而不是抽样：flushAndWait 本身就是一次
            // 至少数百微秒的等待，一次 nanoTime + 一次桶自增（约 30ns）占比 < 0.01%。
            // 而且它的分布正是 P1-05 要观测的核心指标，抽样反而会丢掉尾部信息。
            if (started != 0L) {
                metrics.recordLatency(MetricsRegistry.FLUSH_LATENCY,
                        System.nanoTime() - started);
            }
        }
    }

    @Override
    public long durableLsn(int partitionId) {
        PartitionImpl partition = registry.get(partitionId);
        return partition == null ? 0L : partition.durableLsn();
    }

    @Override
    public long publishedLsn(int partitionId) {
        PartitionImpl partition = registry.get(partitionId);
        return partition == null ? 0L : partition.publishedLsn();
    }

    // ==================================================================
    // Wal：读取
    // ==================================================================

    @Override
    public WalCursor cursor(int partitionId, CursorOptions options) {
        checkOpen();
        Preconditions.checkNotNull(options, "options");
        if (registry.get(partitionId) == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        return new CursorImpl(partitionId, segmentManager, decoder, options,
                checkpointStore, config.mappedWindowCount() > 0 ? windowCache : null,
                retentionPin());
    }

    @Override
    public WalTailer tailer(TailerOptions options) {
        checkOpen();
        Preconditions.checkNotNull(options, "options");
        int partitionId = options.partitionId();
        if (registry.get(partitionId) == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        return new TailerImpl(partitionId, options, segmentManager, decoder, checkpointStore,
                config.mappedWindowCount() > 0 ? windowCache : null, registry, metrics, "wal",
                retentionPin());
    }

    /**
     * 把 {@link RetentionManager} 的显式 pin 能力暴露给读者（游标 / 订阅者）。
     *
     * <p>读者是唯一的"数据还在被使用"的权威来源：只有它知道自己读到了哪里、
     * 还需要后面的哪一段。把这件事写进保留策略的 {@code pins} 表（而不是依赖
     * "扫段序列找 readerCount"，后者在段被摘出序列时会退化为"无人读"），
     * 才能保证"pin 点之后一个段都不删"这条硬约束不被绕开。
     *
     * @return pin 出口；保留管理器不存在时返回 {@link RetentionPin#NONE}
     */
    private RetentionPin retentionPin() {
        final RetentionManager rm = this.retentionManager;
        if (rm == null) {
            return RetentionPin.NONE;
        }
        return new RetentionPin() {

            @Override
            public void pin(long lsn) {
                rm.pin(lsn);
            }

            @Override
            public void unpin(long lsn) {
                rm.unpin(lsn);
            }
        };
    }

    @Override
    public RecoveryReport recover(ReplayListener listener) {
        checkOpen();
        try {
            RecoveryManager recovery = new RecoveryManager(backend, registry, segmentManager,
                    indexStore, pluginManager.codec(), config, metrics);
            return recovery.recover(false, listener);
        } catch (IOException e) {
            throw new WalException.RecoveryFailedException("重放失败", e);
        }
    }

    // ==================================================================
    // Wal：分区管理
    // ==================================================================

    @Override
    public int addPartition(PartitionOptions options) {
        checkOpen();
        int[] active = registry.activeIdsCopy();
        for (int pid = 0; pid < config.maxPartitions(); pid++) {
            if (registry.get(pid) == null) {
                addPartition(pid, options);
                return pid;
            }
        }
        throw new WalException.PartitionException(
                "分区数已达上限 maxPartitions=" + config.maxPartitions()
                        + "（当前存活 " + active.length + " 个）");
    }

    @Override
    public void addPartition(int partitionId, PartitionOptions options) {
        checkOpen();
        Preconditions.checkNotNull(options, "options");
        if (partitionId < 0 || partitionId > SegmentNaming.MAX_PARTITION_ID) {
            throw new WalException.PartitionException(
                    "partitionId 越界（合法区间 [0, " + SegmentNaming.MAX_PARTITION_ID + "]）: "
                            + partitionId);
        }
        if (registry.get(partitionId) != null) {
            throw new WalException.PartitionException("分区已存在: " + partitionId);
        }
        if (purgedPartitionIds.contains(partitionId) && !options.allowIdReuse()) {
            throw new WalException.PartitionException(
                    "分区 " + partitionId + " 已被 PURGE，默认禁止复用该 ID"
                            + "（如需复用请显式传 PartitionOptions.allowingIdReuse(true)）");
        }
        purgedPartitionIds.remove(partitionId);
        try {
            PartitionImpl partition = new PartitionImpl(partitionId);
            registry.register(partition);
            Segment first = segmentManager.createFirstSegment(partitionId, lsnAllocator.peek());
            partition.setActiveSegment(first);
            segmentManager.warmUp(partitionId);
            writeMeta(false, lsnAllocator.peek(), registry.activeIdsCopy(), config);
        } catch (IOException e) {
            registry.unregister(partitionId);
            throw new WalException.IoException("新增分区失败: " + partitionId, e);
        }
    }

    @Override
    public void removePartition(int partitionId, RemoveMode mode) {
        checkOpen();
        Preconditions.checkNotNull(mode, "mode");
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        switch (mode) {
            case FREEZE:
                freezePartition(partitionId, partition, PartitionInfo.State.FROZEN);
                break;
            case ARCHIVE:
                freezePartition(partitionId, partition, PartitionInfo.State.ARCHIVED);
                // 归档 = 冻结 + 通知保留策略走归档流程（本地删除仍由 RetentionManager 执行）
                retentionManager.notifyArchive(partitionId);
                break;
            case PURGE:
                freezePartition(partitionId, partition, PartitionInfo.State.PURGED);
                segmentManager.deletePartitionData(partitionId);
                segmentManager.detachPartition(partitionId);
                registry.unregister(partitionId);
                PartitionStateFile.delete(config.baseDir(), partitionId);
                purgedPartitionIds.add(partitionId);
                break;
            default:
                throw new WalException.PartitionException("未支持的移除模式: " + mode);
        }
        try {
            writeMeta(false, lsnAllocator.peek(), registry.activeIdsCopy(), config);
        } catch (IOException e) {
            throw new WalException.IoException("更新 wal.meta 失败", e);
        }
    }

    private void freezePartition(int partitionId, PartitionImpl partition,
                                 PartitionInfo.State target) {
        // ---- F2：先拿分区写锁，再封存段 ----
        //
        // 不持锁封存时，一个已通过 isWritable 预检、正在临界区内写帧的写线程
        // （LSN 已分配、帧已编码到一半）会与 seal 竞争：封存把段标记为只读并
        // 记下 finalLength，写线程随后提交写指针，就出现"写指针越过 finalLength"
        // 的封存段——恢复期按 finalLength 截断会丢掉这批已发布给调用方的 LSN。
        // 拿到写锁即证明该分区没有 in-flight 写；此后新进入的写线程会在
        // AppendPipeline 的锁内复核（isWritable）处被拒绝。
        //
        // 锁序：只拿分区写锁，段封存在锁内进行（与写入路径一致，不反向）。
        partition.lock();
        try {
            try {
                for (Segment segment : segmentManager.segments(partitionId)) {
                    if (segment.isActive()) {
                        segment.sealToReadOnly(segment.publishedWritePos());
                    }
                }
            } catch (IOException e) {
                throw new WalException.IoException("冻结分区时封存段失败: " + partitionId, e);
            }
            partition.transitionTo(target);
            // 清空活跃段引用：封存后的段不再接收写入，若还挂着引用，组提交线程会在
            // force 一个已封存段上白花一次系统调用（Windows 上尤其明显）。
            partition.setActiveSegment(null);
        } finally {
            partition.unlock();
        }
        // ---- 锁外部分：与写路径不再共享可变状态 ----
        // freeze(索引/后端元数据)、分区状态文件、PURGE 的目录删除都只影响
        // "重启恢复后能看到什么"，读路径有 readerCount/pin 双保护，无需写锁。
        if (target != PartitionInfo.State.FROZEN) {
            segmentManager.freeze(partitionId);
        }
        // 把"已下线"落到磁盘：否则重启后恢复流程只看到段文件，会把它当可写分区。
        // PURGE 不写状态文件——整个分区目录马上会被删掉，写进去等于白写。
        if (target != PartitionInfo.State.PURGED) {
            try {
                PartitionStateFile.write(config.baseDir(), partitionId, target);
            } catch (IOException e) {
                throw new WalException.IoException("写分区状态文件失败: " + partitionId, e);
            }
        }
    }

    @Override
    public void rotate(int partitionId) {
        checkOpen();
        try {
            Segment rotated = segmentManager.rotate(partitionId);
            PartitionImpl partition = registry.get(partitionId);
            if (partition != null) {
                partition.setActiveSegment(rotated);
            }
        } catch (IOException e) {
            throw new WalException.IoException("轮转失败: " + partitionId, e);
        }
    }

    @Override
    public PartitionInfo partitionInfo(int partitionId) {
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            return null;
        }
        List<Segment> segments = segmentManager.segments(partitionId);
        long totalBytes = 0L;
        long firstLsn = partition.firstLsnOfPartition();
        for (Segment segment : segments) {
            totalBytes += segment.capacityBytes();
            if (firstLsn <= 0L) {
                firstLsn = segment.firstLsn();
            }
        }
        return new PartitionInfo(partitionId, firstLsn, partition.publishedLsn(),
                partition.durableLsn(), segments.size(), totalBytes, partition.state());
    }

    // ==================================================================
    // Wal：观测与扩展
    // ==================================================================

    @Override
    public WalStats stats() {
        long totalBytes = 0L;
        long totalRecords = 0L;
        long segmentCount = 0L;
        long publishedMax = 0L;
        long durableMax = 0L;
        int activePartitions = 0;
        for (int pid : registry.activeIdsCopy()) {
            activePartitions++;
            for (Segment segment : segmentManager.segments(pid)) {
                segmentCount++;
                // 口径必须是"真实数据字节数"，而不是段的配置容量：
                // 活跃段的 capacity 是 segmentSizeBytes（4MB~256MB），已写可能只有几百字节，
                // 用 capacity 会让 wal.log.bytes 虚高 1~3 个数量级，也会与
                // RetentionManager 的 sizeBytes（活跃段=publishedWritePos、封存段=finalLength）
                // 不一致，进而让"按大小保留"与"看板日志总量"两个数字对不上。
                totalBytes += dataLengthOf(segment);
                totalRecords += segment.recordCount();
            }
            PartitionImpl partition = registry.get(pid);
            if (partition != null) {
                publishedMax = Math.max(publishedMax, partition.publishedLsn());
                durableMax = Math.max(durableMax, partition.durableLsn());
            }
        }
        GroupCommitCoordinator.FlushStats flushStats = coordinator.stats();
        long indexHeapBytes = indexStore == null ? 0L : indexStore.memoryBytes();
        long offHeapBytes = windowCache == null ? 0L : windowCache.mappedBytes();
        Map<String, String> active = pluginManager.current().activeIds();
        // ---- gauge：瞬时量必须在取快照时装进注册中心，否则外接 sink 拿不到 ----
        // （counter 只增不减，采样线程取差分算速率；gauge 是"最后一次观测到的值"）
        Health health = coordinator.health();
        metrics.setGauge(MetricsRegistry.HEALTH, health.ordinal());
        metrics.setGauge(MetricsRegistry.LOG_BYTES, totalBytes);
        metrics.setGauge(MetricsRegistry.PARTITIONS, activePartitions);
        metrics.setGauge(MetricsRegistry.SEGMENTS, segmentCount);
        // ---- 延迟分位：把内部直方图搬成 api 包的只读视图（不泄漏内部类型） ----
        Map<String, WalStats.LatencyView> latencies = new LinkedHashMap<>();
        for (Map.Entry<String, LatencyHistogram> e : metrics.histogramSnapshot().entrySet()) {
            LatencyHistogram h = e.getValue();
            latencies.put(e.getKey(), new WalStats.LatencyView(e.getKey(), h.count(),
                    h.p50(), h.p99(), h.p999()));
        }
        return new WalStats(health, totalBytes, totalRecords, publishedMax,
                durableMax, offHeapBytes, indexHeapBytes,
                metrics.counter(MetricsRegistry.CORRUPTION_COUNT).sum(),
                segmentManager.fallbackCount(), active, activePartitions,
                segmentCount, segmentManager.rotationCount(),
                flushStats.batches(), flushStats.forcedPartitions(), flushStats.failedBatches(),
                coordinator.lastFailure(),
                metrics.counterSnapshot(), latencies, metrics.gaugeSnapshot());
    }

    /**
     * 计算一个段<b>已写入的数据字节数</b>（不是段的配置容量）。
     *
     * <p>规则与 {@code RetentionManager#toInfo(Segment)} 完全一致，保证
     * {@code wal.stats().totalLogBytes()} 与"按大小保留"的判定依据是同一个数字：
     * <ul>
     *   <li>活跃段：{@code publishedWritePos()}（写指针已提交位置）；</li>
     *   <li>封存段：{@code finalLength()}，取不到（{@code < 0}）时退回 {@code publishedWritePos()}。</li>
     * </ul>
     *
     * @param segment 段
     * @return 数据字节数
     */
    private static long dataLengthOf(Segment segment) {
        if (segment.isActive()) {
            return Math.max(0L, segment.publishedWritePos());
        }
        long sealed = segment.finalLength();
        return Math.max(0L, sealed >= 0L ? sealed : segment.publishedWritePos());
    }

    @Override
    public PluginManager pluginManager() {
        return pluginManager;
    }

    @Override
    public String codecId() {
        return pluginManager.codec() == null ? "<none>" : pluginManager.codec().id();
    }

    // ==================================================================
    // 内部
    // ==================================================================

    private void checkOpen() {
        if (closed.get()) {
            throw new WalException.ClosedException(
                    "WAL 已关闭，拒绝操作（baseDir=" + baseDir + "）");
        }
    }

    private void forceActiveSegment(int partitionId) {
        Segment active = segmentManager.activeSegment(partitionId);
        if (active == null) {
            return;
        }
        long started = metrics.enabled() ? System.nanoTime() : 0L;
        try {
            active.force(true);
            // ---- R4：失败计数语义修正 ----
            // 原代码把 FSYNC_COUNT 记在 IOException 分支里——失败的 force 被统计成
            // "一次成功的 fsync"，而真正成功的 force 反而不计数（组提交路径的
            // FSYNC_COUNT/FSYNC_LATENCY 在 GroupCommitCoordinator 里，close 路径
            // 与它完全脱节）。修正为：成功记 count + latency，失败记 failure。
            if (started != 0L) {
                metrics.recordLatency(MetricsRegistry.FSYNC_LATENCY,
                        System.nanoTime() - started);
                metrics.increment(MetricsRegistry.FSYNC_COUNT);
            }
            PartitionImpl partition = registry.get(partitionId);
            if (partition != null && active.lastLsn() > 0L) {
                partition.publishDurableLsn(active.lastLsn());
            }
        } catch (IOException e) {
            // close 不因单个分区 force 失败而中断（其他分区还要继续关）；
            // wal.meta 会写 cleanShutdown=false，让下次重启走完整 CRC 恢复。
            if (metrics != null) {
                metrics.increment(MetricsRegistry.FSYNC_FAILURE);
            }
        }
    }

    private static void writeMeta(boolean cleanShutdown, long nextLsn, int[] partitionIds,
                                  WalConfig config) throws IOException {
        String ids = joinIds(partitionIds);
        Map<String, String> values = MetaFile.newEntries(WalVersion.FORMAT_VERSION,
                System.currentTimeMillis(), nextLsn, cleanShutdown, ids);
        values.put(MetaFile.KEY_PARTITION_COUNT, String.valueOf(config.partitionCount()));
        MetaFile.write(config.baseDir(), values);
    }

    private static String joinIds(int[] partitionIds) {
        if (partitionIds == null || partitionIds.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(partitionIds.length * 4);
        for (int i = 0; i < partitionIds.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(partitionIds[i]);
        }
        return sb.toString();
    }

    private static void releaseLock(FileLock lock, FileChannel channel) {
        if (lock != null && lock.isValid()) {
            try {
                lock.release();
            } catch (IOException ignored) {
                // 释放失败会在 channel.close() 时被系统回收
            }
        }
        closeQuietly(channel);
    }

    /**
     * 尝试在限期内拿到一个分区的写锁（自旋 + park 混合）。
     *
     * <p>不用 {@code lock()} 是因为 close 不能被一个失控的写线程无限期挂住；
     * 不用单次 {@code tryLock()} 是因为写临界区虽然短，但碰上线程被换出时
     * 一次 tryLock 会有可观的假阴性率。50µs 的 park 粒度足够细，且在
     * 5 秒上限内最多自旋 10 万次，CPU 占用可以忽略。
     *
     * @param partition 目标分区
     * @param waitMillis 等待上限（毫秒），至少 1
     * @return true = 已拿到锁（调用方负责 unlock）；false = 超时未拿到
     */
    private static boolean tryLockPartition(PartitionImpl partition, long waitMillis) {
        long deadline = System.nanoTime() + Math.max(1L, waitMillis) * 1_000_000L;
        while (true) {
            if (partition.tryLock()) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            LockSupport.parkNanos(50_000L);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // 关闭失败不影响主流程；真正的错误已在各自路径上抛出过
        }
    }

    /**
     * 健康度（= 组提交协调器的健康度）。
     *
     * @return 健康度
     */
    public Health health() {
        return coordinator.health();
    }

    /**
     * 写入管线（诊断/测试用）。
     *
     * @return 管线
     */
    public AppendPipeline pipeline() {
        return pipeline;
    }

    /**
     * 段管理器（诊断/测试用）。
     *
     * @return 段管理器
     */
    public SegmentManager segmentManager() {
        return segmentManager;
    }

    /**
     * 快照 / checkpoint 编排器（运维/测试用）。
     *
     * @return 编排器
     */
    public CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * 指标注册中心（运维/测试用：可在此挂 {@code MeterSink} 把指标桥接出去）。
     *
     * @return 注册中心，永不为 null
     */
    public MetricsRegistry metrics() {
        return metrics;
    }

    @Override
    public com.walcore.spi.SnapshotStore snapshotStore() {
        return checkpointManager == null ? null : checkpointManager.store();
    }

    /**
     * 保留策略管理器（运维/测试用）。
     *
     * @return 管理器
     */
    public RetentionManager retentionManager() {
        return retentionManager;
    }

    /**
     * 分区注册表（诊断/测试用）。
     *
     * @return 注册表
     */
    public PartitionRegistry registry() {
        return registry;
    }

    /**
     * 帧编码器（诊断/测试用）。
     *
     * @return 编码器
     */
    public FrameEncoder encoder() {
        return encoder;
    }

    /**
     * 帧解码器（诊断/测试用）。
     *
     * @return 解码器
     */
    public FrameDecoder decoder() {
        return decoder;
    }

    /**
     * 组提交协调器（诊断/测试用）。
     *
     * @return 协调器
     */
    public GroupCommitCoordinator coordinator() {
        return coordinator;
    }

    @Override
    public String toString() {
        return "DefaultWal{baseDir=" + baseDir
                + ", partitions=" + registry.activeCount()
                + ", closed=" + closed.get()
                + ", health=" + coordinator.health() + '}';
    }

    /**
     * 组装用的可变中转对象（只在本类内部使用，避免一个 20 参数的构造器）。
     */
    private static final class Builder {
        private WalConfig config;
        private Path baseDir;
        private MetricsRegistry metrics;
        private PluginManager pluginManager;
        private StorageBackend backend;
        private PartitionRegistry registry;
        private LsnAllocator lsnAllocator;
        private IndexStore indexStore;
        private SegmentManager segmentManager;
        private DirtyTracker dirtyTracker;
        private GroupCommitCoordinator coordinator;
        private AppendPipeline pipeline;
        private FrameEncoder encoder;
        private FrameDecoder decoder;
        private MappedWindowCache windowCache;
        private CursorCheckpointStore checkpointStore;
        private RetentionManager retentionManager;
        private CheckpointManager checkpointManager;
        private FileChannel lockChannel;
        private FileLock lock;
        private RecoveryReport openReport;
    }

    /**
     * 有效的分区列表（升序，诊断用）。
     *
     * @return 不可变列表
     */
    public List<Integer> partitionIds() {
        int[] ids = registry.activeIdsCopy();
        List<Integer> out = new ArrayList<>(ids.length);
        for (int id : ids) {
            out.add(id);
        }
        return out;
    }

    /**
     * 全部分区的 durable 水位（诊断用）。
     *
     * @return 分区 ID -&gt; durableLsn（升序）
     */
    public Map<Integer, Long> durableWatermarks() {
        Map<Integer, Long> out = new LinkedHashMap<>();
        for (int pid : registry.activeIdsCopy()) {
            out.put(pid, durableLsn(pid));
        }
        return out;
    }
}
