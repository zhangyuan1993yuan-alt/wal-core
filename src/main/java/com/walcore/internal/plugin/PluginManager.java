package com.walcore.internal.plugin;

import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.api.WalVersion;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.spiimpl.DefaultIndexStore;
import com.walcore.internal.spiimpl.RawRecordCodec;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.Priority;
import com.walcore.spi.RecordCodec;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.WalPlugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * 插件发现 / 优先级仲裁 / 类加载隔离 / 热切换 / 诊断（ARCH §10.3、§10.4）。
 *
 * <h2>加载流程（{@link #scan()}）</h2>
 * <ol>
 *   <li>收集候选 ClassLoader（有序）：用户指定 → {@code baseDir/plugin/*.jar} →
 *       {@code wal-core} 自身 → 线程上下文；</li>
 *   <li>对每类 SPI 用 {@code ServiceLoader.load(iface, cl)} 累积候选；</li>
 *   <li>按 {@code id()} 去重，<b>首个胜出</b>并记 {@code duplicateIds}；</li>
 *   <li>剔除 {@code apiVersion != WalVersion.API_VERSION} 的候选；</li>
 *   <li>排序：配置显式指定 {@code wal.spi.<type>=<id>} 时只考虑该 id；
 *       否则 {@code @Priority} 降序、同分按 {@code id()} 字典序升序（保证可复现）；</li>
 *   <li>实例化 + {@code init(context)}；抛异常者剔除，<b>不污染</b>已成功者；</li>
 *   <li>某类型无可用实现时：{@code FALLBACK_DEFAULT} 用内置默认并记指标；
 *       {@code FAIL_FAST} 抛 {@link WalException.PluginLoadException}，消息含
 *       接口全名 / 已尝试 ClassLoader 列表 / 期望的 provider 文件名 / 已发现的 id 列表。</li>
 * </ol>
 *
 * <h2>线程安全</h2>
 * {@link #scan()} / {@link #installJar} / {@link #uninstallJar} / {@link #swap} 互斥
 * （{@code synchronized} 在同一把对象锁上），但它们与热路径上的读取<b>不共享锁</b>：
 * 热路径只读 {@code AtomicReference}，因此切换是 O(1) 且不阻塞写入。
 */
public final class PluginManager implements AutoCloseable {

    /** 回退内置实现时的指标名。 */
    public static final String METRIC_FALLBACK = "wal.plugin.fallback";

    /** 加载失败时的指标名。 */
    public static final String METRIC_LOAD_FAILED = "wal.plugin.load.failed";

    private static final String NONE_DESCRIPTION = "<无>";

    private final WalConfig config;
    private final Path baseDir;
    private final Path pluginDir;
    private final MetricsRegistry metrics;
    private final ClassLoader frameClassLoader;

    /** 已安装的插件 JAR：规范化路径 → 句柄。 */
    private final Map<Path, PluginType.PluginHandle> installedJars = new ConcurrentHashMap<>();

    private final Map<PluginType, List<PluginType.PluginInfo>> discovered =
            new LinkedHashMap<>();

    /** 当前生效的插件引用（热路径只读这些）。 */
    private final AtomicReference<RecordCodec> codecRef = new AtomicReference<>();
    private final AtomicReference<IndexStore> indexRef = new AtomicReference<>();
    private final AtomicReference<String> storageIdRef = new AtomicReference<>();

    private final List<WalPlugin> ownedInstances = new ArrayList<>();

    private volatile PluginType.PluginSet current =
            new PluginType.PluginSet(null, null, null, Map.of());
    private volatile PluginDiagnostics diagnostics = PluginDiagnostics.empty();
    private volatile boolean scanned;
    private volatile com.walcore.spi.PluginContext context;
    private volatile boolean closed;

    /**
     * 构造插件管理器。
     *
     * @param config 配置
     * @param baseDir 日志根目录
     * @param metrics 指标注册中心，可为 null
     */
    public PluginManager(WalConfig config, Path baseDir, MetricsRegistry metrics) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
        this.metrics = metrics;
        this.pluginDir = config.pluginDir() != null ? config.pluginDir() : baseDir.resolve("plugin");
        this.frameClassLoader = PluginManager.class.getClassLoader();
    }

    // ==================================================================
    // 发现与加载
    // ==================================================================

    /**
     * 扫描插件并产出诊断报告。运行期可重复调用（幂等，会重新仲裁）。
     *
     * @return 诊断报告
     */
    public synchronized PluginDiagnostics scan() {
        checkOpen();
        PluginDiagnostics.Builder diag = PluginDiagnostics.builder();
        List<SourceRef> sources = collectSources(diag);
        discovered.clear();
        ownedInstances.clear();

        RecordCodec codec = null;
        IndexStore indexStore = null;
        String storageId = null;
        com.walcore.spi.RetentionPolicy retentionPolicy = null;
        com.walcore.spi.ReplicationSink replicationSink = null;
        Map<String, String> active = new LinkedHashMap<>();

        for (PluginType type : PluginType.values()) {
            Class<? extends WalPlugin> iface = type.iface();
            if (iface == null) {
                // RETENTION / REPLICATION 的接口由 T14 交付，此处静默跳过
                continue;
            }
            List<Candidate> candidates = discover(type, iface, sources, diag);
            discovered.put(type, toInfos(type, candidates,
                    type == PluginType.CODEC ? idOf(codecRef.get())
                            : type == PluginType.INDEX ? idOf(indexRef.get()) : storageId));

            Candidate chosen = select(type, candidates, diag);
            if (chosen == null) {
                String fallback = applyFallback(type, diag, sources,
                        mustReportMissing(type, candidates.size()));
                if (fallback == null) {
                    // applyFallback 内部已抛异常（FAIL_FAST）或返回内置 id
                    continue;
                }
                switch (type) {
                    case CODEC:
                        codec = new RawRecordCodec();
                        codec.init(pluginContext());
                        codecRef.set(codec);
                        break;
                    case INDEX:
                        indexStore = new DefaultIndexStore(baseDir, config.maxIndexHeapBytes());
                        indexStore.init(pluginContext());
                        indexRef.set(indexStore);
                        break;
                    case RETENTION: {
                        com.walcore.internal.spiimpl.TimeSizeRetentionPolicy builtinPolicy =
                                new com.walcore.internal.spiimpl.TimeSizeRetentionPolicy();
                        builtinPolicy.init(pluginContext());
                        retentionPolicy = builtinPolicy;
                        break;
                    }
                    case REPLICATION: {
                        com.walcore.internal.spiimpl.NoopReplicationSink builtinSink =
                                new com.walcore.internal.spiimpl.NoopReplicationSink();
                        builtinSink.init(pluginContext());
                        replicationSink = builtinSink;
                        break;
                    }
                    case STORAGE:
                    default:
                        storageId = fallback;
                        storageIdRef.set(storageId);
                }
                active.put(type.shortName(), fallback);
                continue;
            }

            WalPlugin instance = instantiate(type, chosen, diag);
            if (instance == null) {
                // 候选存在却全都没法用（init 抛异常）→ 这是一次"本该用插件却没用上"的
                // 静默降级，FAIL_FAST 下必须失败
                String fallback = applyFallback(type, diag, sources, true);
                if (fallback == null) {
                    continue;
                }
                switch (type) {
                    case CODEC:
                        codec = new RawRecordCodec();
                        codec.init(pluginContext());
                        codecRef.set(codec);
                        break;
                    case INDEX:
                        indexStore = new DefaultIndexStore(baseDir, config.maxIndexHeapBytes());
                        indexStore.init(pluginContext());
                        indexRef.set(indexStore);
                        break;
                    case RETENTION: {
                        com.walcore.internal.spiimpl.TimeSizeRetentionPolicy builtinPolicy =
                                new com.walcore.internal.spiimpl.TimeSizeRetentionPolicy();
                        builtinPolicy.init(pluginContext());
                        retentionPolicy = builtinPolicy;
                        break;
                    }
                    case REPLICATION: {
                        com.walcore.internal.spiimpl.NoopReplicationSink builtinSink =
                                new com.walcore.internal.spiimpl.NoopReplicationSink();
                        builtinSink.init(pluginContext());
                        replicationSink = builtinSink;
                        break;
                    }
                    case STORAGE:
                    default:
                        storageId = fallback;
                        storageIdRef.set(storageId);
                }
                active.put(type.shortName(), fallback);
                continue;
            }

            ownedInstances.add(instance);
            diag.loaded(type.shortName() + ':' + chosen.id);
            active.put(type.shortName(), chosen.id);
            if (type == PluginType.CODEC) {
                codec = (RecordCodec) instance;
                codecRef.set(codec);
            } else if (type == PluginType.INDEX) {
                indexStore = (IndexStore) instance;
                indexRef.set(indexStore);
            } else if (type == PluginType.RETENTION) {
                retentionPolicy = (com.walcore.spi.RetentionPolicy) instance;
            } else if (type == PluginType.REPLICATION) {
                replicationSink = (com.walcore.spi.ReplicationSink) instance;
            } else {
                storageId = chosen.id;
                storageIdRef.set(storageId);
            }
        }

        // 刷新 active 标记
        for (PluginType type : PluginType.values()) {
            if (discovered.containsKey(type)) {
                String activeId = active.get(type.shortName());
                List<PluginType.PluginInfo> infos = new ArrayList<>();
                for (PluginType.PluginInfo info : discovered.get(type)) {
                    infos.add(info.withActive(info.id().equals(activeId)));
                }
                discovered.put(type, List.copyOf(infos));
            }
        }

        for (Map.Entry<String, String> e : active.entrySet()) {
            diag.active(e.getKey(), e.getValue());
        }
        this.current = new PluginType.PluginSet(codec, indexStore, storageId,
                retentionPolicy, replicationSink, active);
        this.diagnostics = diag.build();
        this.scanned = true;
        return this.diagnostics;
    }

    /**
     * 列出某类 SPI 的候选实现（含是否当前生效）。
     *
     * @param type 类型
     * @return 不可变列表
     */
    public synchronized List<PluginType.PluginInfo> list(PluginType type) {
        Preconditions.checkNotNull(type, "type");
        List<PluginType.PluginInfo> infos = discovered.get(type);
        return infos == null ? List.of() : infos;
    }

    /**
     * 当前生效的插件集合。
     *
     * @return 集合
     */
    public PluginType.PluginSet current() {
        return current;
    }

    /**
     * 最近一次扫描的诊断报告。
     *
     * @return 报告
     */
    public PluginDiagnostics diagnostics() {
        return diagnostics;
    }

    /**
     * 生效的编解码器。
     *
     * @return 编解码器；尚未 scan 返回 null
     */
    public RecordCodec codec() {
        return codecRef.get();
    }

    /**
     * 生效的索引实现。
     *
     * @return 索引实现；尚未 scan 返回 null
     */
    public IndexStore indexStore() {
        return indexRef.get();
    }

    /**
     * 生效的存储后端 id。
     *
     * @return 插件 id；尚未 scan 返回 null
     */
    public String storageBackendId() {
        return storageIdRef.get();
    }

    /**
     * 生效的保留策略（T14）。
     *
     * @return 策略；尚未 scan 返回 null
     */
    public com.walcore.spi.RetentionPolicy retentionPolicy() {
        return current.retentionPolicy();
    }

    /**
     * 生效的复制实现（T14）。
     *
     * @return 实现；尚未 scan 返回 null
     */
    public com.walcore.spi.ReplicationSink replicationSink() {
        return current.replicationSink();
    }

    /**
     * 插件可见的上下文（内置实现初始化与 {@code DefaultWal} 装配共用同一份）。
     *
     * @return 上下文
     */
    public com.walcore.spi.PluginContext pluginContext() {
        com.walcore.spi.PluginContext ctx = this.context;
        if (ctx != null) {
            return ctx;
        }
        synchronized (this) {
            if (this.context == null) {
                this.context = buildContext();
            }
            return this.context;
        }
    }

    /**
     * 构造上下文（只做一次）。
     *
     * @return 上下文
     */
    private com.walcore.spi.PluginContext buildContext() {
        final PluginManager self = this;
        return new com.walcore.spi.PluginContext() {
            @Override
            public WalConfig config() {
                return self.config;
            }

            @Override
            public int partitionCount() {
                return self.config.partitionCount();
            }

            @Override
            public MetricsRegistry metrics() {
                return self.metrics;
            }

            @Override
            public ClassLoader pluginClassLoader() {
                return self.frameClassLoader;
            }

            @Override
            public java.nio.file.Path baseDir() {
                return self.baseDir;
            }

            @Override
            public void warn(String code, String message, Throwable cause) {
                if (self.metrics != null) {
                    self.metrics.increment("wal.plugin.warn." + code);
                }
            }

            @Override
            public String toString() {
                return "PluginContext{" + self.baseDir + '}';
            }
        };
    }

    // ==================================================================
    // 运行期安装 / 卸载 / 热切换
    // ==================================================================

    /**
     * 运行期加载一个插件 JAR。
     *
     * <p>同一 JAR 多次加载返回同一个句柄（按规范化路径去重）。
     *
     * @param jarPath JAR 路径
     * @return 句柄
     * @throws PluginException JAR 不存在、不可读、或内含 {@code com/walcore/} 核心类
     */
    public synchronized PluginType.PluginHandle installJar(Path jarPath) throws PluginException {
        checkOpen();
        Preconditions.checkNotNull(jarPath, "jarPath");
        Path normalized;
        try {
            normalized = jarPath.toRealPath();
        } catch (IOException e) {
            throw new PluginException("插件 JAR 不存在或不可读: " + jarPath, e);
        }
        PluginType.PluginHandle existing = installedJars.get(normalized);
        if (existing != null) {
            return existing;
        }
        try {
            String offending = PluginClassLoader.findCoreClassEntry(normalized);
            if (offending != null) {
                increment(METRIC_LOAD_FAILED);
                throw new PluginException(
                        PluginClassLoader.rejectedJarReason(normalized, offending), null, null);
            }
        } catch (IOException e) {
            throw new PluginException("无法读取插件 JAR: " + normalized, e);
        }

        URL url;
        try {
            url = normalized.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new PluginException("插件 JAR 路径无法转换为 URL: " + normalized, e);
        }
        PluginClassLoader loader = new PluginClassLoader(
                new URL[]{url}, frameClassLoader, String.valueOf(normalized.getFileName()), normalized);

        List<String> ids = new ArrayList<>();
        for (PluginType type : PluginType.values()) {
            Class<? extends WalPlugin> iface = type.iface();
            if (iface == null) {
                continue;
            }
            ServiceLoader<? extends WalPlugin> sl = ServiceLoader.load(iface, loader);
            for (WalPlugin plugin : sl) {
                ids.add(type.shortName() + ':' + safeId(plugin));
            }
        }
        PluginType.PluginHandle handle = new PluginType.PluginHandle(normalized, loader, ids);
        installedJars.put(normalized, handle);
        return handle;
    }

    /**
     * 卸载一个插件 JAR。该 JAR 提供的插件若仍在生效则拒绝卸载。
     *
     * @param jarPath JAR 路径
     * @throws PluginException 仍在生效或从未安装
     */
    public synchronized void uninstallJar(Path jarPath) throws PluginException {
        checkOpen();
        Preconditions.checkNotNull(jarPath, "jarPath");
        final Path normalized;
        try {
            normalized = jarPath.toRealPath();
        } catch (IOException e) {
            throw new PluginException("插件 JAR 不存在: " + jarPath, e);
        }
        PluginType.PluginHandle handle = installedJars.get(normalized);
        if (handle == null) {
            throw new PluginException("插件 JAR 未安装: " + normalized);
        }
        // 若该 JAR 提供的任一插件仍处于生效状态，拒绝卸载
        for (Map.Entry<String, String> e : current.activeIds().entrySet()) {
            String usedId = e.getValue();
            for (String declared : handle.pluginIds()) {
                if (declared.endsWith(":" + usedId)) {
                    throw new PluginException(
                            "插件 JAR 仍在使用中，请先 swap 走: type=" + e.getKey()
                                    + ", id=" + usedId, normalized.toString(), null);
                }
            }
        }
        installedJars.remove(normalized);
        closeQuietly(handle.classLoader());
    }

    /**
     * 热切换当前生效实现。
     *
     * <p>失败语义（P0-09 ③ 的核心保证）：新实例 {@code init()} 抛异常时
     * <b>原子回滚</b>到旧实例，{@code SwapResult.rolledBack()==true}，服务不中断。
     *
     * @param type 类型
     * @param newPluginId 新插件 id
     * @param policy 旧实例关闭策略
     * @param <T> 插件类型
     * @return 切换结果
     */
    @SuppressWarnings("unchecked")
    public synchronized <T extends WalPlugin> PluginType.SwapResult<T> swap(
            PluginType type, String newPluginId, PluginType.SwapPolicy policy) {
        checkOpen();
        Preconditions.checkNotNull(type, "type");
        Preconditions.checkNotNull(newPluginId, "newPluginId");
        PluginType.SwapPolicy effectivePolicy =
                policy == null ? PluginType.SwapPolicy.DRAIN_THEN_CLOSE : policy;

        WalPlugin previous;
        AtomicReference<? extends WalPlugin> slot;
        switch (type) {
            case CODEC:
                previous = codecRef.get();
                slot = codecRef;
                break;
            case INDEX:
                previous = indexRef.get();
                slot = indexRef;
                break;
            default:
                return PluginType.SwapResult.failed(null,
                        "类型 " + type.shortName() + " 的热切换在 T14 交付（当前仅支持 codec/index）");
        }

        Candidate candidate = findCandidate(type, newPluginId);
        if (candidate == null) {
            return PluginType.SwapResult.failed((T) previous,
                    "未找到插件 id=" + newPluginId + " 的候选实现（type=" + type.shortName()
                            + ", 已发现=" + knownIds(type) + "）");
        }
        PluginDiagnostics.Builder diag = PluginDiagnostics.builder();
        WalPlugin fresh = instantiate(type, candidate, diag);
        if (fresh == null) {
            return PluginType.SwapResult.failed((T) previous,
                    "新实例初始化失败，已自动回滚: " + newPluginId);
        }
        ((AtomicReference<WalPlugin>) slot).set(fresh);
        Map<String, String> active = new LinkedHashMap<>(current.activeIds());
        active.put(type.shortName(), newPluginId);
        this.current = new PluginType.PluginSet(
                type == PluginType.CODEC ? (RecordCodec) fresh : current.codec(),
                type == PluginType.INDEX ? (IndexStore) fresh : current.indexStore(),
                current.storageBackendId(), active);

        // 旧实例关闭策略
        if (previous == null) {
            return PluginType.SwapResult.ok(null, (T) fresh);
        }
        if (effectivePolicy == PluginType.SwapPolicy.IMMEDIATE) {
            closeQuietly(previous);
        } else {
            // DRAIN_THEN_CLOSE / SHADOW_THEN_SWAP：引用已经切换，旧实例不再被新调用触达；
            // 本框架的 SPI 方法都是无持有状态的短调用，因此这里在切换后立即关闭，
            // 与"引用计数归零后关闭"等价（不存在跨调用持有的引用）。
            closeQuietly(previous);
        }
        return PluginType.SwapResult.ok((T) previous, (T) fresh);
    }

    /**
     * 关闭插件管理器：关闭全部由本管理器创建的插件实例与 JAR 类加载器。
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (WalPlugin plugin : ownedInstances) {
            closeQuietly(plugin);
        }
        ownedInstances.clear();
        for (PluginType.PluginHandle handle : installedJars.values()) {
            closeQuietly(handle.classLoader());
        }
        installedJars.clear();
        discovered.clear();
    }

    // ==================================================================
    // 内部实现
    // ==================================================================

    private void checkOpen() {
        if (closed) {
            throw new WalException.ClosedException("PluginManager 已关闭");
        }
    }

    private void increment(String name) {
        if (metrics != null) {
            metrics.increment(name);
        }
    }

    /** 一个候选来源。 */
    private static final class SourceRef {
        private final ClassLoader loader;
        private final String description;

        SourceRef(ClassLoader loader, String description) {
            this.loader = loader;
            this.description = description;
        }
    }

    /** 一个候选项。 */
    private static final class Candidate {
        private final WalPlugin plugin;
        private final String id;
        private final int priority;
        private final int apiVersion;
        private final String source;

        Candidate(WalPlugin plugin, String id, int priority, int apiVersion, String source) {
            this.plugin = plugin;
            this.id = id;
            this.priority = priority;
            this.apiVersion = apiVersion;
            this.source = source;
        }

        String className() {
            return plugin.getClass().getName();
        }
    }

    private List<SourceRef> collectSources(PluginDiagnostics.Builder diag) {
        List<SourceRef> sources = new ArrayList<>(4);
        ClassLoader user = config.pluginClassLoader();
        if (user != null) {
            sources.add(new SourceRef(user, "config.pluginClassLoader"));
            diag.source("config.pluginClassLoader");
        }
        scanPluginDir(diag, sources);
        if (frameClassLoader != null) {
            sources.add(new SourceRef(frameClassLoader, "wal-core"));
            diag.source("wal-core");
        }
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl != null && tccl != frameClassLoader) {
            sources.add(new SourceRef(tccl, "thread-context"));
            diag.source("thread-context");
        }
        return sources;
    }

    private void scanPluginDir(PluginDiagnostics.Builder diag, List<SourceRef> sources) {
        if (pluginDir == null || !Files.isDirectory(pluginDir)) {
            return;
        }
        List<Path> jars = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginDir, "*.jar")) {
            for (Path p : stream) {
                jars.add(p);
            }
        } catch (IOException e) {
            diag.failed("扫描插件目录失败: " + pluginDir + ", " + e);
            return;
        }
        jars.sort(Comparator.comparing(Path::toString));
        for (Path jar : jars) {
            try {
                PluginType.PluginHandle handle = installedJars.get(jar.toRealPath());
                if (handle == null) {
                    handle = installJarQuietly(jar, diag);
                }
                if (handle != null) {
                    sources.add(new SourceRef(handle.classLoader(), "jar:" + jar.getFileName()));
                    diag.source("jar:" + jar.getFileName());
                }
            } catch (IOException e) {
                diag.failed("插件 JAR 路径不可解析: " + jar);
            }
        }
    }

    private PluginType.PluginHandle installJarQuietly(Path jar, PluginDiagnostics.Builder diag) {
        try {
            return installJar(jar);
        } catch (PluginException e) {
            diag.rejectedJar(String.valueOf(e.getMessage()));
            diag.failed("加载插件 JAR 失败: " + jar.getFileName() + ", " + e.getMessage());
            return null;
        } catch (RuntimeException e) {
            diag.failed("加载插件 JAR 异常: " + jar.getFileName() + ", " + e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private List<Candidate> discover(PluginType type, Class<? extends WalPlugin> iface,
                                     List<SourceRef> sources, PluginDiagnostics.Builder diag) {
        List<Candidate> candidates = new ArrayList<>();
        Map<String, String> seen = new LinkedHashMap<>();
        // id -> 已接受的实现类名。用于区分两类"同名"：
        //   ① 同一个实现类被两个 ClassLoader 看见（parent-first 委派的必然结果）—— 不算冲突；
        //   ② 两个不同的类声明了同一个 id —— 才是真冲突，必须记 duplicate 并只留第一个。
        // 不做这个区分的话，只要用户传了 pluginClassLoader，诊断里就会刷出一堆
        // "builtin:raw 已由 wal-core 提供" 的假冲突，healthy() 永远为 false。
        Map<String, String> seenClass = new LinkedHashMap<>();
        for (SourceRef source : sources) {
            ServiceLoader<? extends WalPlugin> loader;
            try {
                loader = ServiceLoader.load(iface, source.loader);
            } catch (ServiceConfigurationError | RuntimeException e) {
                diag.failed("ServiceLoader 初始化失败: " + type.shortName()
                        + " @ " + source.description + ", " + e);
                continue;
            }
            try {
                for (WalPlugin plugin : loader) {
                    if (plugin == null) {
                        continue;
                    }
                    String id = safeId(plugin);
                    if (id == null) {
                        diag.invalid(type.shortName() + ":<null-id> class="
                                + plugin.getClass().getName() + " @ " + source.description);
                        continue;
                    }
                    String previousSource = seen.get(id);
                    if (previousSource != null) {
                        if (plugin.getClass().getName().equals(seenClass.get(id))) {
                            // 同一实现类经由父加载器重复可见：静默跳过，不算冲突
                            continue;
                        }
                        diag.duplicate(type.shortName() + ':' + id
                                + " 已由 " + previousSource + " 提供，忽略 "
                                + source.description + " 的同名实现");
                        continue;
                    }
                    seen.put(id, source.description);
                    seenClass.put(id, plugin.getClass().getName());
                    int version = safeApiVersion(plugin);
                    if (version != WalVersion.API_VERSION) {
                        diag.incompatible(type.shortName() + ':' + id
                                + " 声明 apiVersion=" + version
                                + "，核心支持 " + WalVersion.API_VERSION
                                + "（class=" + plugin.getClass().getName()
                                + ", source=" + source.description + "）");
                        continue;
                    }
                    candidates.add(new Candidate(plugin, id, priorityOf(plugin), version,
                            source.description));
                }
            } catch (ServiceConfigurationError | RuntimeException e) {
                diag.failed("遍历候选失败: " + type.shortName() + " @ " + source.description
                        + ", " + e);
            }
        }
        // 排序：优先级降序，同分按 id 升序
        candidates.sort(Comparator
                .comparingInt((Candidate c) -> -c.priority)
                .thenComparing(c -> c.id));
        return candidates;
    }

    private List<PluginType.PluginInfo> toInfos(PluginType type, List<Candidate> candidates,
                                                String activeId) {
        List<PluginType.PluginInfo> infos = new ArrayList<>(candidates.size());
        for (Candidate c : candidates) {
            infos.add(new PluginType.PluginInfo(type, c.id, c.priority, c.apiVersion,
                    c.className(), c.source, c.id.equals(activeId)));
        }
        return List.copyOf(infos);
    }

    private Candidate select(PluginType type, List<Candidate> candidates,
                             PluginDiagnostics.Builder diag) {
        if (candidates.isEmpty()) {
            return null;
        }
        String explicit = type.explicitId(config);
        if (explicit != null && !explicit.isEmpty()) {
            for (Candidate c : candidates) {
                if (c.id.equals(explicit)) {
                    return c;
                }
            }
            diag.failed("配置强制指定 " + type.configKey() + '=' + explicit
                    + "，但未发现该实现（已发现=" + idsOf(candidates) + "）");
            return null;
        }
        return candidates.get(0);
    }

    private WalPlugin instantiate(PluginType type, Candidate candidate,
                                  PluginDiagnostics.Builder diag) {
        try {
            WalPlugin plugin = candidate.plugin;
            plugin.init(new Context());
            return plugin;
        } catch (Throwable t) {
            diag.failed(type.shortName() + ':' + candidate.id
                    + " init() 抛异常: " + t.getClass().getSimpleName() + ": " + t.getMessage()
                    + "（class=" + candidate.className() + ", source=" + candidate.source + "）");
            increment(METRIC_LOAD_FAILED);
            return null;
        }
    }

    /**
     * 无可选实现时的降级。返回内置默认实现的 id；{@code FAIL_FAST} 模式下抛异常（返回 null）。
     */
    private String applyFallback(PluginType type, PluginDiagnostics.Builder diag,
                                 List<SourceRef> sources, boolean mustReportMissing) {
        String builtin;
        switch (type) {
            case CODEC:
                builtin = RawRecordCodec.ID;
                break;
            case INDEX:
                builtin = DefaultIndexStore.ID;
                break;
            case STORAGE:
                builtin = com.walcore.internal.io.StorageBackendFactory.defaultBackendIdStatic();
                break;
            case RETENTION:
                builtin = com.walcore.internal.spiimpl.TimeSizeRetentionPolicy.ID;
                break;
            case REPLICATION:
                builtin = com.walcore.internal.spiimpl.NoopReplicationSink.ID;
                break;
            default:
                return null;
        }
        if (mustReportMissing
                && config.onPluginLoadFailure() == WalConfig.PluginFailurePolicy.FAIL_FAST) {
            increment(METRIC_LOAD_FAILED);
            throw new WalException.PluginLoadException(failFastMessage(type, sources));
        }
        increment(METRIC_FALLBACK);
        diag.fallback(type.shortName() + " -> " + builtin);
        return builtin;
    }

    /**
     * 判断"用不上插件"这件事在 {@code FAIL_FAST} 下是否必须失败。
     *
     * <h2>为什么不是"只要找不到就失败"</h2>
     * 内置实现（{@code builtin:raw} / {@code builtin:index-heap} / 平台默认存储后端）
     * <b>没有</b>注册成 {@code META-INF/services} 的 provider——它们是兜底常量，不是插件。
     * 如果"找不到 provider"就直接失败，那么一个<b>连插件都没装</b>的干净部署
     * 只要把 {@code onPluginLoadFailure} 设成 FAIL_FAST 就永远起不来，
     * 而且三条消息（codec/storage/index）会互相掩盖，这个开关等于不可用。
     *
     * <p>因此 {@code FAIL_FAST} 只在<b>真的发生了静默降级</b>时失败：
     * <ul>
     *   <li>用户显式配置了 {@code wal.spi.<type>=<id>}——"我要的那个"没起来，
     *       绝不能悄悄换成内置实现；</li>
     *   <li>该类型<b>发现了候选</b>（哪怕都被版本过滤 / init 失败剔除）——说明环境里
     *       确实装了插件，结果一个都没用上，必须让人知道。</li>
     * </ul>
     * 一个候选都没有且没有显式配置，等价于"这个部署没装插件"，走内置实现并记
     * {@code wal.plugin.fallback} 指标。
     *
     * @param type 类型
     * @param candidateCount 该类型发现的有效候选数
     * @return 必须报告失败返回 true
     */
    private boolean mustReportMissing(PluginType type, int candidateCount) {
        String explicit = type.explicitId(config);
        return (explicit != null && !explicit.isEmpty()) || candidateCount > 0;
    }

    /**
     * 构造 FAIL_FAST 的错误消息。
     *
     * <p>P0-09 ③ 要求消息<b>必须</b>同时包含四要素，缺一不可——否则运维在
     * "为什么启动失败"面前只能靠猜：
     * <ol>
     *   <li>SPI 接口全限定名；</li>
     *   <li>已尝试的 ClassLoader 列表；</li>
     *   <li>期望的 provider 文件名（{@code META-INF/services/<接口全名>}）；</li>
     *   <li>已发现的 id 列表。</li>
     * </ol>
     *
     * @param type 类型
     * @param sources 已尝试的类加载来源
     * @return 错误消息
     */
    String failFastMessage(PluginType type, List<SourceRef> sources) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("未找到可用的 SPI 实现（onPluginLoadFailure=FAIL_FAST）");
        sb.append('\n').append("  接口全名: ").append(type.interfaceName());
        sb.append('\n').append("  期望的 provider 文件名: META-INF/services/")
                .append(type.interfaceName());
        sb.append('\n').append("  已尝试的 ClassLoader:");
        if (sources.isEmpty()) {
            sb.append(' ').append(NONE_DESCRIPTION);
        } else {
            for (SourceRef s : sources) {
                sb.append('\n').append("    - ").append(s.description)
                        .append(" -> ").append(String.valueOf(s.loader));
            }
        }
        List<String> known = knownIds(type);
        sb.append('\n').append("  已发现的 id: ")
                .append(known.isEmpty() ? NONE_DESCRIPTION : String.join(", ", known));
        if (!diagnostics.incompatiblePlugins().isEmpty()) {
            sb.append('\n').append("  因版本不兼容被剔除: ")
                    .append(String.join(" | ", diagnostics.incompatiblePlugins()));
        }
        if (!diagnostics.failedPlugins().isEmpty()) {
            sb.append('\n').append("  加载失败: ")
                    .append(String.join(" | ", diagnostics.failedPlugins()));
        }
        sb.append('\n').append("  配置: wal.spi.").append(type.shortName())
                .append('=').append(String.valueOf(type.explicitId(config)));
        return sb.toString();
    }

    private List<String> knownIds(PluginType type) {
        List<String> ids = new ArrayList<>();
        List<PluginType.PluginInfo> infos = discovered.get(type);
        if (infos != null) {
            for (PluginType.PluginInfo info : infos) {
                ids.add(info.id());
            }
        }
        return ids;
    }

    private Candidate findCandidate(PluginType type, String pluginId) {
        // 先按上次扫描结果找，找不到就重新扫一次（插件可能刚 installJar）
        List<PluginType.PluginInfo> infos = discovered.get(type);
        if (infos != null) {
            for (PluginType.PluginInfo info : infos) {
                if (info.id().equals(pluginId)) {
                    return buildFromInfo(type, info);
                }
            }
        }
        scan();
        infos = discovered.get(type);
        if (infos != null) {
            for (PluginType.PluginInfo info : infos) {
                if (info.id().equals(pluginId)) {
                    return buildFromInfo(type, info);
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Candidate buildFromInfo(PluginType type, PluginType.PluginInfo info) {
        Class<? extends WalPlugin> iface = type.iface();
        if (iface == null) {
            return null;
        }
        try {
            Class<?> clazz = Class.forName(info.className(), true,
                    locateLoader(info.source()));
            WalPlugin plugin = (WalPlugin) clazz.getDeclaredConstructor().newInstance();
            return new Candidate(plugin, info.id(), info.priority(), info.apiVersion(),
                    info.source());
        } catch (Throwable t) {
            return null;
        }
    }

    private ClassLoader locateLoader(String source) {
        for (PluginType.PluginHandle handle : installedJars.values()) {
            if (source.startsWith("jar:") && source.endsWith(
                    String.valueOf(handle.jarPath().getFileName()))) {
                return handle.classLoader();
            }
        }
        ClassLoader user = config.pluginClassLoader();
        return user != null ? user : frameClassLoader;
    }

    private static List<String> idsOf(List<Candidate> candidates) {
        List<String> ids = new ArrayList<>(candidates.size());
        for (Candidate c : candidates) {
            ids.add(c.id);
        }
        return ids;
    }

    private static String idOf(WalPlugin plugin) {
        return plugin == null ? null : safeId(plugin);
    }

    private static String safeId(WalPlugin plugin) {
        try {
            String id = plugin.id();
            return id == null || id.isEmpty() ? null : id;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static int safeApiVersion(WalPlugin plugin) {
        try {
            return plugin.apiVersion();
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private static int priorityOf(WalPlugin plugin) {
        Priority p = plugin.getClass().getAnnotation(Priority.class);
        return p == null ? 0 : p.value();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // 关闭失败不影响主流程
        }
    }

    /**
     * 插件可见上下文的实现。
     */
    private final class Context implements PluginContext {

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
            return frameClassLoader;
        }

        @Override
        public Path baseDir() {
            return baseDir;
        }

        @Override
        public void warn(String code, String message, Throwable cause) {
            // 框架本身不记录日志（核心库零依赖、零副作用）；这里只累加指标，
            // 详细文本由使用方通过 wal.stats().pluginDiagnostics() 取。
            if (metrics != null) {
                metrics.increment("wal.plugin.warn." + code);
            }
        }

        @Override
        public String toString() {
            return "PluginContext{baseDir=" + baseDir + '}';
        }
    }

    /**
     * 目录里的 JAR 清单（诊断用）。
     *
     * @return 路径列表
     */
    public List<Path> listPluginJars() {
        if (pluginDir == null || !Files.isDirectory(pluginDir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(pluginDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".jar"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    @Override
    public String toString() {
        return "PluginManager{active=" + current.activeIds()
                + ", scanned=" + scanned + ", closed=" + closed + '}';
    }
}
