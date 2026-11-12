package com.walcore.internal.plugin;

import com.walcore.api.WalConfig;
import com.walcore.spi.IndexStore;
import com.walcore.spi.RecordCodec;
import com.walcore.spi.WalPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 五类 SPI 的类型标识，以及簇生在同一个文件里的小值对象
 * （{@code PluginInfo} / {@code PluginSet} / {@code SwapResult} / {@code SwapPolicy} / {@code PluginHandle}）。
 *
 * <p>刻意合并到一个文件：这五个类型总是一起被使用、一起演进，分散成 5 个文件只会
 * 增加导航成本而没有隔离收益（ARCH §17.2 #77 的决策）。
 *
 * <p><b>关于 RETENTION / REPLICATION</b>：对应 SPI 接口（{@code RetentionPolicy} /
 * {@code ReplicationSink}）属于 T14 的交付范围，因此本枚举里这两项的
 * {@link #iface()} 目前返回 {@code null}。{@link PluginManager} 对 {@code null}
 * 接口的类型返回空候选列表，并<b>不会</b>因此抛异常——这样 T14 落地时只需把
 * 接口接上，插件框架零改动。
 */
public enum PluginType {

    /** ① 记录编码器，SPI 接口 {@code com.walcore.spi.RecordCodec}。 */
    CODEC("codec", "com.walcore.spi.RecordCodec"),

    /** ② 存储介质，SPI 接口 {@code com.walcore.spi.StorageBackend}。 */
    STORAGE("storage", "com.walcore.spi.StorageBackend"),

    /** ③ 稀疏索引，SPI 接口 {@code com.walcore.spi.IndexStore}。 */
    INDEX("index", "com.walcore.spi.IndexStore"),

    /** ④ 保留策略，SPI 接口 {@code com.walcore.spi.RetentionPolicy}（T14 交付）。 */
    RETENTION("retention", "com.walcore.spi.RetentionPolicy"),

    /** ⑤ 复制/订阅，SPI 接口 {@code com.walcore.spi.ReplicationSink}（T14 交付）。 */
    REPLICATION("replication", "com.walcore.spi.ReplicationSink");

    private final String shortName;
    private final String interfaceName;

    PluginType(String shortName, String interfaceName) {
        this.shortName = shortName;
        this.interfaceName = interfaceName;
    }

    /**
     * 短名，用于配置键 {@code wal.spi.<shortName>=<id>} 与指标标签。
     *
     * @return 短名
     */
    public String shortName() {
        return shortName;
    }

    /**
     * SPI 接口的全限定名。
     *
     * @return 全限定名
     */
    public String interfaceName() {
        return interfaceName;
    }

    /**
     * 配置键：{@code wal.spi.<shortName>}。
     *
     * @return 配置键
     */
    public String configKey() {
        return "wal.spi." + shortName;
    }

    /**
     * 对应的 SPI 接口类型。
     *
     * @return 接口类型；尚未交付的类型返回 {@code null}（见类注释）
     */
    @SuppressWarnings("unchecked")
    public Class<? extends WalPlugin> iface() {
        try {
            return (Class<? extends WalPlugin>) Class.forName(interfaceName);
        } catch (ClassNotFoundException e) {
            // 五类接口都在 api 包里随核心一起发布，理论上不可能找不到；
            // 返回 null 让 PluginManager 走"该类型无候选"的既定路径，而不是让开办失败。
            return null;
        }
    }

    /**
     * 该类型在 {@link WalConfig} 中的显式指定 id（强制生效的那一个）。
     *
     * @param config 配置
     * @return 显式 id；未指定返回 null
     */
    public String explicitId(WalConfig config) {
        if (config == null) {
            return null;
        }
        switch (this) {
            case CODEC:
                return config.codecId();
            case STORAGE:
                return config.storageBackendId();
            case INDEX:
                return config.indexStoreId();
            case RETENTION:
                return config.retentionPolicyId();
            default:
                return config.replicationSinkId();
        }
    }

    /**
     * 一个已发现的插件候选。
     */
    public static final class PluginInfo {

        private final PluginType type;
        private final String id;
        private final int priority;
        private final int apiVersion;
        private final String className;
        private final String source;
        private final boolean active;

        /**
         * 构造插件信息。
         *
         * @param type 类型
         * @param id 插件 id
         * @param priority 优先级
         * @param apiVersion 声明的契约版本
         * @param className 实现类全限定名
         * @param source 来源描述（哪个 ClassLoader / 哪个 JAR）
         * @param active 是否当前生效
         */
        public PluginInfo(PluginType type, String id, int priority, int apiVersion,
                          String className, String source, boolean active) {
            this.type = type;
            this.id = id;
            this.priority = priority;
            this.apiVersion = apiVersion;
            this.className = className;
            this.source = source;
            this.active = active;
        }

        /**
         * 类型。
         *
         * @return 类型
         */
        public PluginType type() {
            return type;
        }

        /**
         * 插件 id。
         *
         * @return id
         */
        public String id() {
            return id;
        }

        /**
         * 优先级（越大越优先）。
         *
         * @return 优先级
         */
        public int priority() {
            return priority;
        }

        /**
         * 声明的契约版本。
         *
         * @return 版本
         */
        public int apiVersion() {
            return apiVersion;
        }

        /**
         * 实现类全限定名。
         *
         * @return 类名
         */
        public String className() {
            return className;
        }

        /**
         * 来源描述。
         *
         * @return 来源
         */
        public String source() {
            return source;
        }

        /**
         * 是否当前生效。
         *
         * @return 生效返回 true
         */
        public boolean active() {
            return active;
        }

        /**
         * 返回一个 active 标记不同的副本。
         *
         * @param newActive 新的 active 标记
         * @return 新实例
         */
        public PluginInfo withActive(boolean newActive) {
            return new PluginInfo(type, id, priority, apiVersion, className, source, newActive);
        }

        @Override
        public String toString() {
            return "PluginInfo{" + type.shortName() + '/' + id
                    + ", priority=" + priority
                    + ", apiVersion=" + apiVersion
                    + ", class=" + className
                    + ", source=" + source
                    + (active ? ", ACTIVE" : "") + '}';
        }
    }

    /**
     * 一次加载产出的<b>不可变</b>插件集合，装入 WAL 实例。
     *
     * <p>运行期热切换通过替换这些引用实现，见 {@link PluginManager#swap}。
     */
    public static final class PluginSet {

        private final RecordCodec codec;
        private final IndexStore indexStore;
        private final String storageBackendId;
        private final com.walcore.spi.RetentionPolicy retentionPolicy;
        private final com.walcore.spi.ReplicationSink replicationSink;
        private final Map<String, String> activeIds;

        /**
         * 构造插件集合。
         *
         * @param codec 生效的编解码器
         * @param indexStore 生效的索引实现
         * @param storageBackendId 生效的存储后端 id
         * @param activeIds 类型短名 -&gt; 插件 id
         */
        public PluginSet(RecordCodec codec, IndexStore indexStore, String storageBackendId,
                         Map<String, String> activeIds) {
            this(codec, indexStore, storageBackendId, null, null, activeIds);
        }

        /**
         * 构造<b>完整</b>插件集合（T14 起包含保留策略与复制实现）。
         *
         * @param codec 生效的编解码器
         * @param indexStore 生效的索引实现
         * @param storageBackendId 生效的存储后端 id
         * @param retentionPolicy 生效的保留策略
         * @param replicationSink 生效的复制实现
         * @param activeIds 类型短名 -&gt; 插件 id
         */
        public PluginSet(RecordCodec codec, IndexStore indexStore, String storageBackendId,
                         com.walcore.spi.RetentionPolicy retentionPolicy,
                         com.walcore.spi.ReplicationSink replicationSink,
                         Map<String, String> activeIds) {
            this.codec = codec;
            this.indexStore = indexStore;
            this.storageBackendId = storageBackendId;
            this.retentionPolicy = retentionPolicy;
            this.replicationSink = replicationSink;
            this.activeIds = activeIds == null
                    ? Collections.emptyMap()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(activeIds));
        }

        /**
         * 生效的编解码器。
         *
         * @return 编解码器
         */
        public RecordCodec codec() {
            return codec;
        }

        /**
         * 生效的索引实现。
         *
         * @return 索引实现，可能为 null
         */
        public IndexStore indexStore() {
            return indexStore;
        }

        /**
         * 生效的存储后端 id。
         *
         * @return 插件 id
         */
        public String storageBackendId() {
            return storageBackendId;
        }

        /**
         * 生效的保留策略。
         *
         * @return 策略；尚未加载返回 null
         */
        public com.walcore.spi.RetentionPolicy retentionPolicy() {
            return retentionPolicy;
        }

        /**
         * 生效的复制实现。
         *
         * @return 实现；尚未加载返回 null
         */
        public com.walcore.spi.ReplicationSink replicationSink() {
            return replicationSink;
        }

        /**
         * 类型短名 -&gt; 插件 id 的不可变映射（供 {@code wal.stats().activePlugins()} 使用）。
         *
         * @return 映射
         */
        public Map<String, String> activeIds() {
            return activeIds;
        }

        @Override
        public String toString() {
            return "PluginSet" + activeIds;
        }
    }

    /**
     * 旧实例何时关闭的切换策略。
     */
    public enum SwapPolicy {

        /** 立即切换并立刻 {@code close()} 旧实例。危险：in-flight 调用会失败。仅测试用。 */
        IMMEDIATE,

        /** 新实例就绪后原子切换引用，旧实例在引用计数归零后关闭（默认）。 */
        DRAIN_THEN_CLOSE,

        /**
         * 新旧并行 {@code windowMillis} 后切换并关闭旧的（灰度用）。
         *
         * <p>本实现把它等价于 {@link #DRAIN_THEN_CLOSE}，因为"双写"对
         * {@code RecordCodec}（纯函数式）没有意义，对 {@code ReplicationSink}
         * 才有意义（T14）。此处如实标注，避免给出虚假能力。
         */
        SHADOW_THEN_SWAP
    }

    /**
     * 热切换结果。
     *
     * @param <T> 插件类型
     */
    public static final class SwapResult<T extends WalPlugin> {

        private final boolean success;
        private final T previous;
        private final T current;
        private final String failureReason;
        private final boolean rolledBack;

        private SwapResult(boolean success, T previous, T current,
                           String failureReason, boolean rolledBack) {
            this.success = success;
            this.previous = previous;
            this.current = current;
            this.failureReason = failureReason;
            this.rolledBack = rolledBack;
        }

        /**
         * 成功结果。
         *
         * @param previous 旧实例
         * @param current 新实例
         * @param <T> 插件类型
         * @return 结果
         */
        public static <T extends WalPlugin> SwapResult<T> ok(T previous, T current) {
            return new SwapResult<>(true, previous, current, null, false);
        }

        /**
         * 失败结果（已自动回滚，服务不中断）。
         *
         * @param previous 仍然生效的旧实例
         * @param reason 失败原因
         * @param <T> 插件类型
         * @return 结果
         */
        public static <T extends WalPlugin> SwapResult<T> failed(T previous, String reason) {
            return new SwapResult<>(false, previous, previous, reason, true);
        }

        /**
         * 是否切换成功。
         *
         * @return 成功返回 true
         */
        public boolean success() {
            return success;
        }

        /**
         * 切换前的实例。
         *
         * @return 旧实例，可能为 null
         */
        public T previous() {
            return previous;
        }

        /**
         * 切换后生效的实例（失败时为旧实例）。
         *
         * @return 实例
         */
        public T current() {
            return current;
        }

        /**
         * 失败原因。
         *
         * @return 失败时非 null
         */
        public String failureReason() {
            return failureReason;
        }

        /**
         * 是否已自动回滚。
         *
         * @return 回滚过返回 true
         */
        public boolean rolledBack() {
            return rolledBack;
        }

        @Override
        public String toString() {
            return "SwapResult{success=" + success
                    + (failureReason == null ? "" : ", reason=" + failureReason)
                    + (rolledBack ? ", ROLLED_BACK" : "") + '}';
        }
    }

    /**
     * 一个运行期加载的插件 JAR 的句柄。
     */
    public static final class PluginHandle {

        private final java.nio.file.Path jarPath;
        private final PluginClassLoader classLoader;
        private final java.util.List<String> pluginIds;

        /**
         * 构造句柄。
         *
         * @param jarPath JAR 路径
         * @param classLoader 该 JAR 专属类加载器
         * @param pluginIds 该 JAR 声明的全部插件 id
         */
        public PluginHandle(java.nio.file.Path jarPath, PluginClassLoader classLoader,
                            java.util.List<String> pluginIds) {
            this.jarPath = jarPath;
            this.classLoader = classLoader;
            this.pluginIds = java.util.List.copyOf(pluginIds);
        }

        /**
         * JAR 路径。
         *
         * @return 路径
         */
        public java.nio.file.Path jarPath() {
            return jarPath;
        }

        /**
         * 该 JAR 的类加载器。
         *
         * @return 类加载器
         */
        public PluginClassLoader classLoader() {
            return classLoader;
        }

        /**
         * 该 JAR 声明的插件 id 列表。
         *
         * @return 不可变列表
         */
        public java.util.List<String> pluginIds() {
            return pluginIds;
        }

        @Override
        public String toString() {
            return "PluginHandle{" + jarPath + ", ids=" + pluginIds + '}';
        }
    }
}
