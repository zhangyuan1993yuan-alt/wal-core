package com.walcore.internal.plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 插件加载诊断报告（ARCH §10.4.4 的"用户可见"部分）。
 *
 * <p>设计目标：<b>任何一次"我的插件为什么没生效"都能在这里查到原因</b>。
 * 因此除了成功列表，本类还分别记录 6 类失败：
 * <ul>
 *   <li>{@link #duplicateIds()} —— id 冲突（首个胜出）；</li>
 *   <li>{@link #incompatiblePlugins()} —— {@code apiVersion} 不匹配；</li>
 *   <li>{@link #invalidPlugins()} —— 不是 {@code WalPlugin} 实例 / id 为空；</li>
 *   <li>{@link #failedPlugins()} —— 实例化或 {@code init()} 抛异常；</li>
 *   <li>{@link #rejectedJars()} —— JAR 内含 {@code com/walcore/} 核心类被拒；</li>
 *   <li>{@link #fallbacks()} —— 该类型无可用实现，回退内置默认。</li>
 * </ul>
 *
 * <p>不可变、线程安全。
 */
public final class PluginDiagnostics {

    private final List<String> loadedIds;
    private final List<String> failedPlugins;
    private final List<String> incompatiblePlugins;
    private final List<String> duplicateIds;
    private final List<String> invalidPlugins;
    private final List<String> rejectedJars;
    private final List<String> fallbacks;
    private final List<String> sources;
    private final Map<String, String> active;

    private PluginDiagnostics(Builder b) {
        this.loadedIds = List.copyOf(b.loadedIds);
        this.failedPlugins = List.copyOf(b.failedPlugins);
        this.incompatiblePlugins = List.copyOf(b.incompatiblePlugins);
        this.duplicateIds = List.copyOf(b.duplicateIds);
        this.invalidPlugins = List.copyOf(b.invalidPlugins);
        this.rejectedJars = List.copyOf(b.rejectedJars);
        this.fallbacks = List.copyOf(b.fallbacks);
        this.sources = List.copyOf(b.sources);
        this.active = Collections.unmodifiableMap(new LinkedHashMap<>(b.active));
    }

    /**
     * 空报告（尚未扫描）。
     *
     * @return 报告
     */
    public static PluginDiagnostics empty() {
        return builder().build();
    }

    /**
     * 创建构建器（仅供 {@link PluginManager} 使用）。
     *
     * @return 构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 加载成功的插件 id（形如 {@code codec:builtin:raw}）。
     *
     * @return 不可变列表
     */
    public List<String> loadedIds() {
        return loadedIds;
    }

    /**
     * 实例化/初始化失败的插件描述（含异常摘要）。
     *
     * @return 不可变列表
     */
    public List<String> failedPlugins() {
        return failedPlugins;
    }

    /**
     * {@code apiVersion} 不兼容而被剔除的插件描述。
     *
     * @return 不可变列表
     */
    public List<String> incompatiblePlugins() {
        return incompatiblePlugins;
    }

    /**
     * id 冲突记录（首个胜出）。
     *
     * @return 不可变列表
     */
    public List<String> duplicateIds() {
        return duplicateIds;
    }

    /**
     * 非法插件描述（不是 {@code WalPlugin}、id 为空等）。
     *
     * @return 不可变列表
     */
    public List<String> invalidPlugins() {
        return invalidPlugins;
    }

    /**
     * 因内含核心类而被拒绝的 JAR。
     *
     * @return 不可变列表
     */
    public List<String> rejectedJars() {
        return rejectedJars;
    }

    /**
     * 回退内置默认的类型（形如 {@code codec -> builtin:raw}）。
     *
     * @return 不可变列表
     */
    public List<String> fallbacks() {
        return fallbacks;
    }

    /**
     * 已扫描的类加载来源（有序，去重）。
     *
     * @return 不可变列表
     */
    public List<String> sources() {
        return sources;
    }

    /**
     * 类型短名 -&gt; 生效插件 id。
     *
     * @return 不可变映射
     */
    public Map<String, String> active() {
        return active;
    }

    /**
     * 是否一切正常（无失败、无不兼容、无重复、无回退）。
     *
     * @return 正常返回 true
     */
    public boolean healthy() {
        return failedPlugins.isEmpty() && incompatiblePlugins.isEmpty()
                && duplicateIds.isEmpty() && invalidPlugins.isEmpty()
                && rejectedJars.isEmpty() && fallbacks.isEmpty();
    }

    @Override
    public String toString() {
        return "PluginDiagnostics{loaded=" + loadedIds
                + ", active=" + active
                + (failedPlugins.isEmpty() ? "" : ", failed=" + failedPlugins)
                + (incompatiblePlugins.isEmpty() ? "" : ", incompatible=" + incompatiblePlugins)
                + (duplicateIds.isEmpty() ? "" : ", duplicates=" + duplicateIds)
                + (invalidPlugins.isEmpty() ? "" : ", invalid=" + invalidPlugins)
                + (rejectedJars.isEmpty() ? "" : ", rejectedJars=" + rejectedJars)
                + (fallbacks.isEmpty() ? "" : ", fallbacks=" + fallbacks) + '}';
    }

    /**
     * {@link PluginDiagnostics} 的可变构建器。
     */
    public static final class Builder {

        private final List<String> loadedIds = new ArrayList<>();
        private final List<String> failedPlugins = new ArrayList<>();
        private final List<String> incompatiblePlugins = new ArrayList<>();
        private final List<String> duplicateIds = new ArrayList<>();
        private final List<String> invalidPlugins = new ArrayList<>();
        private final List<String> rejectedJars = new ArrayList<>();
        private final List<String> fallbacks = new ArrayList<>();
        private final List<String> sources = new ArrayList<>();
        private final Map<String, String> active = new LinkedHashMap<>();

        private Builder() {
        }

        /**
         * 记录一个加载成功的插件。
         *
         * @param desc 描述
         * @return this
         */
        public Builder loaded(String desc) {
            loadedIds.add(desc);
            return this;
        }

        /**
         * 记录一个失败插件。
         *
         * @param desc 描述
         * @return this
         */
        public Builder failed(String desc) {
            failedPlugins.add(desc);
            return this;
        }

        /**
         * 记录一个版本不兼容插件。
         *
         * @param desc 描述
         * @return this
         */
        public Builder incompatible(String desc) {
            incompatiblePlugins.add(desc);
            return this;
        }

        /**
         * 记录一个重复 id。
         *
         * @param desc 描述
         * @return this
         */
        public Builder duplicate(String desc) {
            duplicateIds.add(desc);
            return this;
        }

        /**
         * 记录一个非法插件。
         *
         * @param desc 描述
         * @return this
         */
        public Builder invalid(String desc) {
            invalidPlugins.add(desc);
            return this;
        }

        /**
         * 记录一个被拒绝的 JAR。
         *
         * @param desc 描述
         * @return this
         */
        public Builder rejectedJar(String desc) {
            rejectedJars.add(desc);
            return this;
        }

        /**
         * 记录一次内置回退。
         *
         * @param desc 描述
         * @return this
         */
        public Builder fallback(String desc) {
            fallbacks.add(desc);
            return this;
        }

        /**
         * 记录一个已扫描的类加载来源。
         *
         * @param desc 描述
         * @return this
         */
        public Builder source(String desc) {
            if (!sources.contains(desc)) {
                sources.add(desc);
            }
            return this;
        }

        /**
         * 记录某类型当前生效的插件 id。
         *
         * @param type 类型短名
         * @param id 插件 id
         * @return this
         */
        public Builder active(String type, String id) {
            active.put(type, id);
            return this;
        }

        /**
         * 构建不可变报告。
         *
         * @return 报告
         */
        public PluginDiagnostics build() {
            return new PluginDiagnostics(this);
        }
    }
}
