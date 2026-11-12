package com.walcore.api;

/**
 * 分区运行时选项（动态增删分区时传入）。
 *
 * <p>不可变值对象。默认值覆盖绝大多数场景，需要覆盖时用静态工厂：
 * <pre>{@code
 * wal.addPartition(PartitionOptions.defaults());
 * wal.addPartition(PartitionOptions.withBackend("builtin:storage-fc"));
 * }</pre>
 */
public final class PartitionOptions {

    /** 默认选项：继承 {@code WalConfig} 的存储后端 / codec / 保留策略。 */
    public static final PartitionOptions DEFAULTS = new PartitionOptions(null, null, null, false);

    private final String storageBackendId;
    private final String codecId;
    private final String retentionPolicyId;
    private final boolean allowIdReuse;

    private PartitionOptions(String storageBackendId, String codecId,
                             String retentionPolicyId, boolean allowIdReuse) {
        this.storageBackendId = storageBackendId;
        this.codecId = codecId;
        this.retentionPolicyId = retentionPolicyId;
        this.allowIdReuse = allowIdReuse;
    }

    /**
     * 默认选项（全部继承全局配置）。
     *
     * @return 单例常量
     */
    public static PartitionOptions defaults() {
        return DEFAULTS;
    }

    /**
     * 为某个分区覆盖存储后端。
     *
     * @param pluginId 存储后端插件 id，例如 {@code builtin:storage-fc}
     * @return 选项实例
     */
    public static PartitionOptions withBackend(String pluginId) {
        return new PartitionOptions(pluginId, null, null, false);
    }

    /**
     * 为某个分区覆盖编码器。
     *
     * @param pluginId codec 插件 id
     * @return 选项实例
     */
    public static PartitionOptions withCodec(String pluginId) {
        return new PartitionOptions(null, pluginId, null, false);
    }

    /**
     * 为某个分区覆盖保留策略。
     *
     * @param pluginId 保留策略插件 id
     * @return 选项实例
     */
    public static PartitionOptions withRetention(String pluginId) {
        return new PartitionOptions(null, null, pluginId, false);
    }

    /**
     * 声明允许复用已 PURGED 的分区 ID（默认不允许）。
     *
     * @param allow 是否允许
     * @return 选项实例
     */
    public static PartitionOptions allowingIdReuse(boolean allow) {
        return new PartitionOptions(null, null, null, allow);
    }

    /**
     * 存储后端插件 id 覆盖值。
     *
     * @return 插件 id，null 表示继承全局配置
     */
    public String storageBackendId() {
        return storageBackendId;
    }

    /**
     * 编码器插件 id 覆盖值。
     *
     * @return 插件 id，null 表示继承全局配置
     */
    public String codecId() {
        return codecId;
    }

    /**
     * 保留策略插件 id 覆盖值。
     *
     * @return 插件 id，null 表示继承全局配置
     */
    public String retentionPolicyId() {
        return retentionPolicyId;
    }

    /**
     * 是否允许复用已 PURGED 的分区 ID。
     *
     * @return 允许返回 true
     */
    public boolean allowIdReuse() {
        return allowIdReuse;
    }

    @Override
    public String toString() {
        return "PartitionOptions{storageBackendId=" + storageBackendId
                + ", codecId=" + codecId
                + ", retentionPolicyId=" + retentionPolicyId
                + ", allowIdReuse=" + allowIdReuse
                + '}';
    }
}
