package com.walcore.api;

/**
 * 分区移除模式。
 *
 * <p>三种模式的语义差别只在于<b>数据是否保留</b>，写入侧行为完全一致（都先拒绝新写入）。
 *
 * @see com.walcore.api.PartitionOptions
 */
public enum RemoveMode {

    /**
     * 冻结（默认）：状态 {@code ACTIVE -> FROZEN}，拒绝新 {@code append}，
     * 已有 Cursor / Tailer 可继续读到段尾；后台在引用计数归零后释放 mmap。数据保留、可读。
     *
     * <p>适用场景：品种下线但仍需保留审计数据。
     */
    FREEZE,

    /**
     * 归档：先 {@code FREEZE}，再通知 {@code RetentionPolicy} 走归档流程（P2-03），
     * 上传成功后删除本地文件。
     *
     * <p>适用场景：冷数据下沉到对象存储。
     */
    ARCHIVE,

    /**
     * 清除：{@code FREEZE} → 等待 {@code readerCount == 0}（最长 {@code purgeTimeoutMillis}）
     * → 删除全部段 + {@code .idx} + {@code .cursor} → 写 {@code pXXXX.state = PURGED}
     * 并在 {@code wal.meta} 记录 tombstone。
     *
     * <p>默认 {@code allowPartitionIdReuse=false}，因此该 id 之后永不复用（防止回滚的旧备份
     * 文件被误认成新数据）。
     *
     * <p>适用场景：测试清理、误建分区。
     */
    PURGE
}
