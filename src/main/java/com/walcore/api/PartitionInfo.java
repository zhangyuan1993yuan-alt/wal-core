package com.walcore.api;

/**
 * 分区的只读元信息快照。
 *
 * <p>由 {@code wal.stats()} / {@code wal.partitionInfo(pid)} 现场构造，属于"已冻结的观测数据"，
 * 不保证与随后发生的写入保持严格一致——它表达的是<b>采样瞬间</b>的事实。
 */
public final class PartitionInfo {

    /**
     * 分区生命周期状态。迁移方向单向且不可逆：
     * {@code ACTIVE -> FROZEN -> PURGED}（{@code ARCHIVED} 是 {@code FROZEN} 的一个变体）。
     */
    public enum State {
        /** 可读可写。 */
        ACTIVE,
        /** 拒绝新写入，已有游标可继续读到段尾。 */
        FROZEN,
        /** 已通知保留策略走归档流程，等待本地文件被删除。 */
        ARCHIVED,
        /** 数据已删除，ID 默认永不复用。 */
        PURGED
    }

    private final int partitionId;
    private final long firstLsn;
    private final long lastLsn;
    private final long durableLsn;
    private final int segmentCount;
    private final long totalBytes;
    private final State state;

    /**
     * 构造分区信息快照。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 分区内首条记录 LSN，空分区为 0
     * @param lastLsn 分区内末条记录 LSN（已发布），空分区为 0
     * @param durableLsn 已持久化水位
     * @param segmentCount 现存段数
     * @param totalBytes 现存段总字节数
     * @param state 分区状态
     */
    public PartitionInfo(int partitionId, long firstLsn, long lastLsn, long durableLsn,
                         int segmentCount, long totalBytes, State state) {
        this.partitionId = partitionId;
        this.firstLsn = firstLsn;
        this.lastLsn = lastLsn;
        this.durableLsn = durableLsn;
        this.segmentCount = segmentCount;
        this.totalBytes = totalBytes;
        this.state = state;
    }

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * 分区内首条记录 LSN。
     *
     * @return LSN，空分区返回 0
     */
    public long firstLsn() {
        return firstLsn;
    }

    /**
     * 分区内已发布的末条记录 LSN。
     *
     * @return LSN，空分区返回 0
     */
    public long lastLsn() {
        return lastLsn;
    }

    /**
     * 分区已持久化水位。
     *
     * @return LSN
     */
    public long durableLsn() {
        return durableLsn;
    }

    /**
     * 现存段数。
     *
     * @return 段数
     */
    public int segmentCount() {
        return segmentCount;
    }

    /**
     * 现存段总字节数。
     *
     * @return 字节数
     */
    public long totalBytes() {
        return totalBytes;
    }

    /**
     * 分区状态。
     *
     * @return 状态
     */
    public State state() {
        return state;
    }

    @Override
    public String toString() {
        return "PartitionInfo{id=" + partitionId
                + ", firstLsn=" + firstLsn
                + ", lastLsn=" + lastLsn
                + ", durableLsn=" + durableLsn
                + ", segments=" + segmentCount
                + ", bytes=" + totalBytes
                + ", state=" + state
                + '}';
    }
}
