package com.walcore.spi;

import java.util.List;

/**
 * 保留策略的输入快照：<b>一次评估所需的全部事实</b>，不可变。
 *
 * <p>刻意做成"把事实一次性给全"而不是"给策略一个管理器句柄"：策略必须只读、
 * 不得自己删文件（删除动作由 {@code RetentionManager} 统一执行，它才能保证
 * {@code readerCount == 0} 与索引/映射的释放顺序）。这样第三方策略写错也不会造成
 * 数据损坏，最坏只是"该删的没删"。
 */
public final class RetentionContext {

    private final int partitionId;
    private final List<SegmentInfo> segments;
    private final long nowMillis;
    private final long totalBytes;
    private final long minPinnedLsn;
    private final long durableLsn;

    /**
     * 构造上下文。
     *
     * @param partitionId 分区 ID
     * @param segments 全部段，按 {@code firstLsn} 升序
     * @param nowMillis 评估时刻（epoch 毫秒，由管理器统一取，保证同一轮内一致）
     * @param totalBytes 全部段的逻辑占用之和
     * @param minPinnedLsn 不可删除边界（最小的 tailer/cursor/快照位点）；0 表示无消费者
     * @param durableLsn 已持久化水位
     */
    public RetentionContext(int partitionId, List<SegmentInfo> segments, long nowMillis,
                            long totalBytes, long minPinnedLsn, long durableLsn) {
        this.partitionId = partitionId;
        this.segments = List.copyOf(segments);
        this.nowMillis = nowMillis;
        this.totalBytes = totalBytes;
        this.minPinnedLsn = minPinnedLsn;
        this.durableLsn = durableLsn;
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
     * 全部段（升序，不可变）。
     *
     * @return 段列表
     */
    public List<SegmentInfo> segments() {
        return segments;
    }

    /**
     * 评估时刻。
     *
     * @return epoch 毫秒
     */
    public long nowMillis() {
        return nowMillis;
    }

    /**
     * 全部段逻辑占用之和。
     *
     * @return 字节数
     */
    public long totalBytes() {
        return totalBytes;
    }

    /**
     * 不可删除边界：{@code > 0} 时，任何 {@code lastLsn >= minPinnedLsn} 的段都不得删除。
     *
     * @return LSN；0 表示没有活跃消费者，无保护
     */
    public long minPinnedLsn() {
        return minPinnedLsn;
    }

    /**
     * 已持久化水位。
     *
     * @return LSN
     */
    public long durableLsn() {
        return durableLsn;
    }

    @Override
    public String toString() {
        return "RetentionContext{p" + partitionId
                + ", segments=" + segments.size()
                + ", totalBytes=" + totalBytes
                + ", minPinnedLsn=" + minPinnedLsn
                + ", durableLsn=" + durableLsn + '}';
    }
}
