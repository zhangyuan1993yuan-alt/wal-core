package com.walcore.api;

import java.util.Collections;
import java.util.List;

/**
 * 关闭结果报告。
 *
 * <p>{@code close(Duration)} 在超时后<b>不抛异常</b>（抛异常会掩盖原始故障），
 * 而是返回 {@code clean = false} 的报告，由运维侧决定告警。这一点是 P0-10 的明确要求。
 */
public final class CloseReport {

    private final boolean clean;
    private final List<Integer> unflushedPartitions;
    private final long maxUnflushedLsn;
    private final long elapsedMillis;
    private final String detail;

    /**
     * 构造关闭报告。
     *
     * @param clean 是否干净关闭（所有分区都刷盘完成且 {@code wal.meta} 已写入 cleanShutdown=true）
     * @param unflushedPartitions 未刷盘完成的分区 ID 列表，无则空列表
     * @param maxUnflushedLsn 未刷盘分区中最大的已发布 LSN，无则 0
     * @param elapsedMillis 关闭耗时（毫秒）
     * @param detail 人类可读的补充说明，可为 null
     */
    public CloseReport(boolean clean, List<Integer> unflushedPartitions,
                       long maxUnflushedLsn, long elapsedMillis, String detail) {
        this.clean = clean;
        this.unflushedPartitions = unflushedPartitions == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(unflushedPartitions);
        this.maxUnflushedLsn = maxUnflushedLsn;
        this.elapsedMillis = elapsedMillis;
        this.detail = detail;
    }

    /**
     * 构造一个干净的关闭报告。
     *
     * @param elapsedMillis 关闭耗时（毫秒）
     * @return 报告
     */
    public static CloseReport clean(long elapsedMillis) {
        return new CloseReport(true, Collections.emptyList(), 0L, elapsedMillis, null);
    }

    /**
     * 是否干净关闭。
     *
     * @return 干净返回 true
     */
    public boolean clean() {
        return clean;
    }

    /**
     * 未刷盘完成的分区 ID 列表。
     *
     * @return 不可变列表
     */
    public List<Integer> unflushedPartitions() {
        return unflushedPartitions;
    }

    /**
     * 未刷盘分区中最大的已发布 LSN。
     *
     * @return LSN，无未刷盘分区返回 0
     */
    public long maxUnflushedLsn() {
        return maxUnflushedLsn;
    }

    /**
     * 关闭耗时（毫秒）。
     *
     * @return 毫秒
     */
    public long elapsedMillis() {
        return elapsedMillis;
    }

    /**
     * 补充说明。
     *
     * @return 说明文本，可能为 null
     */
    public String detail() {
        return detail;
    }

    @Override
    public String toString() {
        return "CloseReport{clean=" + clean
                + ", unflushedPartitions=" + unflushedPartitions
                + ", maxUnflushedLsn=" + maxUnflushedLsn
                + ", elapsedMillis=" + elapsedMillis
                + (detail == null ? "" : ", detail=" + detail)
                + '}';
    }
}
