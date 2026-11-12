package com.walcore.spi;

import java.util.List;

/**
 * 保留策略的评估结果：<b>建议删除哪些段</b> + 一句人可读的原因。
 *
 * <p>策略只"建议"，实际删除由 {@code RetentionManager} 执行并做二次安全检查
 * （活跃段不删、仍有读者不删、越过 {@code minPinnedLsn} 不删）。
 */
public final class RetentionDecision {

    private static final RetentionDecision KEEP_ALL =
            new RetentionDecision(List.of(), "keep-all");

    private final List<SegmentInfo> toDelete;
    private final String reason;

    private RetentionDecision(List<SegmentInfo> toDelete, String reason) {
        this.toDelete = List.copyOf(toDelete);
        this.reason = reason;
    }

    /**
     * 一个段都不删。
     *
     * @return 决策
     */
    public static RetentionDecision keepAll() {
        return KEEP_ALL;
    }

    /**
     * 建议删除若干段。
     *
     * @param segments 待删除段（按 {@code firstLsn} 升序）
     * @param reason 原因（会进指标标签/日志，建议短且稳定，如 {@code older-than-7d}）
     * @return 决策
     */
    public static RetentionDecision delete(List<SegmentInfo> segments, String reason) {
        if (segments == null || segments.isEmpty()) {
            return KEEP_ALL;
        }
        return new RetentionDecision(segments, reason == null ? "unspecified" : reason);
    }

    /**
     * 待删除段列表（升序，不可变）。
     *
     * @return 段列表
     */
    public List<SegmentInfo> toDelete() {
        return toDelete;
    }

    /**
     * 决策原因。
     *
     * @return 原因
     */
    public String reason() {
        return reason;
    }

    /**
     * 是否有段需要删除。
     *
     * @return 有则 true
     */
    public boolean hasWork() {
        return !toDelete.isEmpty();
    }

    @Override
    public String toString() {
        return "RetentionDecision{delete=" + toDelete.size() + ", reason=" + reason + '}';
    }
}
