package com.walcore.api;

/**
 * 单个分区的顺序读游标（ARCH §8.1）。
 *
 * <p><b>非线程安全</b>：一个游标只能被一个线程使用；多线程读请各开一个游标。
 *
 * <p><b>语义要点</b>：
 * <ul>
 *   <li>只读已发布的字节区间（{@code Segment.publishedWritePos}），因此<b>不会读到半条记录</b>；</li>
 *   <li>可以追上正在写入的活跃段，游标与写入方无锁并发；</li>
 *   <li>{@link #close()} 后必须释放段的 reader 引用计数，否则段永远删不掉。</li>
 * </ul>
 *
 * <pre>{@code
 * try (WalCursor c = wal.cursor(0, CursorOptions.DEFAULTS)) {
 *     WalRecord r;
 *     while ((r = c.next()) != null) {
 *         handle(r);
 *     }
 * }
 * }</pre>
 */
public interface WalCursor extends AutoCloseable {

    /**
     * 游标所属分区。
     *
     * @return 分区 ID
     */
    int partitionId();

    /**
     * 下一次 {@link #next()} 将返回的 LSN。
     *
     * <p>已读到末尾时返回"期望的下一个 LSN"（即 {@code lastReturnedLsn + 1}），
     * 而不是 -1——这样调用方可以把它安全地当作 checkpoint 存下来。
     *
     * @return LSN
     */
    long position();

    /**
     * 定位到第一条 {@code lsn >= target} 的记录。
     *
     * <p>定位路径：{@code SegmentManager.findSegmentFor} 段级二分 → {@code SparseIndex.lookup}
     * 索引级二分 → 段内顺序扫。目标大于分区末尾时返回 {@code false} 且游标停在末尾。
     *
     * @param lsn 目标 LSN
     * @return 找到返回 true；已越过分区末尾返回 false
     */
    boolean seekTo(long lsn);

    /**
     * 取下一条记录（不阻塞）。
     *
     * <p>返回的 {@link WalRecord} 是<b>复用实例</b>，下次调用即被覆盖。
     *
     * @return 记录；无更多可读记录（已追上写入方）返回 {@code null}
     */
    WalRecord next();

    /**
     * 批量读取。
     *
     * <p>与逐条 {@link #next()} 结果完全一致，但 visitor 收到的是同一个复用实例，
     * 因此不产生逐条对象分配。
     *
     * @param maxRecords 本批最多条数，须为正
     * @param visitor 回调，返回 {@code false} 提前结束
     * @return 实际条数（0 表示已追平）
     */
    int nextBatch(int maxRecords, WalRecordVisitor visitor);

    /**
     * 注册段切换监听（跨段读时收到通知，便于监控"正在读第几个段"）。
     *
     * @param listener 监听器，可为 null（取消）
     */
    void onSegmentRollover(SegmentRolloverListener listener);

    /**
     * 关闭游标并释放段引用。
     */
    @Override
    void close();

    /**
     * 段切换回调。
     */
    @FunctionalInterface
    interface SegmentRolloverListener {

        /**
         * 游标从一个段切到另一个段。
         *
         * @param partitionId 分区 ID
         * @param newSegmentFirstLsn 新段的首条 LSN（段名中的值，可能大于真实首条 LSN）
         */
        void onRollover(int partitionId, long newSegmentFirstLsn);
    }
}
