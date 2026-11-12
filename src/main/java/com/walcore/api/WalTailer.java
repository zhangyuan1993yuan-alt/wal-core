package com.walcore.api;

/**
 * 实时订阅句柄：从某个 LSN 开始"先追历史、追平后转实时"，并把记录推给 {@link TailHandler}。
 *
 * <h2>语义保证</h2>
 * <ol>
 *   <li><b>无空隙、无重复</b>：历史阶段与实时阶段共用同一个游标，切换点只做一次
 *       "已追平"判定，因此交付的 LSN 序列是 {@code [fromLsn, ...]} 的连续前缀（在未溢出时）；</li>
 *   <li><b>不阻塞写入</b>：消费在独立线程上进行，回调慢只会让自己落后；</li>
 *   <li><b>可观测</b>：{@link #lagRecords()} 与 {@link #isLive()} 让调用方区分
 *       "还在补历史"与"已实时"；</li>
 *   <li><b>可关闭</b>：{@link #close()} 后线程在有限时间内退出，不再回调。</li>
 * </ol>
 *
 * <h2>溢出</h2>
 * 落后条数超过 {@link TailerOptions#bufferSize()} 时按
 * {@link WalConfig.TailerOverflowPolicy} 处理：
 * <ul>
 *   <li>{@code DISCONNECT}：回调 {@link TailHandler#onOverflow} 后关闭；</li>
 *   <li>{@code DROP_TO_LATEST}：跳到最新一条继续（丢弃量计入 {@code wal.tailer.dropped}）；</li>
 *   <li>{@code BLOCK}：<b>不丢</b>，继续按顺序消费（落后的代价由订阅方自己承担）。</li>
 * </ul>
 */
public interface WalTailer extends AutoCloseable {

    /**
     * 订阅的分区 ID。
     *
     * @return 分区 ID
     */
    int partitionId();

    /**
     * 订阅起始 LSN（构造时确定，不随进度变化）。
     *
     * @return LSN
     */
    long subscribedFromLsn();

    /**
     * 当前落后的条数（{@code publishedLsn - 已交付的最大 LSN}，不为负）。
     *
     * @return 条数；追平后为 0
     */
    long lagRecords();

    /**
     * 是否已进入实时阶段（历史上已追平，且此后保持追平）。
     *
     * @return 实时返回 true
     */
    boolean isLive();

    /**
     * 已交付给 handler 的记录条数（不含被丢弃的）。
     *
     * @return 条数
     */
    long deliveredRecords();

    /**
     * 因 {@code DROP_TO_LATEST} 被丢弃的记录条数。
     *
     * @return 条数；未发生过丢弃返回 0
     */
    long droppedRecords();

    /**
     * 是否已关闭（正常关闭或溢出断开）。
     *
     * @return 已关闭返回 true
     */
    boolean isClosed();

    /**
     * 关闭订阅：停止回调、等待线程退出（超时也会返回，不抛异常）。
     *
     * <p>幂等。
     */
    @Override
    void close();
}
