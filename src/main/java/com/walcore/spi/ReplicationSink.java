package com.walcore.spi;

/**
 * ⑤ 复制 / 订阅 SPI：把已写入的记录投递给外部副本（异地复制、下游索引、审计流）。
 *
 * <h2>与 Tailer 的分工（别用错）</h2>
 * <ul>
 *   <li>{@code WalTailer}（{@code api} 包）：<b>应用层</b>订阅，独立线程、可落后、
 *       有溢出策略，适合"业务要消费日志"；</li>
 *   <li>{@code ReplicationSink}（本接口）：<b>框架层</b>复制钩子，在写入路径上被调用，
 *       适合"要把日志同步到别处"，且能参与 {@code SEMI_SYNC} 的持久确认。</li>
 * </ul>
 *
 * <h2>实现约束</h2>
 * {@link #onRecord} 与 {@link #onPartitionAdded}/{@link #onPartitionRemoved} 可能被
 * <b>多个写线程并发</b>调用（不同分区），实现必须自行保证线程安全；
 * 且不得阻塞超过 100µs——重活（网络发送、批量提交）必须自己做异步化，
 * 否则直接拖垮 {@code append()} 的 p99。
 */
public interface ReplicationSink extends WalPlugin {

    /**
     * 复制模式。
     */
    enum Mode {

        /**
         * 异步：写入即返回，复制在后台追赶。吞吐最好，故障时可能丢最后一段。
         */
        ASYNC,

        /**
         * 半同步：{@code flushAndWait} 除等本地 {@code force} 外，还等
         * {@link #replicatedLsn(int)} 追上目标 LSN。吞吐取决于副本 RTT。
         */
        SEMI_SYNC,

        /**
         * 同步：每条记录都要副本确认后才算写入完成。最安全、最慢。
         */
        SYNC
    }

    /**
     * 复制模式。
     *
     * @return 模式
     */
    Mode mode();

    /**
     * 新分区出现（启动时恢复出的既有分区也会逐条回调一次）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 该分区首条记录 LSN
     */
    void onPartitionAdded(int partitionId, long firstLsn);

    /**
     * 分区被停用/移除。
     *
     * @param partitionId 分区 ID
     * @param lastLsn 该分区末条记录 LSN
     */
    void onPartitionRemoved(int partitionId, long lastLsn);

    /**
     * 收到一条已写入的记录（ASYNC 模式下不保证已持久化）。
     *
     * @param record 只读视图，<b>仅在本次调用期间有效</b>
     */
    void onRecord(WalRecordView record);

    /**
     * 已确认复制到的最大 LSN（{@code SEMI_SYNC} 模式下框架据此放行 {@code flushAndWait}）。
     *
     * @param partitionId 分区 ID
     * @return LSN；未知返回 0
     */
    long replicatedLsn(int partitionId);

    /**
     * 分区发生尾部截断（恢复期撕裂写截断）的通知。默认空实现。
     *
     * @param partitionId 分区 ID
     * @param fromLsn 从该 LSN 起的记录已被截断
     */
    default void onTruncated(int partitionId, long fromLsn) {
        // 默认忽略
    }
}
