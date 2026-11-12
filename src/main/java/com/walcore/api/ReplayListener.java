package com.walcore.api;

/**
 * 崩溃恢复期的重放回调（ARCH §9.1 的 {@code R}/{@code R2}/{@code S} 三个节点）。
 *
 * <p>全部方法都是 {@code default} 空实现：绝大多数使用方只关心
 * {@link #onRecord(WalRecord)}，没必要为其余三个方法写空方法体。
 *
 * <p><b>线程契约</b>：所有回调都在调用 {@code Wal.open()}/{@code wal.recover()} 的
 * <b>同一个线程</b>上串行执行，回调内部可以安全使用非线程安全的数据结构。
 *
 * <p><b>性能提示</b>：{@code onRecord} 收到的是复用实例，与
 * {@link WalRecordVisitor#accept(WalRecord)} 相同的约束——需要留存请调
 * {@link WalRecord#copy()}。框架不在这里做任何缓冲或异步化，重放速率就是回调速率。
 */
public interface ReplayListener {

    /**
     * 重放到一条记录。所有分区的记录按 <b>LSN 全局升序</b> 交错回调。
     *
     * @param record 记录视图（复用实例）
     */
    default void onRecord(WalRecord record) {
        // 默认不做事
    }

    /**
     * 进度回调，每重放 10 万条一次。
     *
     * @param partitionId 刚重放完的分区 ID
     * @param lsn 刚重放完的 LSN
     * @param replayed 累计已重放条数
     */
    default void onProgress(int partitionId, long lsn, long replayed) {
        // 默认不做事
    }

    /**
     * 发现一处损坏。
     *
     * @param event 损坏事件
     */
    default void onCorruption(RecoveryReport.CorruptionEvent event) {
        // 默认不做事
    }

    /**
     * 恢复完成。
     *
     * @param report 恢复报告
     */
    default void onRecoveryComplete(RecoveryReport report) {
        // 默认不做事
    }
}
