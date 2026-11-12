package com.walcore.api;

/**
 * 实时订阅（{@link WalTailer}）的回调接口。
 *
 * <h2>回调线程与时长约束</h2>
 * 所有回调都在<b>该订阅自己的 tailer 线程</b>上执行，<b>不在</b>写入线程上。
 * 因此：
 * <ul>
 *   <li>回调里做 IO / 睡眠<b>不会</b>拖慢 {@code append()}——这正是"慢订阅者不拖垮写入"
 *       （PRD P1-01 ④）的实现基础；</li>
 *   <li>但回调会拖慢<b>自己</b>的消费速度，进而让 {@link WalTailer#lagRecords()} 增长，
 *       最终按 {@code TailerOverflowPolicy} 处理；</li>
 *   <li>回调抛出的 {@link RuntimeException} 会被框架捕获并转交 {@link #onError(Throwable)}，
 *       随后订阅被关闭——<b>不会</b>让线程"带病继续跑"。</li>
 * </ul>
 *
 * <h2>记录的生存期（重要）</h2>
 * 传给 {@link #onRecord(WalRecord)} 的是<b>可复用</b>的记录对象（同一个实例会被下一条记录覆盖），
 * 这是"订阅零分配"的前提。若要在回调返回后继续持有内容，请自行
 * {@link WalRecord#copy()} 或 {@link WalRecord#payloadBytes()}。
 */
public interface TailHandler {

    /**
     * 收到一条记录。
     *
     * <p>记录对象在本次调用期间有效；调用返回后其内容可能被下一条记录覆盖。
     *
     * @param record 记录（只读视图，可复用）
     */
    void onRecord(WalRecord record);

    /**
     * 发生溢出且策略为 {@code DISCONNECT} 时回调，随后订阅自动关闭。
     *
     * @param cause 溢出异常，含分区 ID、落后条数、缓冲上限
     */
    default void onOverflow(WalException.TailerOverflowException cause) {
        // 默认忽略
    }

    /**
     * 订阅被关闭时回调（正常 {@code close()}、溢出断开、或回调抛异常后都会走到这里）。
     *
     * <p>保证<b>至多</b>被调用一次。可在其中释放 handler 自己持有的资源。
     */
    default void onClose() {
        // 默认忽略
    }

    /**
     * 回调处理过程中抛出的异常。
     *
     * <p>框架保证在 {@link #onClose()} 之前调用。
     *
     * @param error 异常
     */
    default void onError(Throwable error) {
        // 默认忽略
    }
}
