package com.walcore.spi;

/**
 * ④ 保留策略 SPI：<b>只做判断，不做删除</b>。
 *
 * <h2>为什么策略不许自己删文件</h2>
 * 删除一个段不是一次 {@code Files.delete}：必须先确认没有活跃游标/订阅在读它
 * （{@code readerCount == 0}）、必须先 {@code unmap}（Windows 上否则删不掉）、
 * 还要同步清掉 {@code .idx} 与内存里的查找结构。这些顺序一旦出错就是
 * "数据看起来在、实际读不出来"或"文件删不掉、磁盘慢慢满"。
 * 因此统一由 {@code RetentionManager} 执行，策略只返回
 * {@link RetentionDecision}。
 *
 * <h2>实现约束</h2>
 * <ul>
 *   <li>{@link #evaluate} 在<b>保留线程</b>上被周期调用（默认 30s 一次），可以慢，
 *       但不得阻塞超过秒级；</li>
 *   <li>必须<b>只读</b>：不得修改文件、不得改变段状态；</li>
 *   <li>必须尊重 {@link RetentionContext#minPinnedLsn()}：把
 *       {@code lastLsn >= minPinnedLsn} 的段排除在外。管理器还会兜底再检查一次，
 *       但策略自己排除能让"原因"字段更准确；</li>
 *   <li>{@link #onSegmentDeleted} 在删除<b>之后</b>回调，用于更新内部统计。</li>
 * </ul>
 */
public interface RetentionPolicy extends WalPlugin {

    /**
     * 评估哪些段可删。
     *
     * @param context 事实快照，非 null
     * @return 决策，不得为 null（不删请返回 {@link RetentionDecision#keepAll()}）
     */
    RetentionDecision evaluate(RetentionContext context);

    /**
     * 某个段被实际删除后的回调。默认空实现。
     *
     * @param info 被删段的描述
     */
    default void onSegmentDeleted(SegmentInfo info) {
        // 默认无需处理
    }
}
