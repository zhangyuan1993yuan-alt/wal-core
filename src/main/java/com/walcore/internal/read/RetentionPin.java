package com.walcore.internal.read;

/**
 * 读者向保留策略声明"我还在读这个位置"的最小接口（内部协作类型，不属于交付 API）。
 *
 * <h2>为什么读端需要主动 pin，而不能只依赖 {@code readerCount}</h2>
 * {@code RetentionManager.minPinnedLsn} 的兜底逻辑是"扫一遍<b>当前段序列</b>，
 * 找第一个 {@code readerCount > 0} 的段"。这条路径有两个固有弱点：
 * <ol>
 *   <li><b>依赖段还在序列里</b>：一旦该段因任何原因被摘出序列（删除、重建、恢复重排），
 *       扫描就看不到它，兜底值退化为 0（"无人读"），于是<b>剩余所有段都可删</b>——
 *       一次误删会引发级联误删，这正是"静默丢区间"的放大器；</li>
 *   <li><b>只有段粒度、且与决策时刻不同步</b>：删除判定在轮首采样一次，读者可能在
 *       采样之后才转移到某段。</li>
 * </ol>
 * 显式 pin 是按 <b>LSN</b> 表达的围栏：只要 pin 还在，任何 {@code lastLsn >= pin} 的段
 * 都不可能被删——与"段对象是否还在序列里"完全解耦。因此游标在<b>跨段时</b>把
 * "当前段的保护点"登记进来、离开时撤销，形成一条随读位置前进的单向围栏。
 *
 * <p>登记/撤销都是低频动作（每个段一次，不是每条记录一次），
 * 底层是 {@code ConcurrentSkipListMap} 上的引用计数，代价可忽略。
 */
public interface RetentionPin {

    /** 空实现（不登记任何 pin；用于恢复重放等"没有保留策略参与"的场景）。 */
    RetentionPin NONE = new RetentionPin() {

        @Override
        public void pin(long lsn) {
            // 无保留策略参与，无需登记
        }

        @Override
        public void unpin(long lsn) {
            // 无保留策略参与，无需撤销
        }
    };

    /**
     * 登记一个不可删除的 LSN（引用计数）。
     *
     * @param lsn LSN；{@code <= 0} 应被忽略
     */
    void pin(long lsn);

    /**
     * 撤销一个登记（引用计数归零后失效）。
     *
     * @param lsn LSN；{@code <= 0} 应被忽略
     */
    void unpin(long lsn);
}
