package com.walcore.internal.append;

import com.walcore.internal.util.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局 LSN 分配器：<b>单一 {@link AtomicLong} 计数器，从 1 开始，全局唯一、严格单调、无空洞</b>。
 *
 * <h2>为什么不用"拼接式 LSN"或"分段预分配"</h2>
 * <ul>
 *   <li>{@code (epoch<<52)|(partition<<40)|localSeq}：跨分区不再全局有序，恢复期无法按
 *       LSN 全局升序归并重放（P0-05 ④ 直接失败）。</li>
 *   <li>每分区一个计数器 + 全局取 max 校准：会产生空洞，且重启后可能重复。P0-01 ① 失败。</li>
 *   <li>分段预分配（一次 CAS 拿 64 个）：未写完时就是<b>临时空洞</b>，崩溃后变成
 *       <b>永久空洞</b>。P0-05 ④ 失败。</li>
 * </ul>
 * 争用成本可接受：1M TPS × 一次 {@code lock xadd}（约 100ns）= 0.1 核，
 * 在"1M TPS ≤ 1 核"的预算里占 10%（ARCH §5.2）。
 *
 * <h2>关键使用纪律（TASKS 铁律 5）</h2>
 * {@link #allocate()} <b>必须</b>在分区写锁内、紧邻写帧之后调用。这是
 * "同分区文件顺序 == LSN 顺序"这条关键不变量的唯一保证。出了锁再分配，
 * P0-01 ① 会以"偶发读到 LSN 逆序"的方式失败，且极难 debug。
 *
 * <h2>初值注入</h2>
 * 构造时注入 {@code firstLsn}（{@code RecoveryManager} 完成后由 {@code Wal.open()} 传入
 * {@code max(所有分区 lastLsn) + 1}）。故意<b>不提供</b>运行期 {@code set()}，
 * 只提供受控的 {@link #rewind(long)}（恢复期回退用）——避免运行期被误改而破坏单调性。
 */
public final class LsnAllocator {

    /** 默认起始 LSN。 */
    public static final long DEFAULT_FIRST_LSN = 1L;

    private final AtomicLong next;

    /** 构造时注入的起始 LSN，仅用于 {@link #allocated()} 的观测口径。 */
    private final long firstLsn;

    /**
     * 构造分配器，从 1 开始。
     */
    public LsnAllocator() {
        this(DEFAULT_FIRST_LSN);
    }

    /**
     * 构造分配器并注入初值。
     *
     * @param firstLsn 第一个将返回的 LSN，必须 &gt;= 1
     */
    public LsnAllocator(long firstLsn) {
        Preconditions.checkArgument(firstLsn >= 1L, "firstLsn 必须 >= 1，实际 = %s", firstLsn);
        this.next = new AtomicLong(firstLsn);
        this.firstLsn = firstLsn;
    }

    /**
     * 分配下一个 LSN。
     *
     * <p>无分配、无锁（一次 {@code getAndIncrement}），可在 append 热路径上调用。
     *
     * @return 分配到的 LSN，严格递增
     * @throws IllegalStateException LSN 计数器溢出（理论上 1M TPS 需 29 万年才会发生，
     *                               一旦发生说明数据已错乱，抛异常优于静默回绕）
     */
    public long allocate() {
        long value = next.getAndIncrement();
        if (value <= 0L) {
            throw new IllegalStateException(
                    "LSN 计数器溢出（当前值 " + value + "），log 已不可继续使用");
        }
        return value;
    }

    /**
     * 查看下一个将分配的 LSN，<b>不消耗</b>。
     *
     * <p>用途：{@code addPartition} 时给新分区的首段命名；{@code NextSegmentPreparer}
     * 给预建段命名。二者都不得真的占用 LSN（否则产生空洞）。
     *
     * @return 下一个 LSN
     */
    public long peek() {
        return next.get();
    }

    /**
     * 回退计数器（<b>仅恢复期使用，运行期严禁调用</b>）。
     *
     * <h2>为什么运行期（尤其是写失败路径）不能调用本方法（F1 修复的契约化）</h2>
     * 分区写锁只串行化<b>本分区</b>的写入：其他分区的写线程可以在此刻持各自的锁继续
     * {@link #allocate()}。若一个写失败的线程在此时把 {@code next} 回退到 {@code lsn}，
     * 而 <b>其他分区已经把比它大的 LSN 写进了文件</b>，那么后续的 allocate 会重新
     * 发出与已落盘记录相同的 LSN——全局唯一性被破坏，恢复期的按 LSN 归并重放直接错乱。
     * <b>重复比空洞危害更大</b>：空洞只会被恢复期的 {@code max(lastLsn)+1} 校正吸收，
     * 重复没有任何自动修复手段。
     *
     * <p>因此本方法只允许两个调用点：
     * <ol>
     *   <li>恢复期收尾（{@code RecoveryManager} 完成、{@code Wal.open()} 单线程阶段，
     *       此刻不存在并发写入）；</li>
     *   <li>测试代码中无并发写入的受控场景。</li>
     * </ol>
     * 写失败路径的正确做法是<b>接受 LSN 空洞</b>（见 {@code AppendPipeline.append}
     * 的失败分支注释）。
     *
     * @param lsn 新的下一个 LSN
     * @throws IllegalArgumentException 当 {@code lsn < 1}
     */
    public void rewind(long lsn) {
        Preconditions.checkArgument(lsn >= 1L, "rewind 目标必须 >= 1，实际 = %s", lsn);
        next.set(lsn);
    }

    /**
     * 计算 {@code peek() + delta}，带溢出保护。
     *
     * <p>读取路径上有大量 {@code lsn + k} 运算，统一用 {@link Math#addExact} 包裹：
     * 溢出即说明数据错乱，抛异常优于静默回绕（ARCH §5.2）。
     *
     * @param delta 偏移量
     * @return {@code peek() + delta}
     * @throws ArithmeticException 溢出
     */
    public long peekAfter(long delta) {
        return Math.addExact(peek(), delta);
    }

    /**
     * 已分配的 LSN 个数（用于统计与测试断言）。
     *
     * <p>口径：{@code peek() - 构造时注入的 firstLsn}。<b>注意</b>：恢复期调用
     * {@link #rewind(long)} 之后该值会变小（它衡量的是"距当前游标还有多远"，
     * 不是"进程生命周期内分配了多少"）——这一点在监控里要小心解读，
     * 因此指标发布用的是 {@code wal.append.records} 计数器而不是本方法。
     *
     * @return 个数
     */
    public long allocated() {
        return peek() - firstLsn;
    }

    /**
     * 构造时注入的起始 LSN。
     *
     * @return 起始 LSN
     */
    public long firstLsn() {
        return firstLsn;
    }

    @Override
    public String toString() {
        return "LsnAllocator{next=" + peek() + '}';
    }
}
