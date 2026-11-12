package com.walcore.internal.partition;

import com.walcore.api.PartitionInfo;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.util.Preconditions;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分区运行时：<b>非公平 {@link ReentrantLock} + 两个水位 + 单向状态机 + 活跃段引用</b>。
 *
 * <h2>为什么锁是非公平的</h2>
 * 公平锁需要维护等待队列并按顺序唤醒，平均唤醒延迟反而更高（典型相差 2~5µs）。
 * WAL 的写线程是"短临界区 + 高频"的形态，非公平锁让刚释放锁的线程有机会直接重入
 * （典型临界区 ≈ 0.5µs），实测抖动更小（ARCH §3.2）。
 *
 * <h2>两个水位为什么用 VarHandle 而不是 volatile</h2>
 * <ul>
 *   <li>{@code publishedLsn}：写入线程 {@code setRelease}，读取方 {@code getAcquire}。
 *       不需要全栅栏（{@code volatile} 的语义），release/acquire 在 x86 上零指令、在 ARM 上
 *       也只用轻量屏障，避免了每次 append 一次的 {@code dmb ish}。</li>
 *   <li>{@code durableLsn}：组提交线程单写、多读。写方用 {@code setRelease} 保证
 *       "先刷盘、后发布水位"的顺序不被重排；读方 {@code getAcquire} 保证读到水位后
 *       能看到对应的数据。</li>
 * </ul>
 *
 * <h2>与 ARCH 类图的差异</h2>
 * 类图里写了 {@code nextLsnAndLock() long}，语义含糊（既锁又返回 LSN，但 LSN 必须由
 * {@code LsnAllocator} 分配，且分配必须发生在锁内、写帧之前）。这里拆成显式的
 * {@link #lock()} / {@link #unlock()}，让"锁 → 分配 → 写帧 → 发布"四步在调用方一目了然。
 */
public final class PartitionImpl {

    private static final VarHandle PUBLISHED_LSN;
    private static final VarHandle DURABLE_LSN;

    static {
        try {
            PUBLISHED_LSN = MethodHandles.lookup().findVarHandle(
                    PartitionImpl.class, "publishedLsn", long.class);
            DURABLE_LSN = MethodHandles.lookup().findVarHandle(
                    PartitionImpl.class, "durableLsn", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int id;
    private final ReentrantLock writeLock;
    private final long createdMillis;

    /** 已完整写入（未持久）的最大 LSN，写方 release、读方 acquire。 */
    private long publishedLsn;

    /** 已 force 持久化的最大 LSN，组提交线程 release、读方 acquire。 */
    private long durableLsn;

    private volatile PartitionInfo.State state = PartitionInfo.State.ACTIVE;
    private volatile Segment activeSegment;
    private volatile long firstLsnOfPartition;

    /**
     * 构造分区。
     *
     * @param id 分区 ID
     */
    public PartitionImpl(int id) {
        Preconditions.checkArgument(id >= 0, "partitionId 不得为负: %s", id);
        this.id = id;
        this.writeLock = new ReentrantLock(false);
        this.createdMillis = System.currentTimeMillis();
    }

    // ------------------------------------------------------------------
    // 写锁
    // ------------------------------------------------------------------

    /**
     * 获取分区写锁（必须在 {@code try/finally} 中配 {@link #unlock()}）。
     */
    public void lock() {
        writeLock.lock();
    }

    /**
     * 尝试获取分区写锁。
     *
     * @return 成功返回 true
     */
    public boolean tryLock() {
        return writeLock.tryLock();
    }

    /**
     * 释放分区写锁。
     */
    public void unlock() {
        writeLock.unlock();
    }

    /**
     * 当前线程是否持有写锁。
     *
     * @return 持有返回 true
     */
    public boolean isHeldByCurrentThread() {
        return writeLock.isHeldByCurrentThread();
    }

    /**
     * 是否有线程在等待该锁（背压判断用）。
     *
     * @return 有等待者返回 true
     */
    public boolean hasQueuedThreads() {
        return writeLock.hasQueuedThreads();
    }

    // ------------------------------------------------------------------
    // 水位
    // ------------------------------------------------------------------

    /**
     * 已完整写入（不保证持久）的最大 LSN。
     *
     * @return LSN，未写入返回 0
     */
    public long publishedLsn() {
        return (long) PUBLISHED_LSN.getAcquire(this);
    }

    /**
     * 发布写水位（release 语义，<b>只允许前进</b>）。
     *
     * <p>为什么用 CAS 循环而不是"先 get 再 set"：后者是非原子的读-改-写，两个写者并发时
     * 可能出现 <i>X 读到 cur=100 → Y 把水位推到 200 → X 写回 101</i>，水位<b>倒退</b>。
     * 这类 bug 在生产环境表现为"偶发少读几条日志"，且几乎无法复现。
     *
     * <p>成本：无竞争时第一次 CAS 即成功，x86 上一条 {@code lock cmpxchg}（约 7ns），
     * 与 append 路径上 {@code LsnAllocator.allocate()} 的 {@code lock xadd} 同量级，
     * 可接受。正常形态下每个分区写锁内只有一个写者，所以循环几乎不会第二次迭代。
     *
     * @param lsn 新水位
     */
    public void publishLsn(long lsn) {
        long cur = (long) PUBLISHED_LSN.getAcquire(this);
        while (lsn > cur) {
            if (PUBLISHED_LSN.compareAndSet(this, cur, lsn)) {
                return;
            }
            cur = (long) PUBLISHED_LSN.getAcquire(this);
        }
    }

    /**
     * 已持久化的最大 LSN。
     *
     * @return LSN，未持久化返回 0
     */
    public long durableLsn() {
        return (long) DURABLE_LSN.getAcquire(this);
    }

    /**
     * 发布持久化水位（release 语义）。
     *
     * <p><b>只允许前进</b>：组提交线程之间可能乱序完成，若允许回退会让
     * {@code flushAndWait} 出现"已返回成功又被判为未持久"的荒谬状态。
     * 用 CAS 循环保证"比较"与"写入"之间不被其他 flusher 穿插。
     *
     * @param lsn 新水位
     */
    public void publishDurableLsn(long lsn) {
        long cur = (long) DURABLE_LSN.getAcquire(this);
        while (lsn > cur) {
            if (DURABLE_LSN.compareAndSet(this, cur, lsn)) {
                return;
            }
            cur = (long) DURABLE_LSN.getAcquire(this);
        }
    }

    // ------------------------------------------------------------------
    // 状态机
    // ------------------------------------------------------------------

    /**
     * 分区状态。
     *
     * @return 状态
     */
    public PartitionInfo.State state() {
        return state;
    }

    /**
     * 是否可写（{@code ACTIVE}）。
     *
     * @return 可写返回 true
     */
    public boolean isWritable() {
        return state == PartitionInfo.State.ACTIVE;
    }

    /**
     * 迁移状态。单向且不可逆：{@code ACTIVE -> FROZEN -> (ARCHIVED|PURGED) -> PURGED}。
     *
     * @param target 目标状态
     * @return 实际发生了迁移返回 true；已是目标状态或非法迁移返回 false
     */
    public boolean transitionTo(PartitionInfo.State target) {
        Preconditions.checkNotNull(target, "target");
        synchronized (this) {
            PartitionInfo.State cur = state;
            if (cur == target) {
                return false;
            }
            if (rank(target) <= rank(cur)) {
                return false;
            }
            state = target;
            return true;
        }
    }

    /**
     * 状态序号，用于单向性判定。
     *
     * @param s 状态
     * @return 序号
     */
    private static int rank(PartitionInfo.State s) {
        switch (s) {
            case ACTIVE:
                return 0;
            case FROZEN:
                return 1;
            case ARCHIVED:
                return 2;
            case PURGED:
                return 3;
            default:
                return 0;
        }
    }

    // ------------------------------------------------------------------
    // 活跃段
    // ------------------------------------------------------------------

    /**
     * 当前活跃段。
     *
     * @return 段；尚未创建返回 null
     */
    public Segment activeSegment() {
        return activeSegment;
    }

    /**
     * 替换活跃段（轮转时调用，调用方必须持有写锁）。
     *
     * @param segment 新活跃段
     */
    public void setActiveSegment(Segment segment) {
        this.activeSegment = segment;
        if (segment != null && firstLsnOfPartition == 0L) {
            this.firstLsnOfPartition = segment.firstLsn();
        }
    }

    /**
     * 分区内首条记录 LSN（约等于首个段的名字 LSN）。
     *
     * @return LSN，空分区返回 0
     */
    public long firstLsnOfPartition() {
        return firstLsnOfPartition;
    }

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    public int id() {
        return id;
    }

    /**
     * 创建时刻（epoch 毫秒）。
     *
     * @return 毫秒
     */
    public long createdMillis() {
        return createdMillis;
    }

    @Override
    public String toString() {
        return "PartitionImpl{id=" + id
                + ", state=" + state
                + ", publishedLsn=" + publishedLsn()
                + ", durableLsn=" + durableLsn()
                + '}';
    }
}
