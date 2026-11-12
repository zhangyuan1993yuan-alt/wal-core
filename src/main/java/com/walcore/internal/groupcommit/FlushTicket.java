package com.walcore.internal.groupcommit;

import com.walcore.api.WalException;
import com.walcore.internal.util.Preconditions;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 一次"等待某 LSN 落盘"的凭据：<b>4 行语义的 CountDownLatch + 线程本地池</b>。
 *
 * <h2>为什么不用 {@code CountDownLatch} / {@code CompletableFuture}</h2>
 * <ul>
 *   <li>{@code CountDownLatch} 内部是 AQS：每创建一个都要初始化 state、Node、条件队列，
 *       约 100ns + 至少 1 个对象。{@code flushAndWait} 在高频 ACK 场景下每秒几十万次调用，
 *       这个分配量会把 young GC 从"每小时"变成"每分钟"。</li>
 *   <li>{@code CompletableFuture} 更重（依赖链、UniApply 等），且异常语义过宽。</li>
 *   <li>本类只需要"一个 int 状态 + 一次 {@code unpark}"，AQS 的能力全用不上。</li>
 * </ul>
 *
 * <h2>池化为什么是安全的</h2>
 * 每个线程一条 {@link ArrayDeque} 栈，{@code acquire} 出栈、{@link #close()} 入栈。
 * 复用前 {@link #reset()} 把状态与失败原因清空。因为 {@code await()} 返回后调用方
 * <b>已不再持有</b>该 ticket（{@code try-with-resources} 保证），所以不会出现
 * "旧 ticket 被 signal 到新等待者"的串号问题。
 *
 * <h2>状态机</h2>
 * {@code WAITING -> DONE}（正常）或 {@code WAITING -> FAILED}（force 失败）。
 * 终态不可逆；重复 {@link #signal()} / {@link #fail(Throwable)} 是幂等的。
 */
public final class FlushTicket implements AutoCloseable {

    /** 等待中。 */
    private static final int WAITING = 0;

    /** 已完成（durableLsn 已推进到 targetLsn 之上）。 */
    private static final int DONE = 1;

    /** 失败（force 抛异常，durableLsn <b>未</b>推进）。 */
    private static final int FAILED = 2;

    /** 每个线程的复用栈上限（超过就丢弃，避免长期持有大量对象）。 */
    private static final int POOL_LIMIT = 8;

    private static final ThreadLocal<ArrayDeque<FlushTicket>> POOL =
            ThreadLocal.withInitial(ArrayDeque::new);

    /** 创建计数（测试用于证明"池化生效，没有每次都 new"）。 */
    private static final AtomicLong CREATED = new AtomicLong();

    private volatile int state = WAITING;
    private volatile Throwable failure;
    private volatile Thread waiter;
    private int partitionId;
    private long targetLsn;

    private FlushTicket() {
        CREATED.incrementAndGet();
    }

    /**
     * 取一个 ticket（优先复用线程本地池中的实例）。
     *
     * @param partitionId 分区 ID
     * @param targetLsn 期望落盘的 LSN
     * @return ticket（用完必须 {@link #close()} 归还）
     */
    public static FlushTicket acquire(int partitionId, long targetLsn) {
        ArrayDeque<FlushTicket> pool = POOL.get();
        FlushTicket t = pool.pollFirst();
        if (t == null) {
            t = new FlushTicket();
        }
        t.reset();
        t.partitionId = partitionId;
        t.targetLsn = targetLsn;
        return t;
    }

    /**
     * 清空状态（复用时调用）。
     */
    private void reset() {
        state = WAITING;
        failure = null;
        waiter = null;
        partitionId = -1;
        targetLsn = 0L;
    }

    /**
     * 已完成：唤醒等待者。
     */
    public void signal() {
        if (state == WAITING) {
            state = DONE;
        }
        Thread t = waiter;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * 失败：唤醒等待者并让其抛出 IO 异常。
     *
     * @param cause 失败原因
     */
    public void fail(Throwable cause) {
        if (state == WAITING) {
            state = FAILED;
            failure = cause;
        }
        Thread t = waiter;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * 阻塞等待完成。
     *
     * <p>用 {@code parkNanos} 而非 {@code park}：即使 {@code unpark} 因任何原因丢失，
     * 等待者也会在超时后醒来检查状态，绝不会永久挂死（"宁可超时，不可假死"）。
     *
     * @param timeoutMillis 超时（毫秒）
     * @throws WalException.IoException 刷盘失败
     * @throws WalException.TimeoutException 超时
     */
    public void await(long timeoutMillis) {
        Preconditions.checkArgument(timeoutMillis >= 0L, "timeoutMillis 不得为负");
        if (state != WAITING) {
            checkFailure();
            return;
        }
        waiter = Thread.currentThread();
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        try {
            while (state == WAITING) {
                long remain = deadline - System.nanoTime();
                if (remain <= 0L) {
                    break;
                }
                LockSupport.parkNanos(this, remain);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    throw new WalException.TimeoutException(
                            "等待 LSN=" + targetLsn + " 落盘时被中断（分区 " + partitionId + "）");
                }
            }
        } finally {
            waiter = null;
        }
        if (state == WAITING) {
            throw new WalException.TimeoutException(
                    "等待 LSN=" + targetLsn + " 落盘超时（分区 " + partitionId
                            + "，timeout=" + timeoutMillis + "ms）");
        }
        checkFailure();
    }

    /**
     * 若已失败则抛出。
     */
    private void checkFailure() {
        if (state == FAILED) {
            Throwable cause = failure;
            throw new WalException.IoException(
                    "刷盘失败，LSN=" + targetLsn + " 未持久化（分区 " + partitionId + "）",
                    cause == null ? new java.io.IOException("force 失败") : cause);
        }
    }

    /**
     * 是否已完成（含失败）。
     *
     * @return 已完成返回 true
     */
    public boolean isDone() {
        return state != WAITING;
    }

    /**
     * 是否成功完成。
     *
     * @return 成功返回 true
     */
    public boolean isSuccess() {
        return state == DONE;
    }

    /**
     * 失败原因。
     *
     * @return 原因；成功或仍在等待返回 null
     */
    public Throwable failure() {
        return failure;
    }

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * 目标 LSN。
     *
     * @return LSN
     */
    public long targetLsn() {
        return targetLsn;
    }

    /**
     * 归还到线程本地池。
     *
     * <p><b>只在"已到达终态"时回收</b>。若 {@code await()} 超时返回时仍是
     * {@code WAITING}，说明组提交线程手里可能还留着这个 ticket 的引用：
     * 一旦回收复用，迟到的 {@code signal()} 就会把"新等待者"直接判为已完成
     * ——那是<b>假持久性</b>，比丢一个对象严重一万倍。所以超时路径直接丢弃（1 个对象而已）。
     */
    @Override
    public void close() {
        if (waiter != null || state == WAITING) {
            return;
        }
        reset();
        ArrayDeque<FlushTicket> pool = POOL.get();
        if (pool.size() < POOL_LIMIT) {
            pool.addFirst(this);
        }
    }

    /**
     * 累计创建的 ticket 实例数（池化有效时应远小于调用次数）。
     *
     * @return 数量
     */
    public static long createdCount() {
        return CREATED.get();
    }

    /**
     * 重置创建计数（仅测试用）。
     */
    public static void resetCreatedCount() {
        CREATED.set(0L);
    }

    @Override
    public String toString() {
        return "FlushTicket{pid=" + partitionId + ", targetLsn=" + targetLsn
                + ", state=" + (state == WAITING ? "WAITING" : state == DONE ? "DONE" : "FAILED") + '}';
    }
}
