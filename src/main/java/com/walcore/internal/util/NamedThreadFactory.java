package com.walcore.internal.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 统一线程命名的 {@link ThreadFactory}。
 *
 * <p>线程名形如 {@code wal-gc-0}、{@code wal-prep-0}、{@code wal-retention-0}。
 * 统一前缀的目的很实际：线上用 {@code jstack} 抓现场时，能一眼看出哪些线程属于 WAL，
 * 以及组提交线程是否卡在 {@code fdatasync} 上。
 *
 * <p>默认创建<b>非守护</b>线程：WAL 的组提交线程必须在 JVM 退出前被显式 {@code close()}，
 * 用守护线程会掩盖"忘记 close"的 bug。测试与纯后台辅助线程可显式要求守护。
 */
public final class NamedThreadFactory implements ThreadFactory {

    private final String prefix;
    private final boolean daemon;
    private final int priority;
    private final AtomicInteger sequence = new AtomicInteger();
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    /**
     * 构造线程工厂。
     *
     * @param prefix 线程名前缀，非空
     * @param daemon 是否守护线程
     */
    public NamedThreadFactory(String prefix, boolean daemon) {
        this(prefix, daemon, Thread.NORM_PRIORITY, null);
    }

    /**
     * 构造线程工厂。
     *
     * @param prefix 线程名前缀，非空
     * @param daemon 是否守护线程
     * @param priority 线程优先级，取值 1..10
     * @param uncaughtExceptionHandler 未捕获异常处理器，可为 null
     */
    public NamedThreadFactory(String prefix, boolean daemon, int priority,
                              Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        Preconditions.checkNotNull(prefix, "prefix");
        Preconditions.checkArgument(!prefix.isEmpty(), "prefix 不得为空字符串");
        Preconditions.checkInRange(priority, Thread.MIN_PRIORITY, Thread.MAX_PRIORITY, "priority");
        this.prefix = prefix;
        this.daemon = daemon;
        this.priority = priority;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Preconditions.checkNotNull(runnable, "runnable");
        Thread thread = new Thread(runnable, prefix + "-" + sequence.getAndIncrement());
        thread.setDaemon(daemon);
        thread.setPriority(priority);
        if (uncaughtExceptionHandler != null) {
            thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        return thread;
    }

    /**
     * 已创建线程数，用于测试断言线程无泄漏。
     *
     * @return 计数
     */
    public int createdCount() {
        return sequence.get();
    }

    /**
     * 线程名前缀。
     *
     * @return 前缀
     */
    public String prefix() {
        return prefix;
    }
}
