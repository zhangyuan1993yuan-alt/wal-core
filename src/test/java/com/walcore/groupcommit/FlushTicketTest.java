package com.walcore.groupcommit;

import com.walcore.api.WalException;
import com.walcore.internal.groupcommit.FlushTicket;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T07 验收之一：<b>FlushTicket 的唤醒语义与池化</b>。
 *
 * <p>三条容易被忽略但一旦出错就致命的性质：
 * <ol>
 *   <li><b>不丢唤醒</b>：100 万次 signal/await 交错，不得有一次挂到超时。
 *       丢一次唤醒在生产上表现为"偶发 flushAndWait 超时 5s"，几乎无法复现。</li>
 *   <li><b>超时的 ticket 绝不回收</b>：否则迟到的 {@code signal} 会把下一个等待者
 *       直接判为"已持久"——那是假持久性。</li>
 *   <li><b>池化有效</b>：1 万次 acquire/close 只应创建个位数实例（否则每次 ACK 都在分配）。</li>
 * </ol>
 */
class FlushTicketTest {

    @BeforeEach
    void resetPool() {
        FlushTicket.resetCreatedCount();
    }

    @Test
    @DisplayName("signal 后 await 立即返回，且 isSuccess")
    void signalThenAwaitReturns() {
        try (FlushTicket t = FlushTicket.acquire(3, 42L)) {
            assertEquals(3, t.partitionId());
            assertEquals(42L, t.targetLsn());
            assertFalse(t.isDone(), "刚 acquire 的 ticket 必须处于等待态");
            t.signal();
            assertTrue(t.isDone());
            assertTrue(t.isSuccess());
            long t0 = System.nanoTime();
            t.await(1_000L);
            long elapsedMicros = (System.nanoTime() - t0) / 1000L;
            assertTrue(elapsedMicros < 1_000L,
                    "已完成时 await 必须立即返回，实际耗时 " + elapsedMicros + "µs");
        }
    }

    @Test
    @DisplayName("未 signal 时 await 超时并抛 TimeoutException（不永久挂死）")
    void awaitTimesOut() {
        FlushTicket t = FlushTicket.acquire(0, 10L);
        try {
            long t0 = System.nanoTime();
            assertThrows(WalException.TimeoutException.class, () -> t.await(50L));
            long elapsedMillis = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue(elapsedMillis >= 40L && elapsedMillis < 2_000L,
                    "超时时间应接近 50ms，实际 " + elapsedMillis + "ms");
            assertFalse(t.isDone());
        } finally {
            t.close();
        }
    }

    @Test
    @DisplayName("fail 后 await 抛 IoException，且 failure() 保留原始原因")
    void failThenAwaitThrowsIoException() {
        FlushTicket t = FlushTicket.acquire(7, 99L);
        try {
            IllegalStateException cause = new IllegalStateException("磁盘满");
            t.fail(cause);
            assertTrue(t.isDone());
            assertFalse(t.isSuccess());
            assertEquals(cause, t.failure());
            WalException.IoException e = assertThrows(WalException.IoException.class,
                    () -> t.await(100L));
            assertEquals("WAL_IO", e.errorCode());
            assertTrue(e.getMessage().contains("99"), "异常消息应包含目标 LSN: " + e.getMessage());
        } finally {
            t.close();
        }
    }

    @Test
    @DisplayName("signal/fail 幂等，重复调用不改变终态")
    void terminalStateIsSticky() {
        FlushTicket t = FlushTicket.acquire(0, 1L);
        try {
            t.signal();
            t.signal();
            t.fail(new RuntimeException("迟到的失败"));
            assertTrue(t.isSuccess(), "已成功的 ticket 不能被迟到的 fail 改写");
            assertEquals(null, t.failure());
        } finally {
            t.close();
        }

        FlushTicket t2 = FlushTicket.acquire(0, 1L);
        try {
            t2.fail(new RuntimeException("先失败"));
            t2.signal();
            assertFalse(t2.isSuccess(), "已失败的 ticket 不能被迟到的 signal 改写");
        } finally {
            t2.close();
        }
    }

    @Test
    @DisplayName("池化：1 万次 acquire/close 只创建个位数实例")
    void poolingAvoidsAllocation() {
        for (int i = 0; i < 10_000; i++) {
            try (FlushTicket t = FlushTicket.acquire(0, i)) {
                t.signal();
                t.await(0L);
            }
        }
        long created = FlushTicket.createdCount();
        assertTrue(created <= 8,
                "线程本地池容量为 8，1 万次复用只应创建 <= 8 个实例，实际 " + created);
    }

    @Test
    @DisplayName("超时未完成的 ticket 不回收（防止迟到 signal 串号）")
    void timedOutTicketIsNotRecycled() {
        FlushTicket t1 = FlushTicket.acquire(0, 1L);
        assertThrows(WalException.TimeoutException.class, () -> t1.await(10L));
        long before = FlushTicket.createdCount();
        t1.close(); // 状态仍是 WAITING → 必须丢弃而不是入池
        FlushTicket t2 = FlushTicket.acquire(0, 2L);
        long after = FlushTicket.createdCount();
        assertEquals(before + 1L, after,
                "超时的 ticket 不得被复用（否则迟到 signal 会让新等待者假确认）");
        // 更关键的行为断言：即使迟到的 signal 打到了 t1，t2 也不能因此完成
        t1.signal();
        assertFalse(t2.isDone(), "t2 不得因为 t1 的迟到 signal 而变成已完成");
        t2.close();
    }

    @Test
    @DisplayName("5 万次跨线程 signal/await：零丢失唤醒")
    void noLostWakeupsUnderContention() throws Exception {
        final int rounds = 50_000;
        Thread consumer = new Thread(() -> {
            for (int i = 0; i < rounds; i++) {
                Feed.awaitTick(i, 2_000L);
            }
        }, "ticket-waiter");
        consumer.start();
        Thread producer = new Thread(() -> {
            for (int i = 0; i < rounds; i++) {
                Feed.signalTick(i);
            }
        }, "ticket-signaller");
        producer.start();
        producer.join(60_000);
        consumer.join(60_000);
        assertFalse(producer.isAlive(), "signal 线程未在 60s 内完成");
        assertFalse(consumer.isAlive(),
                "await 线程未在 60s 内完成，说明存在丢失唤醒（每次丢都要等满 2s 超时）");
        assertEquals(rounds, Feed.completed(), "每一轮都必须成功完成");
    }

    @Test
    @DisplayName("await(0) 在未完成时立即超时，不做任何 park")
    void zeroTimeoutReturnsImmediately() {
        FlushTicket t = FlushTicket.acquire(0, 1L);
        try {
            long t0 = System.nanoTime();
            assertThrows(WalException.TimeoutException.class, () -> t.await(0L));
            assertTrue((System.nanoTime() - t0) < 50_000_000L);
        } finally {
            t.close();
        }
    }

    @Test
    @DisplayName("acquire 会重置复用实例的状态")
    void acquireResetsState() {
        FlushTicket t = FlushTicket.acquire(5, 500L);
        t.signal();
        t.close();
        FlushTicket again = FlushTicket.acquire(6, 600L);
        assertEquals(6, again.partitionId());
        assertEquals(600L, again.targetLsn());
        assertFalse(again.isDone(), "复用实例必须回到等待态");
        assertEquals(null, again.failure());
        again.close();
    }

    @Test
    @DisplayName("超时参数为负时立即拒绝")
    void rejectsNegativeTimeout() {
        try (FlushTicket t = FlushTicket.acquire(0, 1L)) {
            assertThrows(IllegalArgumentException.class, () -> t.await(-1L));
        }
    }

    /**
     * 有界单槽信箱：生产者放 ticket 后立即 signal，消费者取出后 await。
     *
     * <p>注意这里<b>故意不保证</b>"消费者先进入 await、生产者再 signal"的顺序——
     * 因为真实场景也不保证。这正是 {@code FlushTicket} 必须在 {@code await} 入口
     * 先检查终态的原因：signal 可能先到。
     */
    private static final class Feed {

        private static final int CAPACITY = 64;
        private static final FlushTicket[] SLOTS = new FlushTicket[CAPACITY];
        private static final AtomicInteger DONE = new AtomicInteger();
        private static final Object LOCK = new Object();

        static void awaitTick(int i, long timeoutMillis) {
            int slot = i % CAPACITY;
            FlushTicket t = take(slot);
            try {
                t.await(timeoutMillis);
                if (!t.isSuccess()) {
                    throw new AssertionError("ticket " + i + " 被判定失败");
                }
                DONE.incrementAndGet();
            } finally {
                t.close();
            }
        }

        static void signalTick(int i) {
            int slot = i % CAPACITY;
            FlushTicket t = FlushTicket.acquire(0, i);
            put(slot, t);
            t.signal();
        }

        static int completed() {
            return DONE.get();
        }

        private static FlushTicket take(int slot) {
            synchronized (LOCK) {
                while (SLOTS[slot] == null) {
                    try {
                        LOCK.wait(1_000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("等待槽位被中断");
                    }
                }
                FlushTicket t = SLOTS[slot];
                SLOTS[slot] = null;
                LOCK.notifyAll();
                return t;
            }
        }

        private static void put(int slot, FlushTicket t) {
            synchronized (LOCK) {
                while (SLOTS[slot] != null) {
                    try {
                        LOCK.wait(1_000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("放入槽位被中断");
                    }
                }
                SLOTS[slot] = t;
                LOCK.notifyAll();
            }
        }

        private Feed() {
        }
    }

    @Test
    @DisplayName("await 中收到中断：抛 TimeoutException 并保留中断标记")
    void interruptSurfacesAsTimeout() throws Exception {
        FlushTicket t = FlushTicket.acquire(0, 1L);
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch inAwait = new CountDownLatch(1);
        Thread th = new Thread(() -> {
            try {
                inAwait.countDown();
                t.await(10_000L);
            } catch (Throwable e) {
                caught.set(e);
            }
        }, "ticket-interrupt");
        th.start();
        assertTrue(inAwait.await(5, TimeUnit.SECONDS));
        Thread.sleep(20L);
        th.interrupt();
        th.join(5_000);
        assertFalse(th.isAlive(), "被中断的 await 必须退出");
        assertTrue(caught.get() instanceof WalException.TimeoutException,
                "中断应表现为 TimeoutException，实际: " + caught.get());
        t.close();
    }
}
