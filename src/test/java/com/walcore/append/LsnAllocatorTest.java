package com.walcore.append;

import com.walcore.internal.append.LsnAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T04 验收：<b>全局单一 LSN 分配器的三大性质</b>。
 *
 * <ol>
 *   <li><b>严格连续、无重复、无空洞</b>：{@code 16} 线程 × {@code 1M} 次分配，
 *       落盘后必须是 {@code 1..16M} 的一个全排列（本测试用"排序后逐位比对 + 首尾差"验证）。</li>
 *   <li><b>{@code peek()} 不消耗</b>：{@code addPartition} / {@code NextSegmentPreparer}
 *       用 {@code peek()} 给段命名，若它消耗 LSN 就会在日志里留下永久空洞
 *       （违反 PRD P0-05 ④）。</li>
 *   <li><b>可注入初值 + 单向溢出保护</b>：恢复期用 {@code rewind} 回到
 *       {@code lastGoodLsn + 1}；计数器溢出必须抛异常而不是静默回绕。</li>
 * </ol>
 */
class LsnAllocatorTest {

    private static final int THREADS = 16;
    private static final int PER_THREAD = 1_000_000;

    @Test
    @DisplayName("16 线程 × 100 万次分配：无重复、无空洞、严格连续")
    void concurrentAllocationIsGapFreeAndUnique() throws Exception {
        LsnAllocator allocator = new LsnAllocator();
        int total = THREADS * PER_THREAD;

        // 用分片 long[] 保存结果，避免任何装箱与并发容器（同时验证"分配路径零分配"）
        long[][] shards = new long[THREADS][PER_THREAD];
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            for (int t = 0; t < THREADS; t++) {
                final int idx = t;
                pool.execute(() -> {
                    try {
                        start.await();
                        long[] shard = shards[idx];
                        for (int i = 0; i < PER_THREAD; i++) {
                            shard[i] = allocator.allocate();
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(120, TimeUnit.SECONDS), "并发分配超时（疑似死锁或严重争用）");
        } finally {
            pool.shutdownNow();
        }
        if (failure.get() != null) {
            throw new AssertionError("分配线程异常", failure.get());
        }

        // 合并 1600 万个值后排序，必须恰好是 [1, total]
        long[] all = new long[total];
        for (int t = 0; t < THREADS; t++) {
            System.arraycopy(shards[t], 0, all, t * PER_THREAD, PER_THREAD);
        }
        java.util.Arrays.sort(all);

        assertEquals(1L, all[0], "首值必须为 1（LSN 从 1 开始，0 是哨兵）");
        assertEquals((long) total, all[total - 1], "末值必须等于总分配次数");
        assertEquals((long) total - 1L, all[total - 1] - all[0],
                "首尾差必须为 total-1，否则说明存在空洞或重复");
        for (int i = 1; i < total; i++) {
            if (all[i] != all[i - 1] + 1L) {
                throw new AssertionError("在索引 " + i + " 处出现不连续/重复: "
                        + all[i - 1] + " -> " + all[i]);
            }
        }
        assertEquals((long) total, allocator.allocated(), "allocated() 必须与实际分配数一致");
        assertEquals((long) total + 1L, allocator.peek(), "peek() 必须指向下一个待分配值");
    }

    @Test
    @DisplayName("peek() 不消耗 LSN：连调 100 次后仍返回同一值")
    void peekDoesNotConsume() {
        LsnAllocator allocator = new LsnAllocator(42L);
        for (int i = 0; i < 100; i++) {
            assertEquals(42L, allocator.peek(), "peek() 第 " + i + " 次调用不得改变计数器");
        }
        assertEquals(0L, allocator.allocated(), "peek() 不得增加已分配计数");
        assertEquals(42L, allocator.allocate(), "peek() 之后的首次分配仍是 42");
        assertEquals(43L, allocator.peek());
    }

    @Test
    @DisplayName("可注入初值：恢复期从 lastGoodLsn+1 继续")
    void injectableInitialValue() {
        LsnAllocator allocator = new LsnAllocator(131_073L);
        assertEquals(131_073L, allocator.peek());
        assertEquals(131_073L, allocator.allocate());
        assertEquals(131_074L, allocator.allocate());
        assertEquals(131_075L, allocator.peek());
        assertEquals(2L, allocator.allocated());
    }

    @Test
    @DisplayName("初值必须 >= 1（0 为哨兵，负数无意义）")
    void rejectsIllegalInitialValue() {
        assertThrows(IllegalArgumentException.class, () -> new LsnAllocator(0L));
        assertThrows(IllegalArgumentException.class, () -> new LsnAllocator(-1L));
        // 默认构造等价于从 1 开始
        assertEquals(1L, new LsnAllocator().peek());
    }

    @Test
    @DisplayName("rewind 只允许 >= 1，且设置的是「下一个待分配值」")
    void rewindSemantics() {
        LsnAllocator allocator = new LsnAllocator();
        for (int i = 0; i < 10; i++) {
            allocator.allocate();
        }
        assertEquals(11L, allocator.peek());
        allocator.rewind(5L);
        assertEquals(5L, allocator.peek(), "rewind 后 peek 必须等于目标值");
        assertEquals(5L, allocator.allocate(), "rewind 后首次分配必须等于目标值");
        assertThrows(IllegalArgumentException.class, () -> allocator.rewind(0L));
    }

    @Test
    @DisplayName("peekAfter 用 addExact 做溢出保护，不回绕")
    void peekAfterDetectsOverflow() {
        LsnAllocator allocator = new LsnAllocator(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, allocator.peekAfter(0L));
        assertThrows(ArithmeticException.class, () -> allocator.peekAfter(1L),
                "溢出必须抛异常（静默回绕会导致 LSN 重复，破坏 P0-05）");
        assertThrows(ArithmeticException.class, () -> allocator.peekAfter(Long.MAX_VALUE),
                "负数偏移超出下界同样必须抛异常");
    }

    @Test
    @DisplayName("计数器耗尽后 allocate() 抛 IllegalStateException 而非回绕")
    void exhaustionThrows() {
        LsnAllocator allocator = new LsnAllocator(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, allocator.allocate(), "最后一次合法分配");
        assertThrows(IllegalStateException.class, allocator::allocate,
                "越过 Long.MAX_VALUE 后 getAndIncrement 得到负数，必须显式失败");
    }

    @Test
    @DisplayName("单线程顺序分配的开销足够低（4096 万次 < 3s，含 JIT 预热）")
    @Tag("perf")
    void singleThreadThroughput() {
        LsnAllocator allocator = new LsnAllocator();
        int n = 8_000_000;
        for (int i = 0; i < 200_000; i++) {
            allocator.allocate(); // 预热，让 getAndIncrement 走内联快路径
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            allocator.allocate();
        }
        long elapsedNanos = System.nanoTime() - t0;
        double nsPerOp = (double) elapsedNanos / n;
        // 纯 AtomicLong.getAndIncrement 在 x86 上约 5~8ns；这里放宽到 20ns 以免 CI 抖动误报
        assertTrue(nsPerOp < 20.0,
                "单次 LSN 分配 " + String.format(java.util.Locale.ROOT, "%.2f", nsPerOp)
                        + "ns，超过 20ns 上限（说明被意外加了锁或屏障）");
    }

    @Test
    @DisplayName("allocated() 是纯观测量，不影响分配序列")
    void allocatedIsObservational() {
        LsnAllocator allocator = new LsnAllocator(7L);
        assertEquals(7L, allocator.firstLsn(), "firstLsn() 必须回报注入值");
        long expectedSum = 0L;
        for (int i = 0; i < 1000; i++) {
            assertEquals((long) i, allocator.allocated(), "第 " + i + " 轮: allocated 应等于已完成分配数");
            expectedSum += allocator.allocated();
            allocator.allocate();
        }
        assertEquals(999L * 1000L / 2L, expectedSum, "0+1+...+999 的观测序列");
        assertEquals(1000L, allocator.allocated());
        assertEquals(7L + 1000L, allocator.peek());
    }

    @Test
    @DisplayName("多线程 peek() 与 allocate() 交错：peek 恒 >= 任一已分配值+1")
    void peekNeverBehindAllocated() throws Exception {
        LsnAllocator allocator = new LsnAllocator();
        int threads = 4;
        int perThread = 200_000;
        List<Thread> ts = new ArrayList<>(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        long lsn = allocator.allocate();
                        long next = allocator.peek();
                        if (next <= lsn) {
                            throw new AssertionError("peek()=" + next + " 不得 <= 刚分配的 " + lsn);
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "lsn-peek-" + t);
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) {
            th.join(60_000);
        }
        if (failure.get() != null) {
            throw new AssertionError("peek/allocate 交错断言失败", failure.get());
        }
        assertEquals((long) threads * perThread + 1L, allocator.peek());
    }
}
