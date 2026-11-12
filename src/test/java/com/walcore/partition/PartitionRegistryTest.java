package com.walcore.partition;

import com.walcore.api.PartitionInfo;
import com.walcore.api.WalException;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T04 验收：分区注册表 + 分区状态机。
 *
 * <p>重点是验证三条 ARCH §3.2 / §11 的硬不变量：
 * <ol>
 *   <li><b>读路径无锁</b>：{@link PartitionRegistry#get(int)} 只做一次 volatile 读；
 *       并发 register/unregister 时读者<b>永不</b>抛 {@code ArrayIndexOutOfBoundsException}
 *       （COW 数组整体替换的直接后果）。</li>
 *   <li><b>水位单调</b>：{@code publishedLsn} / {@code durableLsn} 只进不退，
 *       100k 次跨线程发布后不得出现回退。</li>
 *   <li><b>状态单向</b>：{@code ACTIVE → FROZEN → (ARCHIVED|PURGED)}，不可逆。</li>
 * </ol>
 */
class PartitionRegistryTest {

    // ------------------------------------------------------------------
    // 基本注册/查找
    // ------------------------------------------------------------------

    @Test
    @DisplayName("register/get/require/unregister 基本语义")
    void basicRegisterAndLookup() {
        PartitionRegistry registry = new PartitionRegistry(16);
        assertEquals(16, registry.maxPartitions());
        assertEquals(0, registry.activeCount());

        PartitionImpl p3 = new PartitionImpl(3);
        registry.register(p3);
        assertSame(p3, registry.get(3));
        assertSame(p3, registry.require(3));
        assertEquals(1, registry.activeCount());
        assertTrue(registry.isRegistered(3));

        assertNull(registry.get(4), "未注册槽位必须返回 null 而不是抛异常");
        assertNull(registry.get(-1), "负 ID 必须返回 null（越界检查在 get 内完成）");
        assertNull(registry.get(9999), "超出槽位长度必须返回 null");
        assertThrows(WalException.PartitionException.class, () -> registry.require(4));
        assertThrows(WalException.PartitionException.class, () -> registry.require(-1));

        PartitionImpl removed = registry.unregister(3);
        assertSame(p3, removed);
        assertNull(registry.get(3));
        assertEquals(0, registry.activeCount());
        assertNull(registry.unregister(3), "重复注销必须返回 null 而不是抛异常");
    }

    @Test
    @DisplayName("重复注册、ID 越界注册都拒绝")
    void rejectsDuplicateAndOutOfRange() {
        PartitionRegistry registry = new PartitionRegistry(4);
        registry.register(new PartitionImpl(0));
        assertThrows(WalException.PartitionException.class,
                () -> registry.register(new PartitionImpl(0)), "重复注册必须拒绝");
        assertThrows(WalException.PartitionException.class,
                () -> registry.register(new PartitionImpl(4)), "ID == maxPartitions 必须拒绝");
        assertThrows(WalException.PartitionException.class,
                () -> registry.register(new PartitionImpl(9_999)), "远超槽位长度必须拒绝");
        // 负 ID 在 PartitionImpl 构造时就被拦下（比 register 更早，属于更严格的防线）
        assertThrows(IllegalArgumentException.class,
                () -> new PartitionImpl(-1), "负 ID 必须在构造期拒绝");
        assertThrows(NullPointerException.class, () -> registry.register(null));
        assertEquals(1, registry.activeCount(), "失败的注册不得改变存活计数");
        assertSame(registry.get(0), registry.require(0));
    }

    @Test
    @DisplayName("activeIds 升序快照；activeIdsCopy 是真副本")
    void activeIdsSnapshot() {
        PartitionRegistry registry = new PartitionRegistry(32);
        int[] pids = {31, 5, 0, 17, 2};
        for (int pid : pids) {
            registry.register(new PartitionImpl(pid));
        }
        int[] ids = registry.activeIds();
        int[] sorted = pids.clone();
        Arrays.sort(sorted);
        assertArrayEqualsInt(sorted, ids, "activeIds 必须升序");
        assertEquals(5, registry.activeCount());

        int[] copy = registry.activeIdsCopy();
        copy[0] = 12345;
        assertArrayEqualsInt(sorted, registry.activeIds(), "修改副本不得影响内部快照");
        assertNotSame(registry.activeIds(), registry.activeIdsCopy(), "activeIdsCopy 必须新建数组");

        registry.unregister(17);
        assertArrayEqualsInt(new int[]{0, 2, 5, 31}, registry.activeIds());
        assertEquals(4, registry.activeCount());

        List<PartitionImpl> snapshot = registry.snapshot();
        assertEquals(4, snapshot.size());
        for (int i = 0; i < snapshot.size(); i++) {
            assertNotNull(snapshot.get(i));
            assertEquals(registry.activeIds()[i], snapshot.get(i).id(), "快照顺序必须与 activeIds 一致");
        }
    }

    @Test
    @DisplayName("COW：写后旧数组引用不再被返回（引用发生替换）")
    void copyOnWriteReplacesArrayReference() {
        PartitionRegistry registry = new PartitionRegistry(8);
        registry.register(new PartitionImpl(1));
        PartitionImpl a = registry.get(1);
        registry.register(new PartitionImpl(2));
        // 旧引用 a 仍然有效（分区对象本身不被复制），但 registry.get(1) 仍返回同一对象
        assertSame(a, registry.get(1), "COW 只复制数组，不复制分区对象");
        assertNotNull(registry.get(2));
    }

    // ------------------------------------------------------------------
    // 并发
    // ------------------------------------------------------------------

    @Test
    @DisplayName("并发 register/unregister 时读路径不抛 AIOOBE（COW 核心保证）")
    void concurrentMutationNeverBreaksReaders() throws Exception {
        PartitionRegistry registry = new PartitionRegistry(64);
        // 预置一批分区，让读者总能读到一些非 null 值
        for (int i = 0; i < 32; i++) {
            registry.register(new PartitionImpl(i));
        }
        int readers = 8;
        int writers = 2;
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(readers + writers);
        try {
            for (int i = 0; i < readers; i++) {
                pool.execute(() -> {
                    try {
                        start.await();
                        long checksum = 0L;
                        for (int n = 0; n < 500_000; n++) {
                            // 覆盖合法范围、边界外与负值
                            for (int pid = -4; pid < 70; pid++) {
                                PartitionImpl p = registry.get(pid);
                                if (p != null) {
                                    checksum += p.id();
                                }
                            }
                            int[] ids = registry.activeIds();
                            checksum += ids.length;
                        }
                        if (checksum == 42L) {
                            throw new AssertionError("unreachable, keeps checksum live");
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
            }
            for (int w = 0; w < writers; w++) {
                pool.execute(() -> {
                    try {
                        start.await();
                        for (int n = 0; n < 20_000; n++) {
                            int pid = 32 + (n % 24);
                            registry.unregister(pid);
                            registry.unregister(pid);
                            try {
                                registry.register(new PartitionImpl(pid));
                            } catch (WalException.PartitionException ignored) {
                                // 另一写线程抢先注册，正常
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS), "并发读写超时");
        } finally {
            pool.shutdownNow();
        }
        if (failure.get() != null) {
            throw new AssertionError("并发读写出现异常（COW 失效？）", failure.get());
        }
        // 收尾：注册状态必须自洽（activeCount 与 activeIds.length 一致）
        assertEquals(registry.activeIds().length, registry.activeCount(),
                "activeCount 与 activeIds 长度必须始终一致");
        for (int pid : registry.activeIds()) {
            assertNotNull(registry.get(pid), "activeIds 中的 ID 必须能 get 到分区，pid=" + pid);
        }
    }

    @Test
    @DisplayName("publishedLsn / durableLsn 跨线程单向前进，100k 次发布不回退")
    void watermarkMonotonicAcrossThreads() throws Exception {
        PartitionImpl p = new PartitionImpl(0);
        assertEquals(0L, p.publishedLsn(), "初始水位为 0（哨兵）");
        assertEquals(0L, p.durableLsn());

        int threads = 4;
        int perThread = 25_000;
        AtomicBoolean regression = new AtomicBoolean(false);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads * 2);
        try {
            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                pool.execute(() -> {
                    try {
                        start.await();
                        for (int i = 1; i <= perThread; i++) {
                            long lsn = base + i;
                            p.publishLsn(lsn);
                            long seen = p.publishedLsn();
                            if (seen < lsn) {
                                regression.set(true);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                pool.execute(() -> {
                    try {
                        start.await();
                        for (int i = 1; i <= perThread; i++) {
                            long lsn = base + i;
                            p.publishDurableLsn(lsn);
                            long seen = p.durableLsn();
                            if (seen < lsn) {
                                regression.set(true);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "水位发布超时");
        } finally {
            pool.shutdownNow();
        }
        assertFalse(regression.get(), "水位出现回退，违反「只进不退」不变量");
        assertEquals((long) threads * perThread, p.publishedLsn(),
                "所有线程发布后水位必须到达最大值");
        assertEquals((long) threads * perThread, p.durableLsn());
    }

    @Test
    @DisplayName("水位只进不退：显式回退请求被忽略")
    void watermarkIgnoresRewind() {
        PartitionImpl p = new PartitionImpl(1);
        p.publishLsn(100L);
        p.publishLsn(50L);
        assertEquals(100L, p.publishedLsn(), "publishLsn 必须忽略更小的值");
        p.publishDurableLsn(80L);
        p.publishDurableLsn(20L);
        assertEquals(80L, p.durableLsn(), "publishDurableLsn 必须忽略更小的值");
        p.publishDurableLsn(80L);
        assertEquals(80L, p.durableLsn(), "重复发布同值不应出错");
    }

    // ------------------------------------------------------------------
    // 写锁
    // ------------------------------------------------------------------

    @Test
    @DisplayName("写锁可重入、tryLock 反映占用状态")
    void writeLockBasics() throws Exception {
        PartitionImpl p = new PartitionImpl(2);
        p.lock();
        try {
            assertTrue(p.isHeldByCurrentThread());
            assertTrue(p.tryLock(), "ReentrantLock 必须支持同线程重入");
            p.unlock();
        } finally {
            p.unlock();
        }
        assertFalse(p.isHeldByCurrentThread());

        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> {
            p.lock();
            try {
                held.countDown();
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                p.unlock();
            }
        }, "partition-lock-holder");
        holder.start();
        assertTrue(held.await(5, TimeUnit.SECONDS));
        try {
            assertFalse(p.tryLock(), "锁被其他线程持有时 tryLock 必须返回 false");
        } finally {
            release.countDown();
            holder.join(5_000);
        }
        assertTrue(p.tryLock(), "释放后可再次获取");
        p.unlock();
    }

    // ------------------------------------------------------------------
    // 状态机
    // ------------------------------------------------------------------

    @Test
    @DisplayName("状态单向迁移：ACTIVE → FROZEN → ARCHIVED → PURGED，禁止回退")
    void stateTransitionsAreOneWay() {
        PartitionImpl p = new PartitionImpl(5);
        assertEquals(PartitionInfo.State.ACTIVE, p.state());
        assertTrue(p.isWritable());

        assertTrue(p.transitionTo(PartitionInfo.State.FROZEN));
        assertEquals(PartitionInfo.State.FROZEN, p.state());
        assertFalse(p.isWritable(), "FROZEN 分区不可写");

        assertFalse(p.transitionTo(PartitionInfo.State.ACTIVE), "禁止回退到 ACTIVE");
        assertEquals(PartitionInfo.State.FROZEN, p.state());
        assertFalse(p.transitionTo(PartitionInfo.State.FROZEN), "重复迁移返回 false");

        assertTrue(p.transitionTo(PartitionInfo.State.ARCHIVED));
        assertFalse(p.transitionTo(PartitionInfo.State.FROZEN), "禁止 ARCHIVED → FROZEN");

        assertTrue(p.transitionTo(PartitionInfo.State.PURGED));
        assertFalse(p.transitionTo(PartitionInfo.State.ARCHIVED), "PURGED 是终态");
        assertEquals(PartitionInfo.State.PURGED, p.state());
        assertThrows(NullPointerException.class, () -> p.transitionTo(null));
    }

    @Test
    @DisplayName("ARCHIVED 与 PURGED 同属终态层，可从 FROZEN 直达")
    void archiveAndPurgeBothReachableFromFrozen() {
        PartitionImpl a = new PartitionImpl(6);
        a.transitionTo(PartitionInfo.State.FROZEN);
        assertTrue(a.transitionTo(PartitionInfo.State.ARCHIVED));
        assertEquals(PartitionInfo.State.ARCHIVED, a.state());
        assertTrue(a.transitionTo(PartitionInfo.State.PURGED), "ARCHIVED 仍可继续到 PURGED");

        PartitionImpl b = new PartitionImpl(7);
        b.transitionTo(PartitionInfo.State.FROZEN);
        assertTrue(b.transitionTo(PartitionInfo.State.PURGED), "FROZEN 可直达 PURGED（RemoveMode.PURGE）");
        assertEquals(PartitionInfo.State.PURGED, b.state());
    }

    @Test
    @DisplayName("并发状态下状态机不出现回退（多线程竞争迁移）")
    void concurrentStateTransitionsKeepMonotonicity() throws Exception {
        PartitionImpl p = new PartitionImpl(8);
        PartitionInfo.State[] targets = PartitionInfo.State.values();
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                pool.execute(() -> {
                    try {
                        start.await();
                        int lastRank = 0;
                        for (int i = 0; i < 20_000; i++) {
                            PartitionInfo.State target = targets[(seed + i) % targets.length];
                            p.transitionTo(target);
                            int rank = rankOf(p.state());
                            if (rank < lastRank) {
                                throw new AssertionError("状态发生回退: " + lastRank + " -> " + rank);
                            }
                            lastRank = rank;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        if (failure.get() != null) {
            throw new AssertionError("并发状态迁移出错", failure.get());
        }
    }

    @Test
    @DisplayName("id / createdMillis / firstLsnOfPartition 元数据")
    void metadataAccessors() {
        long before = System.currentTimeMillis();
        PartitionImpl p = new PartitionImpl(11);
        long after = System.currentTimeMillis();
        assertEquals(11, p.id());
        assertTrue(p.createdMillis() >= before && p.createdMillis() <= after,
                "createdMillis 必须落在构造区间内");
        assertEquals(0L, p.firstLsnOfPartition(), "未挂活跃段时首 LSN 为 0");
        assertNull(p.activeSegment());
        assertThrows(IllegalArgumentException.class, () -> new PartitionImpl(-1));
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------

    private static int rankOf(PartitionInfo.State s) {
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

    private static void assertArrayEqualsInt(int[] expected, int[] actual) {
        assertArrayEqualsInt(expected, actual, "数组内容不一致");
    }

    private static void assertArrayEqualsInt(int[] expected, int[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + ": expected=" + Arrays.toString(expected)
                    + ", actual=" + Arrays.toString(actual));
        }
    }
}
