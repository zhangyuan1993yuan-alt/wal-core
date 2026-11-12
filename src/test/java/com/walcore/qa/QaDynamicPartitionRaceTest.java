package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.PartitionOptions;
import com.walcore.api.RemoveMode;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 对抗性"动态分区操作 × 读写并发"测试（PRD P1-04 + P0-08 的独立复核）。
 *
 * <p>写分区 0~3 的同时 addPartition(4)、写分区 4、removePartition(1, FREEZE)。断言：
 * <ul>
 *   <li>全局 LSN 无空洞：所有成功写入的 LSN 合起来恰为 1..N 的置换；</li>
 *   <li>FREEZE 后对冻结分区写入立刻被拒绝（WAL_PARTITION）；</li>
 *   <li>冻结分区数据保留完整、可读到段尾；</li>
 *   <li>其余分区不受影响；重开后冻结分区数据仍完整、且仍拒绝写入。</li>
 * </ul>
 */
@DisplayName("QA: 动态分区操作与读写并发")
class QaDynamicPartitionRaceTest {

    private static final int WRITES_PER_PARTITION = 2_000;
    private static final int P4_WRITES = 500;

    @Test
    @DisplayName("并发写 0~3 时 addPartition(4)+写+FREEZE(1)：全局 LSN 无空洞、冻结可读、其余不受影响")
    void addAndFreezeDuringConcurrentWrites(@TempDir Path dir) throws Exception {
        Map<Long, byte[]> expectedByLsn = new ConcurrentHashMap<>();
        List<Thread> threads = new ArrayList<>();
        AtomicInteger failures = new AtomicInteger();
        @SuppressWarnings("unchecked")
        List<Long>[] perPartitionLsns = new List[5];
        for (int p = 0; p < 5; p++) {
            perPartitionLsns[p] = Collections.synchronizedList(new ArrayList<>());
        }

        try (DefaultWal wal = TestWal.open(dir, 64L * 1024L,
                b -> b.partitions(4).maxPartitions(64))) {

            CountDownLatch start = new CountDownLatch(1);

            // 4 个写线程：分区 0~3，其中分区 1 会在中途被 FREEZE
            for (int pid = 0; pid < 4; pid++) {
                final int partitionId = pid;
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < WRITES_PER_PARTITION; i++) {
                            try {
                                byte[] bytes = QaSupport.seededPayload(256,
                                        partitionId * 1_000_003L + i);
                                long lsn = wal.append(partitionId,
                                        ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
                                assertNull(expectedByLsn.putIfAbsent(lsn, bytes),
                                        "LSN 重复: " + lsn);
                                perPartitionLsns[partitionId].add(lsn);
                            } catch (WalException.PartitionException e) {
                                // 分区 1 被 FREEZE 之后写入被拒绝是【预期行为】：
                                // 必须是"明确拒绝"，不允许静默丢
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        failures.incrementAndGet();
                        e.printStackTrace();
                    }
                }, "qa-dyn-p" + pid);
                t.start();
                threads.add(t);
            }

            start.countDown();

            // 与写入并发：新增分区 4 并写入
            Thread dynWriter = new Thread(() -> {
                try {
                    Thread.sleep(30L);
                    wal.addPartition(4, PartitionOptions.DEFAULTS);
                    for (int i = 0; i < P4_WRITES; i++) {
                        byte[] bytes = QaSupport.seededPayload(256, 4_000_000_000L + i);
                        long lsn = wal.append(4, ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
                        assertNull(expectedByLsn.putIfAbsent(lsn, bytes), "LSN 重复: " + lsn);
                        perPartitionLsns[4].add(lsn);
                    }
                } catch (Throwable e) {
                    failures.incrementAndGet();
                    e.printStackTrace();
                }
            }, "qa-dyn-adder");
            dynWriter.start();
            threads.add(dynWriter);

            // 与写入并发：冻结分区 1
            Thread.sleep(80L);
            wal.removePartition(1, RemoveMode.FREEZE);

            for (Thread t : threads) {
                t.join();
            }
            assertEquals(0, failures.get(), "并发写入/新增分区不允许失败");
            // 写入速度时快时慢：分区 1 可能写完也可能中途被冻结，两种都合法；
            // 关键是"成功的写入一条不丢、之后的写入被明确拒绝"（下方逐一验证）
            assertTrue(perPartitionLsns[1].size() > 0,
                    "分区 1 至少应有部分成功写入，实际 0");

            // ---- 冻结语义立刻验证 ----
            assertThrows(WalException.PartitionException.class,
                    () -> wal.append(1, ByteBuffer.wrap(QaSupport.seededPayload(256, 99L)),
                            AppendOptions.DEFAULTS),
                    "FREEZE 后写入必须被明确拒绝");

            // 冻结分区的游标可读到段尾：恰为冻结前成功写入的 LSN 集合（有序）
            List<WalRecord> p1Records = QaSupport.readAll(wal, 1);
            assertEquals(perPartitionLsns[1].size(), p1Records.size(),
                    "冻结分区必须保留全部已写入数据（读到段尾）");
            List<Long> p1Expected = new ArrayList<>(perPartitionLsns[1]);
            Collections.sort(p1Expected);
            for (int i = 0; i < p1Records.size(); i++) {
                assertEquals(p1Expected.get(i).longValue(), p1Records.get(i).lsn(),
                        "冻结分区第 " + i + " 条 LSN 应为 " + p1Expected.get(i));
                assertArrayEquals(expectedByLsn.get(p1Records.get(i).lsn()),
                        p1Records.get(i).payloadBytes(), "冻结分区 payload 不一致");
            }

            // 其余分区数据完整
            for (int pid : new int[]{0, 2, 3}) {
                assertEquals(WRITES_PER_PARTITION, QaSupport.readAll(wal, pid).size(),
                        "分区 " + pid + " 不应受 FREEZE(1) 影响");
            }
            assertEquals(P4_WRITES, QaSupport.readAll(wal, 4).size(),
                    "动态新增的分区 4 数据必须完整");

            // 全局 LSN 无空洞：所有存活 LSN（0~4）合并恰为 1..N 的置换
            TreeMap<Long, byte[]> merged = new TreeMap<>();
            for (int pid = 0; pid < 5; pid++) {
                for (WalRecord r : QaSupport.readAll(wal, pid)) {
                    assertNull(merged.put(r.lsn(), r.payloadBytes()),
                            "跨分区 LSN 重复: " + r.lsn());
                }
            }
            long n = merged.size();
            assertEquals(expectedByLsn.size(), (int) n, "读回总数必须等于成功写入总数");
            assertEquals(1L, merged.firstKey().longValue(), "全局最小 LSN 必须为 1");
            assertEquals(n, merged.lastKey().longValue(),
                    "全局 LSN 必须无空洞：max=" + merged.lastKey() + " 应等于条数 " + n);
            for (long lsn = 1L; lsn <= n; lsn++) {
                assertNotNull(merged.get(lsn), "全局 LSN 空洞: " + lsn);
            }
            for (Map.Entry<Long, byte[]> e : merged.entrySet()) {
                assertArrayEquals(expectedByLsn.get(e.getKey()), e.getValue(),
                        "LSN " + e.getKey() + " payload 与写入时不一致");
            }
        }

        // ---- close → reopen：冻结分区数据仍在、仍拒绝写入，其他分区不受影响 ----
        try (DefaultWal reopened = TestWal.open(dir, 64L * 1024L,
                b -> b.partitions(4).maxPartitions(64))) {
            List<WalRecord> p1Records = QaSupport.readAll(reopened, 1);
            assertTrue(p1Records.size() > 0, "重开后冻结分区数据必须仍可读");
            assertEquals(perPartitionLsns[1].size(), p1Records.size(),
                    "重开后冻结分区数据必须完整");
            // 重开后对冻结分区写入仍必须被拒绝（FROZEN 状态必须被持久化并在恢复时生效）
            WalException ex = assertThrows(WalException.PartitionException.class,
                    () -> reopened.append(1, ByteBuffer.wrap(QaSupport.seededPayload(256, 100L)),
                            AppendOptions.DEFAULTS),
                    "重开后对 FROZEN 分区写入必须被拒绝");
            assertEquals("WAL_PARTITION", ex.errorCode());
        }
    }
}
