package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.RecoveryReport;
import com.walcore.api.WalRecord;
import com.walcore.api.WalStats;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 对抗性"崩溃-恢复-续写-再崩溃"多轮循环（PRD P0-05 / P0-10 的独立复核）。
 *
 * <p>单分区（写入串行，截断只会留下连续前缀，断言可以做到最强）：
 * 写 N 条 → 只确认一部分 → 崩溃（不 flush、不写 cleanShutdown）→ 重开 →
 * 再写 M 条 → 再崩溃 → 再重开 → 再写 → 正常 close → 最终全量校验。
 *
 * <p>核心断言（每一轮都做）：
 * <ul>
 *   <li>已 flushAndWait 确认过的 LSN 一定全部在读回集合里（"宁可超时也绝不假确认"）；</li>
 *   <li>读回的 LSN 是 1..K 的连续前缀（无洞、无重复、无乱序）；</li>
 *   <li>每条记录 payload 与写入时字节一致；</li>
 *   <li>nextLsn = K+1，且新写入第一条的 LSN 恰为 K+1（LSN 绝不复用）；</li>
 *   <li>最终 clean close 后：写入过的每一条都在，totalLogBytes == 条数×320（256B payload）。</li>
 * </ul>
 */
@DisplayName("QA: 崩溃-恢复-续写-再崩溃 多轮循环")
class QaCrashRecoveryCycleTest {

    private static final int PAYLOAD = 256;   // frameLen = 64 + 256 = 320
    private static final long SEGMENT = 4096L; // 小段：几十条就轮转，逼出多段恢复路径

    /** lsn -> 写入时的 payload 字节（含被截断的，供最终比对用）。 */
    private final Map<Long, byte[]> expectedByLsn = new HashMap<>();

    @Test
    @DisplayName("两轮崩溃 + 最终正常关闭：确认不丢、前缀连续、LSN 不复用、payload 逐条一致")
    void crashRecoverContinueCrashAgain(@TempDir Path dir) throws Exception {
        // ---------- 第 1 轮：写 200 条，只确认前 100 条，崩溃 ----------
        DefaultWal wal = TestWal.open(dir, SEGMENT, b -> {
        });
        for (int i = 0; i < 200; i++) {
            append(wal, 0, lsn(i));
        }
        // 单分区串行写入：第 100 条的 LSN 恒为 100，用它作为持久确认点
        long confirmedA = 100L;
        assertTrue(wal.flushAndWait(0, confirmedA, 30_000L),
                "第 100 条必须能持久确认");
        wal.crashStopForTesting();

        // ---------- 重开 1：确认的 100 条必须在，前缀连续 ----------
        wal = TestWal.open(dir, SEGMENT, b -> {
        });
        int k1 = verifyPrefix(wal, "崩溃1后");
        assertTrue(k1 >= 100, "崩溃后确认过的 100 条必须全部在，实际只剩 " + k1);
        assertTrue(k1 <= 200, "未确认的尾部不该超过写入总量 200，实际 " + k1);
        verifyNextLsnContinues(wal, k1);

        // ---------- 第 2 轮：续写 150 条，只确认 80 条，再崩溃 ----------
        long base = wal.openReport().nextLsn();
        long confirmedB = base + 80L - 1L;
        for (int i = 0; i < 150; i++) {
            append(wal, 0, base + i);
        }
        assertTrue(wal.flushAndWait(0, confirmedB, 30_000L),
                "第二轮确认点必须能持久确认");
        wal.crashStopForTesting();

        // ---------- 重开 2 ----------
        wal = TestWal.open(dir, SEGMENT, b -> {
        });
        int k2 = verifyPrefix(wal, "崩溃2后");
        assertTrue(k2 >= confirmedB, "第二轮确认点 " + confirmedB + " 必须在读回集合内，实际 K=" + k2);
        verifyNextLsnContinues(wal, k2);

        // ---------- 第 3 轮：再写 100 条，这次正常 close ----------
        long base2 = wal.openReport().nextLsn();
        for (int i = 0; i < 100; i++) {
            append(wal, 0, base2 + i);
        }
        wal.close();

        // ---------- 最终全量校验 ----------
        wal = TestWal.open(dir, SEGMENT, b -> {
        });
        int total = verifyPrefix(wal, "最终");
        // 注：verifyNextLsnContinues 在崩溃2重开后额外探写了一条（LSN k2+1），
        // 且 clean close 前它已随第 3 轮一起落盘，因此最终 = k2 + 1(探写) + 100(第3轮)
        assertEquals(k2 + 101, total,
                "clean close 后必须一条不少：崩溃2残留 " + k2 + " + 新写 100 = " + (k2 + 100)
                        + "，实际 " + total);
        RecoveryReport report = wal.openReport();
        assertTrue(report.clean(), "clean close 后重开，恢复报告应为 clean: " + report);
        assertEquals(0L, report.truncatedBytes(), "clean close 后不应有任何截断");

        // payload 全量比对：所有存活记录的字节必须与写入时一致
        List<WalRecord> all = QaSupport.readAll(wal, 0);
        for (WalRecord record : all) {
            byte[] expected = expectedByLsn.get(record.lsn());
            assertNotNull(expected, "读回了从未写入的 LSN: " + record.lsn());
            assertArrayEquals(expected, record.payloadBytes(),
                    "LSN " + record.lsn() + " payload 与写入时不一致");
        }

        // 数据字节口径：N 条 256B 记录 → totalLogBytes == N × 320
        // （注意：此时尚未做 LSN 续接探写，条数就是 total）
        WalStats stats = wal.stats();
        assertEquals(total, stats.totalRecords(), "totalRecords 口径");
        assertEquals(total * 320L, stats.totalLogBytes(),
                "totalLogBytes 必须等于 条数×320（数据字节口径），实际 " + stats.totalLogBytes());

        // LSN 续接探写放在最后：它会把条数 +1，不影响上面的口径断言
        verifyNextLsnContinues(wal, total);

        wal.close();
    }

    @Test
    @DisplayName("write→crash→重开→appendSync 续写→再 crash：续写的已确认记录同样不丢")
    void crashThenConfirmedContinueAlsoSurvives(@TempDir Path dir) throws Exception {
        DefaultWal wal = TestWal.open(dir, SEGMENT, b -> {
        });
        for (int i = 0; i < 60; i++) {
            append(wal, 0, lsn(i));
        }
        assertTrue(wal.flushAndWait(0, 60L, 30_000L));
        wal.crashStopForTesting();

        wal = TestWal.open(dir, SEGMENT, b -> {
        });
        int k = verifyPrefix(wal, "第1次崩溃");
        assertTrue(k >= 60);

        // 续写 40 条并全部确认
        long base = wal.openReport().nextLsn();
        for (int i = 0; i < 40; i++) {
            append(wal, 0, base + i);
        }
        assertTrue(wal.flushAndWait(0, base + 39L, 30_000L), "续写后必须可确认");
        wal.crashStopForTesting();

        wal = TestWal.open(dir, SEGMENT, b -> {
        });
        int k2 = verifyPrefix(wal, "第2次崩溃");
        assertTrue(k2 >= base + 39,
                "续写且已确认的 40 条必须全部在: 期望 >= " + (base + 39) + "，实际 " + k2);
        wal.close();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private long append(DefaultWal wal, int partitionId, long seed) {
        byte[] bytes = QaSupport.seededPayload(PAYLOAD, seed);
        long lsn = wal.append(partitionId, ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
        expectedByLsn.put(lsn, bytes);
        return lsn;
    }

    private static long lsn(int i) {
        return i;
    }

    /**
     * 读回单分区全部记录：断言 LSN 恰为 1..K 连续前缀、payload 一致；返回 K。
     */
    private int verifyPrefix(DefaultWal wal, String stage) {
        List<WalRecord> all = QaSupport.readAll(wal, 0);
        for (int i = 0; i < all.size(); i++) {
            assertEquals(i + 1L, all.get(i).lsn(),
                    stage + ": 第 " + i + " 条读回的 LSN 应为 " + (i + 1)
                            + "（出现空洞/乱序/重复），实际 " + all.get(i).lsn());
            byte[] expected = expectedByLsn.get(all.get(i).lsn());
            assertNotNull(expected, stage + ": 读回未知 LSN " + all.get(i).lsn());
            assertArrayEquals(expected, all.get(i).payloadBytes(),
                    stage + ": LSN " + all.get(i).lsn() + " payload 不一致");
        }
        return all.size();
    }

    /**
     * 断言 nextLsn == K+1，且实际 append 返回 K+1。
     */
    private void verifyNextLsnContinues(DefaultWal wal, int k) {
        RecoveryReport report = wal.openReport();
        assertEquals(k + 1L, report.nextLsn(),
                "nextLsn 必须等于最后一条存活 LSN + 1，实际 " + report.nextLsn());
        long next = append(wal, 0, k + 1_000L);
        assertEquals(k + 1L, next, "重开后第一条写入的 LSN 必须为 K+1（绝不复用）");
    }
}
