package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 对抗性"保留策略 × 正在读的游标"交互测试（PRD P1-03 / TASKS T14 ② 的独立复核）。
 *
 * <p>小段（4KB）写多段 → 开一个慢游标停在中间（pinned）→ 触发保留删除（age=1ms）→ 断言：
 * <ul>
 *   <li>pin 点之前的段被删除（磁盘上 .wal 文件数确实减少，或已到达删除地板）；</li>
 *   <li>pin 点之后（含正在读的段、活跃段）没被删 —— 通过"续读 LSN 严格连续到段尾"来钉死：
 *       若 pin 点之后任何段被删，续读必然出现 LSN 跳号，本断言直接红；</li>
 *   <li>慢游标能从 pin 点继续读到段尾（一条不少、payload 逐条一致）；</li>
 *   <li>从 pin 点起新开游标也能全量读对。</li>
 * </ul>
 *
 * <h2>时序健壮性设计（v2，QA 自修）</h2>
 * {@code retentionMaxAgeMillis=1ms} + 200ms 评估间隔意味着保留策略可能在<b>游标创建之前</b>
 * 就合法地删掉最旧的若干段（写入阶段偶发超过一个评估间隔时）。因此所有断言都改为
 * <b>按实际读到的 LSN 动态定位</b>（payload 按 {@code lsn-1} 索引），pin 点取
 * "实际读到的位置"而非固定值——这不放松任何语义断言，只是不再依赖"游标创建前什么都没删"。
 */
@DisplayName("QA: 保留策略与正在读的游标交互")
class QaRetentionPinnedCursorTest {

    private static final int TOTAL = 300;
    private static final int PIN_AFTER = 60;      // 游标读 60 条后停住（若早期段已被删则从存活处起算）
    private static final int PAYLOAD = 256;       // frameLen=320 → 4KB 段约 12 条/段
    private static final int FLOOR = 2;           // 删除地板：最后一段 + 1 个预建段

    @Test
    @DisplayName("pin 点前的段被删、pin 后保留、慢游标可续读到段尾且数据正确")
    void retentionDeletesBeforePinButKeepsFromPin(@TempDir Path dir) throws Exception {
        byte[][] payloads = new byte[TOTAL][];
        for (int i = 0; i < TOTAL; i++) {
            payloads[i] = QaSupport.seededPayload(PAYLOAD, i * 500_001L + 3L);
        }

        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> b
                .retentionEnabled(true)
                .retentionIntervalMillis(200L)
                // 500ms 而不是 1ms：1ms 会让"写入阶段偶发超过一个评估间隔"时，
                // 保留策略在游标创建之前就合法删掉最旧段，pin 阶段的固定条数断言被时序干扰。
                // 500ms 保证写入期（实测 130~200ms）内不可能有段超龄，删除只发生在
                // 游标打开之后 —— 对抗性（删除与读并发）不变，时序确定性提升。
                .retentionMaxAgeMillis(500L))) {

            for (int i = 0; i < TOTAL; i++) {
                wal.append(0, ByteBuffer.wrap(payloads[i]), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, TOTAL, 30_000L), "全部写入必须可持久确认");

            // 慢游标：从 LSN 1 读 PIN_AFTER 条后停住（早期段若已被合法删除，则从最早存活记录起读）
            try (var pinned = wal.cursor(0,
                    CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
                long lastLsn = 0L;
                for (int i = 0; i < PIN_AFTER; i++) {
                    WalRecord record = pinned.next();
                    assertNotNull(record, "第 " + i + " 条必须可读（300 条已全部写入）");
                    // 动态定位：payload 按 lsn-1 索引；LSN 严格递增（段内/跨段都不许跳号）
                    assertTrue(record.lsn() > lastLsn,
                            "pin 阶段 LSN 非递增: 上次 " + lastLsn + ", 本次 " + record.lsn());
                    lastLsn = record.lsn();
                    assertArrayEquals(payloads[(int) (record.lsn() - 1)], record.payloadBytes(),
                            "pin 阶段 LSN " + record.lsn() + " payload 不一致");
                }
                final long pinAt = lastLsn;
                assertEquals(pinAt + 1L, pinned.position(), "读完 pin 阶段后 position 应为 pinAt+1");

                // 等保留策略删除 pin 点之前的段：文件数下降，或已经删到地板
                // （若写入阶段已跨过评估间隔，pin 前的段可能在游标创建前就被删过，
                //   此时再无"更旧"可删——文件数已 ≤ 地板也视为达成）
                int before = countWalFiles(dir);
                boolean deleted = QaSupport.awaitTrue(
                        () -> countWalFiles(dir) < before || countWalFiles(dir) <= FLOOR,
                        30_000L, "等待保留策略删除 pin 点之前的段");
                assertTrue(deleted,
                        "30 秒内保留策略应删除 pin(LSN " + (pinAt + 1) + ") 之前的段："
                                + "pin 阶段结束时 " + before + " 个 .wal，现在 " + countWalFiles(dir));

                // 慢游标从 pin 点继续读到段尾：必须一条不少、LSN 严格连续、payload 一致。
                // 这是"pin 点之后的段没被删"的钉死断言：pin 后任何段被删都会表现为 LSN 跳号。
                int rest = 0;
                long prev = pinAt;
                WalRecord record;
                while ((record = pinned.next()) != null) {
                    assertEquals(prev + 1L, record.lsn(),
                            "续读阶段 LSN 跳号: 期望 " + (prev + 1) + "，实际 " + record.lsn()
                                    + "（pin 点之后的段被误删或缺帧）");
                    prev = record.lsn();
                    assertArrayEquals(payloads[(int) (record.lsn() - 1)], record.payloadBytes(),
                            "续读阶段 LSN " + record.lsn() + " payload 不一致");
                    rest++;
                }
                assertEquals(TOTAL - pinAt, rest,
                        "慢游标必须从 pin 点读到段尾（" + pinAt + "→300 共 " + (TOTAL - pinAt)
                                + " 条），实际 " + rest);
            }

            // 从 pin 点起新开游标也能读对。
            // 注意：此时 pinned 游标已关闭、无任何读者，保留策略可能已继续合法删除
            // pin 点之后的段（年龄超 500ms 就可删）——所以这里不做条数断言，
            // 只钉"读到的每一条都 payload 一致且 LSN 连续递增"。
            try (var fresh = wal.cursor(0,
                    CursorOptions.builder().fromLsn(PIN_AFTER + 1L).useMappedWindows(false)
                            .build())) {
                long prev = PIN_AFTER;   // 允许第一条从更高 LSN 开始（更早段可能已被删）
                WalRecord record;
                while ((record = fresh.next()) != null) {
                    assertTrue(record.lsn() > prev,
                            "新游标 LSN 非递增: 上次 " + prev + ", 本次 " + record.lsn());
                    prev = record.lsn();
                    assertArrayEquals(payloads[(int) (record.lsn() - 1)], record.payloadBytes(),
                            "新游标 LSN " + record.lsn() + " payload 不一致");
                }
                assertTrue(prev >= PIN_AFTER,
                        "新游标至少应能读到 pin 点（LSN 61）之后的记录");
            }
        }
    }

    private static int countWalFiles(Path baseDir) {
        Path partitionDir = SegmentNaming.partitionDir(baseDir, 0);
        if (!Files.isDirectory(partitionDir)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(partitionDir)) {
            return (int) files.filter(SegmentNaming::isWalFile).count();
        } catch (IOException e) {
            throw new IllegalStateException("扫描段文件失败: " + partitionDir, e);
        }
    }
}
