package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * QA 对抗性游标/写入并发竞态测试（PRD P0-06 ②"游标与写入方无锁并发"的独立复核）。
 *
 * <p>一个线程持续向分区 0 写入（payload 混合大小），另一个线程从头开游标顺序读。
 * 断言：
 * <ul>
 *   <li>读到序列是 LSN 严格递增的前缀（竞态下最常见的 bug 是读到半条/乱序帧）；</li>
 *   <li>写入停止后，游标能追平到 exact lastLsn（一条不少、一条不重）；</li>
 *   <li>追平后的 payload 与写入时字节一致。</li>
 * </ul>
 */
@DisplayName("QA: 游标与写入并发的竞态")
class QaCursorWriterRaceTest {

    private static final int TOTAL = 20_000;

    @Test
    @DisplayName("边写边读：读到的是严格递增前缀，写停后精确追平，payload 逐条一致")
    void cursorChasesWriter(@TempDir Path dir) throws Exception {
        // payload 预生成（写线程与读线程共用同一份不可变内容，避免共享可变集合）
        byte[][] payloads = new byte[TOTAL][];
        for (int i = 0; i < TOTAL; i++) {
            int len = (i % 3 == 0) ? 48 : ((i % 3 == 1) ? 256 : 1024);
            payloads[i] = QaSupport.seededPayload(len, i * 31L + 7L);
        }

        try (DefaultWal wal = TestWal.open(dir, 64L * 1024L, b -> {
        })) {
            Thread writer = new Thread(() -> {
                try {
                    for (int i = 0; i < TOTAL; i++) {
                        wal.append(0, ByteBuffer.wrap(payloads[i]), AppendOptions.DEFAULTS);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }, "qa-race-writer");
            writer.start();

            List<Long> seenLsns = new ArrayList<>(TOTAL);
            try (var cursor = wal.cursor(0, CursorOptions.builder().fromLsn(1L).build())) {
                long lastLsn = 0L;
                int probeIndex = 0;
                while (true) {
                    WalRecord record = cursor.next();
                    if (record == null) {
                        if (!writer.isAlive()) {
                            break;   // 写入已停止且已读到 published 尾部 → 真正追平
                        }
                        continue;    // 暂时追上，写入还在继续
                    }
                    if (record.lsn() <= lastLsn) {
                        throw new AssertionError("游标读到非递增/重复 LSN: 上次 " + lastLsn
                                + ", 本次 " + record.lsn());
                    }
                    lastLsn = record.lsn();
                    seenLsns.add(record.lsn());
                    assertArrayEquals(payloads[probeIndex], record.payloadBytes(),
                            "竞态读回的第 " + probeIndex + " 条 payload 与写入时不一致");
                    probeIndex++;
                }
                assertEquals(TOTAL, probeIndex, "必须逐条读完 TOTAL 条 payload 校验");
                assertEquals(TOTAL, seenLsns.size(),
                        "写入停止后游标必须追平到 exact lastLsn（一条不少一条不重）");
                assertEquals(TOTAL + 1L, cursor.position(),
                        "追平后游标 position 应为最后一条 LSN + 1 = " + (TOTAL + 1));
            }

            writer.join();
            assertEquals(TOTAL, wal.publishedLsn(0), "写入方 publishedLsn 应恰为 TOTAL");
        }
    }
}
