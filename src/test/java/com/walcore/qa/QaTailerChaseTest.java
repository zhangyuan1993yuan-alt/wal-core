package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.TailHandler;
import com.walcore.api.TailerOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalRecord;
import com.walcore.api.WalTailer;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 对抗性 Tailer 订阅测试（PRD P1-01 ②"从历史追起再转实时"的独立复核）。
 *
 * <p>先写 1000 条历史 → 开 tailer 从 LSN 1 订阅 → 同时继续并发写 2000 条 → 断言：
 * <ul>
 *   <li>收到的 LSN 序列无重复、无跳跃（恰为 1..3000 连续）；</li>
 *   <li>追平后 lagRecords() == 0；</li>
 *   <li>payload 与写入侧逐条一致（防"投递错帧"类 bug）。</li>
 * </ul>
 */
@DisplayName("QA: Tailer 历史追赶 + 并发实时投递")
class QaTailerChaseTest {

    private static final int HISTORY = 1_000;
    private static final int LIVE = 2_000;
    private static final int PAYLOAD = 256;

    @Test
    @DisplayName("1000 历史 + 并发 2000 实时：交付恰为 1..3000 无重复无跳跃，追平 lag=0")
    void tailerChasesHistoryThenLive(@TempDir Path dir) throws Exception {
        byte[][] payloads = new byte[HISTORY + LIVE][];
        for (int i = 0; i < payloads.length; i++) {
            payloads[i] = QaSupport.seededPayload(PAYLOAD, i * 1_000_003L + 11L);
        }

        try (DefaultWal wal = TestWal.open(dir, 64L * 1024L, b -> {
        })) {
            for (int i = 0; i < HISTORY; i++) {
                wal.append(0, ByteBuffer.wrap(payloads[i]), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, HISTORY, 30_000L), "历史数据必须可刷盘");

            Recorder recorder = new Recorder();
            try (WalTailer tailer = wal.tailer(TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(1_000_000)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.BLOCK)
                    .handler(recorder)
                    .build())) {

                // 历史先追完（交付恰 1000 条，不多不少）
                QaSupport.mustAwait(() -> recorder.size() >= HISTORY, 30_000L,
                        "tailer 必须先追完 1000 条历史");
                assertEquals(HISTORY, recorder.size(), "历史阶段不应多交付任何记录");
                QaSupport.mustAwait(tailer::isLive, 10_000L, "追平后必须进入实时阶段");
                assertEquals(0L, tailer.lagRecords(), "追平后 lagRecords 必须为 0");
                assertEquals(0L, tailer.droppedRecords(), "BLOCK 策略下不允许丢弃");

                // 与 tailer 消费并发：继续写 2000 条
                for (int i = 0; i < LIVE; i++) {
                    wal.append(0, ByteBuffer.wrap(payloads[HISTORY + i]),
                            AppendOptions.DEFAULTS);
                }

                QaSupport.mustAwait(() -> recorder.size() >= HISTORY + LIVE, 60_000L,
                        "实时阶段必须交付新增的 2000 条");
                QaSupport.mustAwait(() -> tailer.lagRecords() == 0L, 30_000L,
                        "写完并消费完后 lag 必须归零");
                assertEquals(HISTORY + LIVE, tailer.deliveredRecords(),
                        "交付总数必须恰为 3000");
                assertEquals(0L, tailer.droppedRecords(), "不允许丢弃");
            }

            List<Long> lsns = recorder.snapshot();
            assertEquals(HISTORY + LIVE, lsns.size(), "交付条数不符: " + lsns.size());
            for (int i = 0; i < lsns.size(); i++) {
                assertEquals(i + 1L, lsns.get(i).longValue(),
                        "第 " + i + " 条交付的 LSN 应为 " + (i + 1) + "，实际 " + lsns.get(i)
                                + "（出现重复或跳跃）");
                assertArrayEquals(payloads[i], recorder.payloadAt(i),
                        "第 " + i + " 条交付的 payload 与写入时不一致");
            }
        }
    }

    /** 记录交付的 LSN 与 payload（tailer 回调线程写入，测试线程读取）。 */
    private static final class Recorder implements TailHandler {
        private final List<Long> lsns = new CopyOnWriteArrayList<>();
        private final List<byte[]> payloads = new CopyOnWriteArrayList<>();

        @Override
        public void onRecord(WalRecord record) {
            lsns.add(record.lsn());
            payloads.add(record.payloadBytes());
        }

        int size() {
            return lsns.size();
        }

        List<Long> snapshot() {
            return Collections.unmodifiableList(lsns);
        }

        byte[] payloadAt(int i) {
            return payloads.get(i);
        }
    }
}
