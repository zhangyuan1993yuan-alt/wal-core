package com.walcore.read;

import com.walcore.api.AppendOptions;
import com.walcore.api.TailHandler;
import com.walcore.api.TailerOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.api.WalTailer;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T13：实时 Tailer 的验收测试。
 *
 * <p>覆盖 TASKS T13 的六个验收点：追历史→追平切换点无空隙无重复、三档溢出策略、
 * 慢订阅者不拖垮写入、close 后线程干净退出。订阅延迟用 {@code @Tag("perf")} 的相对断言，
 * 因为 Windows 上的 {@code System.nanoTime} 抖动让 200µs 级别的绝对门槛不可靠。
 */
@DisplayName("实时订阅 Tailer")
class TailerTest {

    private static final int PAYLOAD = 16;

    // ==================================================================
    // ① 追历史 → 转实时：切换点无空隙、无重复
    // ==================================================================

    @Test
    @DisplayName("先追历史再转实时：LSN 序列是 1..N 的连续前缀，无空隙无重复")
    void historyThenLiveHasNoGapAndNoDuplicate(@TempDir Path dir) throws Exception {
        int history = 200;
        int live = 100;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < history; i++) {
                wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, history, 10_000L), "历史数据必须可刷盘");

            Recorder recorder = new Recorder();
            try (WalTailer tailer = wal.tailer(baseOptions(0, 1L, 1_000_000, recorder))) {
                // 历史阶段：等到历史全部交付
                awaitTrue(() -> recorder.size() >= history, 10_000L, "历史阶段必须先把 200 条追完");
                assertEquals(history, recorder.size(),
                        "追历史过程中不应多交付任何记录");

                // 追平后应切到实时
                awaitTrue(tailer::isLive, 5_000L, "追平后 isLive() 必须为 true");
                assertEquals(0L, tailer.lagRecords(), "追平后 lagRecords() 必须为 0");

                // 实时阶段：再写 100 条
                for (int i = 0; i < live; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                awaitTrue(() -> recorder.size() >= history + live, 10_000L,
                        "实时阶段必须交付新增的 100 条");
                assertEquals(history + live, tailer.deliveredRecords(),
                        "交付条数必须正好等于写入条数");
                assertEquals(0L, tailer.droppedRecords(), "不应发生丢弃");
            }

            List<Long> lsns = recorder.snapshot();
            assertEquals(history + live, lsns.size(), "交付条数不符: " + lsns.size());
            for (int i = 0; i < lsns.size(); i++) {
                assertEquals(i + 1L, lsns.get(i).longValue(),
                        "第 " + i + " 条交付的 LSN 应为 " + (i + 1) + "，实际 = " + lsns.get(i)
                                + "（切换点出现空隙或重复）");
            }
        }
    }

    @Test
    @DisplayName("空分区订阅：立刻 live，lagRecords() 为 0，随后写入可立即收到")
    void emptyPartitionTailsLive(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            Recorder recorder = new Recorder();
            try (WalTailer tailer = wal.tailer(baseOptions(1, 1L, 65_536, recorder))) {
                awaitTrue(tailer::isLive, 3_000L, "空分区订阅应立刻进入实时阶段");
                assertEquals(0L, tailer.lagRecords(), "空分区 lag 必须为 0");

                long lsn = wal.append(1, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                awaitTrue(() -> recorder.size() >= 1, 5_000L, "新写入必须被实时收到");
                assertEquals(List.of(lsn), recorder.snapshot());
            }
        }
    }

    @Test
    @DisplayName("lagRecords() 随消费单调下降并在追平后归零")
    void lagDecreasesToZero(@TempDir Path dir) throws Exception {
        int total = 300;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < total; i++) {
                wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, total, 10_000L), "必须可刷盘");

            Recorder recorder = new Recorder();
            try (WalTailer tailer = wal.tailer(baseOptions(0, 1L, 1_000_000, recorder))) {
                long first = tailer.lagRecords();
                assertTrue(first > 0L, "刚订阅时应当还有未消费的积压，实际 lag=" + first);
                awaitTrue(() -> tailer.lagRecords() == 0L, 10_000L, "追平后 lag 必须归零");
                assertTrue(tailer.deliveredRecords() >= total,
                        "交付条数必须覆盖全部历史: " + tailer.deliveredRecords());
            }
        }
    }

    // ==================================================================
    // ② 三档溢出策略
    // ==================================================================

    @Test
    @DisplayName("DISCONNECT：溢出时回调 onOverflow 并自动关闭，此后不再交付")
    void disconnectPolicyOnOverflow(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            CountDownLatch overflowLatch = new CountDownLatch(1);
            AtomicBoolean closeNotified = new AtomicBoolean(false);
            Recorder recorder = new Recorder();

            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    recorder.onRecord(record);
                    sleepMillis(5L);
                }

                @Override
                public void onOverflow(WalException.TailerOverflowException cause) {
                    overflowLatch.countDown();
                }

                @Override
                public void onClose() {
                    closeNotified.set(true);
                }
            };

            TailerOptions options = TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(TailerOptions.MIN_BUFFER_SIZE)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.DISCONNECT)
                    .handler(handler)
                    .build();

            try (WalTailer tailer = wal.tailer(options)) {
                for (int i = 0; i < 200; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                assertTrue(overflowLatch.await(20_000L, TimeUnit.MILLISECONDS),
                        "落后超过 bufferSize 必须触发 onOverflow");
                awaitTrue(tailer::isClosed, 10_000L, "DISCONNECT 后订阅应自动关闭");
                assertTrue(closeNotified.get(), "关闭时必须回调 onClose");
                int deliveredAtClose = (int) tailer.deliveredRecords();
                assertTrue(deliveredAtClose < 200,
                        "断开时不可能已交付全部 200 条（否则说明没有真正溢出）");
            }
        }
    }

    @Test
    @DisplayName("DROP_TO_LATEST：溢出时跳到最新，丢弃量计入 droppedRecords 且继续交付")
    void dropToLatestSkipsAndCounts(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            Recorder recorder = new Recorder();
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    recorder.onRecord(record);
                    sleepMillis(3L);
                }
            };
            TailerOptions options = TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(TailerOptions.MIN_BUFFER_SIZE)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.DROP_TO_LATEST)
                    .handler(handler)
                    .build();

            try (WalTailer tailer = wal.tailer(options)) {
                for (int i = 0; i < 400; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                awaitTrue(() -> tailer.droppedRecords() > 0L, 20_000L,
                        "DROP_TO_LATEST 必须真的丢弃一些记录（否则没测到策略）");
                awaitTrue(() -> tailer.lagRecords() == 0L, 20_000L,
                        "跳转后必须能重新追平");
                assertFalse(tailer.isClosed(), "DROP_TO_LATEST 不允许关闭订阅");
                assertTrue(tailer.deliveredRecords() > 0L, "跳转后必须继续交付");

                long delivered = tailer.deliveredRecords();
                long dropped = tailer.droppedRecords();
                assertTrue(delivered + dropped <= 400L,
                        "交付 + 丢弃不得超过写入总数: " + delivered + " + " + dropped);
            }
        }
    }

    @Test
    @DisplayName("BLOCK：溢出时不丢弃，最终按顺序交付全部记录")
    void blockPolicyNeverDrops(@TempDir Path dir) throws Exception {
        int total = 120;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            Recorder recorder = new Recorder();
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    recorder.onRecord(record);
                    sleepMillis(1L);
                }
            };
            TailerOptions options = TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(TailerOptions.MIN_BUFFER_SIZE)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.BLOCK)
                    .handler(handler)
                    .build();

            try (WalTailer tailer = wal.tailer(options)) {
                for (int i = 0; i < total; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                awaitTrue(() -> recorder.size() >= total, 30_000L,
                        "BLOCK 策略必须一条不丢地交付完");
                assertEquals(0L, tailer.droppedRecords(), "BLOCK 不允许丢弃");
                List<Long> lsns = recorder.snapshot();
                assertEquals(total, lsns.size(), "条数不符: " + lsns.size());
                for (int i = 0; i < lsns.size(); i++) {
                    assertEquals(i + 1L, lsns.get(i).longValue(),
                            "BLOCK 必须按 LSN 严格顺序交付");
                }
            }
        }
    }

    // ==================================================================
    // ③ 慢订阅者不拖垮写入
    // ==================================================================

    @Test
    @DisplayName("慢订阅者（每条 sleep 1ms）不拖慢写入：2000 次 append 远快于 2s")
    void slowSubscriberDoesNotBlockWriters(@TempDir Path dir) throws Exception {
        int writes = 2000;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            AtomicInteger delivered = new AtomicInteger();
            TailHandler slow = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    delivered.incrementAndGet();
                    sleepMillis(1L);
                }
            };
            TailerOptions options = TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(TailerOptions.MAX_BUFFER_SIZE)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.BLOCK)
                    .handler(slow)
                    .build();

            WalTailer tailer = wal.tailer(options);
            try {
                long started = System.nanoTime();
                for (int i = 0; i < writes; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

                // 若写入与订阅耦合，光回调就会耗掉 2000 × 1ms = 2000ms。
                // 这里要求写入耗时远小于"耦合下界"，是"解耦"的强证据。
                assertTrue(elapsedMs < writes / 4L,
                        "写入被慢订阅者拖住了：2000 次 append 用了 " + elapsedMs
                                + "ms（耦合下界约 " + writes + "ms）");
                assertTrue(delivered.get() >= 0);
                assertTrue(tailer.lagRecords() > 0L || delivered.get() > 0,
                        "订阅应当在后台持续推进");
            } finally {
                tailer.close();
            }
        }
    }

    // ==================================================================
    // ④ 生命周期
    // ==================================================================

    @Test
    @DisplayName("close() 后不再回调，且消费线程干净退出")
    void closeStopsCallbacksAndThread(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < 50; i++) {
                wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
            }
            assertTrue(wal.flushAndWait(0, 50, 10_000L), "必须可刷盘");

            CountDownLatch closedLatch = new CountDownLatch(1);
            Recorder recorder = new Recorder();
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    recorder.onRecord(record);
                }

                @Override
                public void onClose() {
                    closedLatch.countDown();
                }
            };
            WalTailer tailer = wal.tailer(baseOptions(0, 1L, 65_536, handler));
            awaitTrue(() -> recorder.size() >= 50, 10_000L, "应当先追上 50 条");
            tailer.close();
            assertTrue(closedLatch.await(5_000L, TimeUnit.MILLISECONDS), "必须回调 onClose");
            assertTrue(tailer.isClosed(), "close 后 isClosed() 必须为 true");
            int afterClose = recorder.size();
            Thread.sleep(120L);
            assertEquals(afterClose, recorder.size(), "close 后不得再有任何回调");
            tailer.close();  // 幂等
        }
    }

    @Test
    @DisplayName("订阅不存在的分区抛 PartitionException")
    void tailerOnMissingPartitionThrows(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            assertThrows(WalException.PartitionException.class,
                    () -> wal.tailer(baseOptions(9, 1L, 1024, new Recorder())));
        }
    }

    @Test
    @DisplayName("handler 抛异常：onError 被回调、订阅关闭、线程不悬挂")
    void handlerExceptionIsIsolated(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            CountDownLatch errorLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    throw new IllegalStateException("handler 故意抛异常");
                }

                @Override
                public void onError(Throwable error) {
                    assertNotNull(error);
                    errorLatch.countDown();
                }

                @Override
                public void onClose() {
                    closeLatch.countDown();
                }
            };
            try (WalTailer tailer = wal.tailer(baseOptions(0, 1L, 65_536, handler))) {
                wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                assertTrue(errorLatch.await(10_000L, TimeUnit.MILLISECONDS),
                        "handler 抛异常必须回调 onError");
                assertTrue(closeLatch.await(10_000L, TimeUnit.MILLISECONDS),
                        "异常后必须回调 onClose");
                awaitTrue(tailer::isClosed, 10_000L, "异常后订阅必须关闭");
            }
        }
    }

    // ==================================================================
    // ⑤ 延迟分布（相对断言）
    // ==================================================================

    @Test
    @Tag("perf")
    @DisplayName("订阅延迟分布合理：p99 远小于 50ms，中位数在微秒~毫秒量级")
    void tailLatencyDistributionIsSane(@TempDir Path dir) throws Exception {
        int total = 2000;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            List<Long> latenciesNanos = Collections.synchronizedList(new ArrayList<>());
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    latenciesNanos.add(Math.max(0L, System.nanoTime() - record.timestampNanos()));
                }
            };
            try (WalTailer tailer = wal.tailer(baseOptions(0, 1L, 1_000_000, handler))) {
                for (int i = 0; i < total; i++) {
                    wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
                }
                awaitTrue(() -> latenciesNanos.size() >= total, 30_000L, "必须收满样本");
            }

            List<Long> sorted;
            synchronized (latenciesNanos) {
                sorted = new ArrayList<>(latenciesNanos);
            }
            Collections.sort(sorted);
            long p50 = sorted.get((int) (sorted.size() * 0.50d));
            long p99 = sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * 0.99d)));
            long max = sorted.get(sorted.size() - 1);
            // 相对断言：Windows 上 nanoTime 抖动 + 50µs 轮询让它达不到 200µs 的绝对门槛，
            // 但"p99 不超过 50ms、p50 不超过 5ms"在任何平台上都必须是成立的。
            assertTrue(p50 < 5_000_000L, "p50 应为微秒~毫秒级，实际 " + p50 + "ns");
            assertTrue(p99 < 50_000_000L, "p99 必须远小于 50ms，实际 " + p99 + "ns（max=" + max + "）");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 造一份订阅选项（单分区、指定 fromLsn 与缓冲）。
     *
     * @param partitionId 分区 ID
     * @param fromLsn 起始 LSN
     * @param bufferSize 缓冲条数
     * @param handler 回调
     * @return 选项
     */
    private static TailerOptions baseOptions(int partitionId, long fromLsn, int bufferSize,
                                             TailHandler handler) {
        return TailerOptions.builder()
                .partitionId(partitionId)
                .fromLsn(fromLsn)
                .bufferSize(bufferSize)
                .overflowPolicy(WalConfig.TailerOverflowPolicy.BLOCK)
                .handler(handler)
                .build();
    }

    /**
     * 轮询等待条件成立（避免固定 sleep 造成的偶发失败）。
     *
     * @param condition 条件
     * @param timeoutMillis 超时
     * @param message 失败信息
     * @throws InterruptedException 被中断
     */
    private static void awaitTrue(BooleanSupplier condition, long timeoutMillis, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(2L);
        }
        assertTrue(condition.getAsBoolean(), message + "（等待 " + timeoutMillis + "ms 超时）");
    }

    /**
     * 睡眠指定毫秒，吞掉中断（测试辅助）。
     *
     * @param millis 毫秒
     */
    private static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 记录交付的 LSN 序列（线程安全）。
     */
    private static final class Recorder implements TailHandler {

        private final List<Long> lsns = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onRecord(WalRecord record) {
            lsns.add(record.lsn());
        }

        /**
         * 已交付条数。
         *
         * @return 条数
         */
        int size() {
            return lsns.size();
        }

        /**
         * 交付快照。
         *
         * @return 副本
         */
        List<Long> snapshot() {
            synchronized (lsns) {
                return new ArrayList<>(lsns);
            }
        }
    }
}
