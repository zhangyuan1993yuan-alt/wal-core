package com.walcore.read;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.TailHandler;
import com.walcore.api.TailerOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.api.WalTailer;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>游标读者登记粒度</b>的回归测试（保留策略 × 正在读的游标）。
 *
 * <h2>被钉住的不变量</h2>
 * <ol>
 *   <li>打开游标/推进游标时，<b>只有"当前正在读的那一个段"</b>登记了读者
 *       （{@code readerCount > 0} 的段数恒为 1），不是整个分区；</li>
 *   <li>因此 {@code RetentionManager.minPinnedLsn} 跟随游标位置推进，而不是恒等于
 *       最旧段的首帧 LSN；<b>已读过的旧段可以被删</b>；</li>
 *   <li>删掉旧段后，游标仍能从当前位置读到段尾，数据一条不少；</li>
 *   <li>{@link WalTailer} 同理（它内部就是一个 {@code CursorImpl}）。</li>
 * </ol>
 *
 * <h2>这条不变量为什么值得一个专门的测试</h2>
 * 早期实现（构造时对分区全部段 {@code retainReader}、{@code close} 才统一释放）不会让任何
 * 单测变红——读全对、写全对、崩溃恢复正常，只有<b>磁盘不再收缩</b>。也就是说这个缺陷
 * 只有在"保留策略 + 长活游标"这个组合下才可见，而且是**静默增长**（没有异常、没有日志）。
 * 一个显式的粒度断言比任何端到端用例都更早、更精确地失败。
 */
@DisplayName("游标只 pin 当前段（保留策略不被游标废掉）")
class CursorReaderPinTest {

    private static final int PAYLOAD = 256;
    private static final long SEGMENT = 4096L;

    @Test
    @DisplayName("读 60 条后：登记读者的段数恒为 1，且就是包含第 60 条的那个段")
    void exactlyOneSegmentIsRetainedWhileReading(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.retentionEnabled(false))) {
            long last = writeAndFlush(wal, 300);
            int segments = wal.segmentManager().segmentCount(0);
            assertTrue(segments >= 5, "300 条 × 320B 应产生多个段，实际 " + segments);

            try (WalCursor cursor = wal.cursor(0, fromLsn(1L))) {
                for (int i = 0; i < 60; i++) {
                    assertNotNull(cursor.next(), "第 " + i + " 条必须可读");
                }
                List<Segment> holders = wal.segmentManager().segments(0).stream()
                        .filter(s -> s.readerCount() > 0)
                        .toList();
                assertEquals(1, holders.size(),
                        "登记了读者的段必须恰好 1 个（当前段），实际 " + holders.size()
                                + " 个：" + holders.stream().map(Segment::tag).toList());
                assertEquals(1, holders.get(0).readerCount(),
                        "同一个段只允许被本游标登记一次（重复登记会导致 close 后计数不归零）");

                Segment current = holders.get(0);
                assertTrue(current.firstLsn() <= 60L && current.lastLsn() >= 60L,
                        "被持有的段必须包含刚读过的第 60 条，实际 "
                                + current.tag() + " 区间 [" + current.firstLsn()
                                + "," + current.lastLsn() + "]");
                assertTrue(last >= 60L);
            }
        }
    }

    @Test
    @DisplayName("minPinnedLsn 跟随游标推进，而不是恒等于最旧段首帧")
    void minPinnedLsnFollowsCursorPosition(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.retentionEnabled(false))) {
            writeAndFlush(wal, 300);
            long oldestFirst = wal.segmentManager().segments(0).get(0).firstLsn();
            assertEquals(0L, wal.retentionManager().minPinnedLsn(0),
                    "没有任何读者/快照时不应有 pin");

            try (WalCursor cursor = wal.cursor(0, fromLsn(1L))) {
                for (int i = 0; i < 120; i++) {
                    cursor.next();
                }
                long pinned = wal.retentionManager().minPinnedLsn(0);
                assertTrue(pinned > oldestFirst,
                        "游标已推进到第 120 条，minPinnedLsn 必须大于最旧段首帧（" + oldestFirst
                                + "），实际 " + pinned + "——若等于最旧段首帧，说明整个分区都被 pin 住了");
                assertTrue(pinned <= 120L,
                        "minPinnedLsn 不应超过游标已读位置（否则会多保护本可删的段），实际 " + pinned);
            }
            assertEquals(0L, wal.retentionManager().minPinnedLsn(0),
                    "游标关闭后 pin 必须归零");
        }
    }

    @Test
    @DisplayName("长活游标不再阻断保留：已读过的段被删，游标仍能读到段尾且一条不少")
    void retentionReclaimsConsumedSegmentsWhileCursorOpen(@TempDir Path dir) throws Exception {
        int total = 300;
        int pinAfter = 60;
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b
                .retentionEnabled(true)
                .retentionIntervalMillis(200L)
                .retentionMaxAgeMillis(1L))) {

            writeAndFlush(wal, total);
            int before = countWalFiles(dir);
            assertTrue(before >= 5, "应产生多个段，实际 " + before);

            try (WalCursor cursor = wal.cursor(0, fromLsn(1L))) {
                for (int i = 0; i < pinAfter; i++) {
                    assertNotNull(cursor.next());
                }

                awaitTrue(() -> countWalFiles(dir) < before, 20_000L,
                        "游标打开期间保留策略必须仍能删除已读过的段（否则磁盘会无限增长）");

                int rest = 0;
                WalRecord record;
                while ((record = cursor.next()) != null) {
                    assertEquals(pinAfter + rest + 1L, record.lsn(),
                            "续读阶段 LSN 错位（删段不得让游标跳号）");
                    rest++;
                }
                assertEquals(total - pinAfter, rest,
                        "游标必须能从 pin 点读到段尾，共 " + (total - pinAfter) + " 条，实际 " + rest);
            }

            // 关闭游标后剩余的旧段应继续被回收。
            // 期望的"地板"是 2 个文件：最后一段（deleteSegment 恒保留至少一个）+ 预建线程
            // 手里那一个已备好但未被采用的段。所以这里不能用 <=1，否则永远等不到。
            awaitTrue(() -> countWalFiles(dir) <= 2, 20_000L,
                    "游标关闭后保留策略必须把旧段全部回收（应只剩最后一段 + 1 个预建段）");
        }
    }

    @Test
    @DisplayName("两个游标停在不同位置：minPinnedLsn 取更小的那个，两者之间一个段都不许删")
    void twoCursorsKeepTheMinimumPin(@TempDir Path dir) throws Exception {
        // 这个用例钉的是<b>用户可见语义</b>：多个读者同时在线时，保留边界取最保守的那一个，
        // 且两个读者都必须能连续读到底。
        //
        // 说明：它<b>不是</b>用来隔离某一条实现的 mutation killer——"取最小值"这件事在实现里
        // 有三条独立冗余路径（轮首采样 / 每次删除前重采样 / minPinnedLsn 的 readerCount 兜底），
        // 任何单独一条被改坏都会被另外两条掩盖。三条并存是刻意的（纵深防御），
        // 所以这里只验证"最终行为对不对"，不追求杀掉某个单点变异。
        int total = 300;
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b
                .retentionEnabled(true)
                .retentionIntervalMillis(200L)
                .retentionMaxAgeMillis(1L))) {

            writeAndFlush(wal, total);
            int before = countWalFiles(dir);
            assertTrue(before >= 5, "应产生多个段，实际 " + before);

            // 老游标停在前面（位置小）、新游标停在后面。两者的围栏都必须在，
            // 保留边界取<b>较小</b>的那一个——否则"较老的读者"会被"较新的读者"顶掉，
            // 中间那些段就会被合法删掉。
            try (WalCursor older = wal.cursor(0, fromLsn(1L));
                 WalCursor newer = wal.cursor(0, fromLsn(1L))) {

                for (int i = 0; i < 60; i++) {
                    assertNotNull(older.next(), "老游标前 60 条必须可读");
                }
                for (int i = 0; i < 120; i++) {
                    assertNotNull(newer.next(), "新游标前 120 条必须可读");
                }
                long olderPos = older.position();
                long newerPos = newer.position();
                assertTrue(newerPos > olderPos,
                        "本用例前提：两个游标停在不同的段（older=" + olderPos
                                + ", newer=" + newerPos + "）");

                long pinned = wal.retentionManager().minPinnedLsn(0);
                assertTrue(pinned > 0L, "两个游标都打开时围栏必须存在，实际 " + pinned);
                assertTrue(pinned <= olderPos,
                        "围栏必须取较小者：围栏=" + pinned + " 不得大于老游标位置=" + olderPos);
                assertTrue(pinned <= newerPos,
                        "围栏不得大于新游标位置，围栏=" + pinned + ", newer=" + newerPos);

                // 让保留策略真的跑起来（先删掉老游标已读过的部分）
                awaitTrue(() -> countWalFiles(dir) < before, 20_000L,
                        "游标打开期间保留策略必须仍能删除已读过的段");

                // 核心不变量：两个游标各自持有的段都还在（没有被围栏越界删除）。
                // 若围栏被"较新的读者"顶掉，老游标那个段会先消失。
                List<Segment> survivors = wal.segmentManager().segments(0);
                List<Segment> held = survivors.stream()
                        .filter(s -> s.readerCount() > 0)
                        .toList();
                assertEquals(2, held.size(),
                        "两个游标必须各持有一个存活段，实际 " + held.size() + " 个："
                                + held.stream().map(Segment::tag).toList()
                                + "；存活段=" + survivors.stream().map(Segment::tag).toList());

                // 数据侧：两个游标都必须从当前位置连续读到末尾，一条不少
                assertContiguousToEnd(older, olderPos, "老游标");
                assertContiguousToEnd(newer, newerPos, "新游标");
            }

            awaitTrue(() -> countWalFiles(dir) <= 2, 20_000L,
                    "游标全部关闭后保留策略必须把旧段回收（应只剩最后一段 + 1 个预建段）");
        }
    }

    /**
     * 断言游标从 {@code expectedFrom} 起连续读到流末尾，且末尾就是 {@code total}。
     *
     * @param cursor 游标
     * @param expectedFrom 期望的下一条 LSN
     * @param label 失败信息里的标签
     */
    private static void assertContiguousToEnd(WalCursor cursor, long expectedFrom, String label) {
        long expected = expectedFrom;
        WalRecord record;
        while ((record = cursor.next()) != null) {
            assertEquals(expected, record.lsn(),
                    label + " 续读出现跳号（删段不得让游标丢区间）");
            expected++;
        }
        assertEquals(301L, expected,
                label + " 必须读到 LSN 300 为止（不早停），实际停在 " + (expected - 1L));
    }

    @Test
    @DisplayName("重复 close 不把读者计数打成负数")
    void closeIsIdempotentAndCountReturnsToZero(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.retentionEnabled(false))) {
            writeAndFlush(wal, 120);
            WalCursor cursor = wal.cursor(0, fromLsn(1L));
            cursor.next();
            cursor.close();
            cursor.close();
            cursor.close();
            assertTrue(wal.segmentManager().segments(0).stream().allMatch(s -> s.readerCount() == 0),
                    "多次 close 后所有段的读者计数必须恰好归零（不得为负）");
        }
    }

    @Test
    @DisplayName("seekTo 到已被保留策略删除的区间：不抛异常，从最早的存活记录开始读")
    void seekIntoDeletedRangeDoesNotThrow(@TempDir Path dir) throws Exception {
        int total = 300;
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b
                .retentionEnabled(true)
                .retentionIntervalMillis(200L)
                .retentionMaxAgeMillis(1L))) {

            writeAndFlush(wal, total);
            int before = countWalFiles(dir);
            awaitTrue(() -> countWalFiles(dir) < before, 20_000L,
                    "先让保留策略删掉最旧的若干个段");

            long earliestSurviving = wal.segmentManager().segments(0).get(0).firstLsn();
            try (WalCursor cursor = wal.cursor(0, fromLsn(1L))) {
                WalRecord first = cursor.next();
                assertNotNull(first, "即使 LSN 1 所在段已被删除，也必须能读到最早存活的记录");
                assertTrue(first.lsn() >= earliestSurviving,
                        "读到的第一条不应早于最早存活段的起点，实际 " + first.lsn()
                                + " < " + earliestSurviving);
                assertTrue(first.lsn() > 1L, "本用例的前提是 LSN 1 已被删除，实际 " + first.lsn());
            }
        }
    }

    @Test
    @DisplayName("Tailer 追平后同样只持有一个段（不能把整个分区 pin 住）")
    void tailerRetainsAtMostOneSegment(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.retentionEnabled(false))) {
            writeAndFlush(wal, 200);

            AtomicInteger delivered = new AtomicInteger();
            TailHandler handler = new TailHandler() {
                @Override
                public void onRecord(WalRecord record) {
                    delivered.incrementAndGet();
                }
            };
            TailerOptions options = TailerOptions.builder()
                    .partitionId(0)
                    .fromLsn(1L)
                    .bufferSize(65536)
                    .overflowPolicy(WalConfig.TailerOverflowPolicy.BLOCK)
                    .handler(handler)
                    .build();

            try (WalTailer tailer = wal.tailer(options)) {
                awaitTrue(() -> delivered.get() >= 200, 20_000L, "tailer 必须追上 200 条");
                awaitTrue(tailer::isLive, 10_000L, "追平后必须进入实时阶段");

                List<Segment> holders = wal.segmentManager().segments(0).stream()
                        .filter(s -> s.readerCount() > 0)
                        .toList();
                assertEquals(1, holders.size(),
                        "tailer 追平后只应持有 1 个段（当前读位置），实际 " + holders.size()
                                + " 个：" + holders.stream().map(Segment::tag).toList());
            }
            assertTrue(wal.segmentManager().segments(0).stream().allMatch(s -> s.readerCount() == 0),
                    "tailer 关闭后读者计数必须归零");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 游标选项：从指定 LSN 起、关闭映射窗口（让测试覆盖“无窗口”兜底路径）。
     *
     * @param from 起始 LSN
     * @return 选项
     */
    private static CursorOptions fromLsn(long from) {
        return CursorOptions.builder().fromLsn(from).useMappedWindows(false).build();
    }

    /**
     * 写 {@code count} 条固定长度记录并刷盘。
     *
     * @param wal 实例
     * @param count 条数
     * @return 最后一条的 LSN
     */
    private static long writeAndFlush(DefaultWal wal, int count) {
        long last = 0L;
        for (int i = 0; i < count; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 30_000L), "必须可持久确认");
        return last;
    }

    /**
     * 统计分区目录里的 {@code .wal} 文件数。
     *
     * @param baseDir 根目录
     * @return 数量
     */
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

    /**
     * 轮询等待条件成立。
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
            Thread.sleep(5L);
        }
        assertTrue(condition.getAsBoolean(), message + "（等待 " + timeoutMillis + "ms 超时）");
    }
}
