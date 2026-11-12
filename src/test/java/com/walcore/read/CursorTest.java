package com.walcore.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.read.CursorCheckpointStore;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * T09 游标与顺序读验收（ARCH §8.1、§13.2）。
 *
 * <h2>覆盖的九类 {@code seekTo} 边界位置</h2>
 * <ol>
 *   <li>{@code lsn = 0}（早于分区首条记录）；</li>
 *   <li>{@code lsn = 分区首条记录 LSN}；</li>
 *   <li>段内<b>中间</b>某条记录；</li>
 *   <li>段内<b>末条</b>记录；</li>
 *   <li>段边界（某个后续段的首条记录）；</li>
 *   <li><b>不存在</b>的 LSN（该分区 LSN 有空洞时，应落到下一个更大的记录）；</li>
 *   <li>{@code lsn = lastLsn + 1}（刚好越过末尾）；</li>
 *   <li>{@code lsn} 远大于 {@code lastLsn}；</li>
 *   <li>负 LSN（必须抛参数异常）。</li>
 * </ol>
 *
 * <h2>另外验证</h2>
 * 跨段连续读"无重复无遗漏"、断点续读（checkpoint）、checkpoint 原子写与 CRC 校验、
 * 批量读、轮转事件回调、关闭后行为。
 */
class CursorTest {

    private static final int PARTITION = 0;

    private static CursorOptions from(long lsn) {
        return CursorOptions.builder().fromLsn(lsn).useMappedWindows(false).build();
    }

    // ==================================================================
    // 九类 seekTo 边界
    // ==================================================================

    @Test
    @DisplayName("1) seekTo(0) 落到分区首条记录")
    void seekToZeroLandsOnFirstRecord(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 20; i++) {
                TestWal.append(wal, PARTITION, 16);
            }
            try (WalCursor cursor = wal.cursor(PARTITION, from(0L))) {
                WalRecord first = cursor.next();
                assertNotNull(first, "seekTo(0) 之后必须能读到首条记录");
                assertEquals(1L, first.lsn());
            }
        }
    }

    @Test
    @DisplayName("2) seekTo(分区首条记录) 精确命中")
    void seekToFirstRecord(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long firstLsn = TestWal.append(wal, PARTITION, 16);
            TestWal.append(wal, PARTITION, 16);
            try (WalCursor cursor = wal.cursor(PARTITION, from(firstLsn))) {
                assertTrue(cursor.seekTo(firstLsn), "首条记录必须可定位");
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(firstLsn, record.lsn());
            }
        }
    }

    @Test
    @DisplayName("3) seekTo(段内中间记录) 精确命中")
    void seekToMiddleRecord(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long target = 0L;
            for (int i = 0; i < 200; i++) {
                long lsn = TestWal.append(wal, PARTITION, 16);
                if (i == 99) {
                    target = lsn;
                }
            }
            wal.flushAndWait(PARTITION, target, 10_000L);
            try (WalCursor cursor = wal.cursor(PARTITION, from(target))) {
                assertTrue(cursor.seekTo(target));
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(target, record.lsn());
            }
        }
    }

    @Test
    @DisplayName("4) seekTo(段内末条记录) 精确命中")
    void seekToLastRecord(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long last = 0L;
            for (int i = 0; i < 50; i++) {
                last = TestWal.append(wal, PARTITION, 16);
            }
            try (WalCursor cursor = wal.cursor(PARTITION, from(last))) {
                assertTrue(cursor.seekTo(last));
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(last, record.lsn());
                assertNull(cursor.next(), "末条记录之后必须没有更多记录");
            }
        }
    }

    @Test
    @DisplayName("5) seekTo(段边界) 落到后续段的首条记录")
    void seekToSegmentBoundary(@TempDir Path dir) {
        // 1KB 段 + 72B 帧 → 每段 14 帧，必然多次轮转
        try (DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        })) {
            List<Long> all = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                all.add(TestWal.append(wal, PARTITION, 16));
            }
            wal.flushAndWait(PARTITION, all.get(all.size() - 1), 10_000L);
            List<com.walcore.internal.segment.Segment> segments =
                    wal.segmentManager().segments(PARTITION);
            assertTrue(segments.size() >= 3, "该配置下必须产生多个段，实际 = " + segments.size());

            // 逐个段的"名字 LSN"做 seekTo：至少要对每个段的真实首条记录命中
            for (com.walcore.internal.segment.Segment segment : segments) {
                long boundary = segment.firstLsn();
                try (WalCursor cursor = wal.cursor(PARTITION, from(boundary))) {
                    boolean found = cursor.seekTo(boundary);
                    if (!found) {
                        continue; // 空段或名字 LSN 大于真实首条（T05 权衡），跳过
                    }
                    WalRecord record = cursor.next();
                    assertNotNull(record);
                    assertTrue(record.lsn() >= boundary,
                            "段边界定位必须落到 >= 边界的记录，实际 = " + record.lsn());
                }
            }
        }
    }

    @Test
    @DisplayName("6) seekTo(不存在的 LSN) 落到下一个更大的记录")
    void seekToMissingLsnLandsOnNextRecord(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            // 交错写两个分区 → 分区 0 的 LSN 出现空洞
            List<Long> p0 = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                p0.add(wal.append(PARTITION, TestWal.payload(16), AppendOptions.DEFAULTS));
                wal.append(1, TestWal.payload(16), AppendOptions.DEFAULTS);
            }
            wal.flushAndWait(PARTITION, p0.get(p0.size() - 1), 10_000L);

            long gap = p0.get(10) + 1L;   // 这个 LSN 属于分区 1，分区 0 里不存在
            assertFalse(p0.contains(gap), "构造前提：gap 必须是分区 0 里不存在的 LSN");
            try (WalCursor cursor = wal.cursor(PARTITION, from(gap))) {
                boolean found = cursor.seekTo(gap);
                assertTrue(found, "空洞 LSN 必须能定位到「下一个」记录");
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(p0.get(11), record.lsn(),
                        "必须落到第一个 lsn >= gap 的记录");
            }
        }
    }

    @Test
    @DisplayName("7) seekTo(lastLsn + 1) 返回 false 且读不到记录")
    void seekToJustPastEnd(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long last = 0L;
            for (int i = 0; i < 30; i++) {
                last = TestWal.append(wal, PARTITION, 16);
            }
            try (WalCursor cursor = wal.cursor(PARTITION, from(last + 1L))) {
                assertFalse(cursor.seekTo(last + 1L), "越过末尾必须返回 false");
                assertNull(cursor.next(), "越过末尾必须读不到记录");
                assertEquals(last + 1L, cursor.position(), "position 应停在期望位置");
            }
        }
    }

    @Test
    @DisplayName("8) seekTo(远大于 lastLsn) 返回 false")
    void seekToFarBeyondEnd(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, PARTITION, 16);
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                assertFalse(cursor.seekTo(Long.MAX_VALUE / 2L));
                assertNull(cursor.next());
            }
        }
    }

    @Test
    @DisplayName("9) seekTo(负数) 抛参数异常")
    void seekToNegativeRejected(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, PARTITION, 16);
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                assertThrows(IllegalArgumentException.class, () -> cursor.seekTo(-1L));
            }
        }
    }

    // ==================================================================
    // 跨段连续读：无重复、无遗漏
    // ==================================================================

    @Test
    @DisplayName("跨段连续读：LSN 严格 +1，无重复无遗漏")
    void crossSegmentReadHasNoDuplicateNoGap(@TempDir Path dir) {
        int total = 2000;
        try (DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        })) {
            long last = 0L;
            for (int i = 0; i < total; i++) {
                last = TestWal.append(wal, PARTITION, 16);
            }
            wal.flushAndWait(PARTITION, last, 30_000L);
            int segments = wal.segmentManager().segmentCount(PARTITION);
            assertTrue(segments >= 20, "必须跨很多段，实际段数 = " + segments);

            List<Long> lsns = TestWal.readAllLsns(wal, PARTITION);
            assertEquals(total, lsns.size(), "记录条数必须与写入条数一致（无遗漏、无重复）");
            for (int i = 0; i < total; i++) {
                assertEquals(i + 1L, lsns.get(i).longValue(),
                        "第 " + i + " 条记录 LSN 必须是 " + (i + 1L));
            }
        }
    }

    @Test
    @DisplayName("payload 跨段读回内容逐字节一致")
    void payloadSurvivesCrossSegmentRead(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        })) {
            long last = 0L;
            for (int i = 0; i < 300; i++) {
                last = wal.append(PARTITION, TestWal.text("row-" + i + "-payload"),
                        AppendOptions.DEFAULTS);
            }
            wal.flushAndWait(PARTITION, last, 10_000L);
            List<WalRecord> records = TestWal.readAll(wal, PARTITION);
            assertEquals(300, records.size());
            for (int i = 0; i < 300; i++) {
                assertEquals("row-" + i + "-payload", records.get(i).payloadAsUtf8());
            }
        }
    }

    // ==================================================================
    // 批量读
    // ==================================================================

    @Test
    @DisplayName("nextBatch 按 maxRecords 分批且 visitor 返回 false 时提前停止")
    void nextBatchBatchesAndStops(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 100; i++) {
                TestWal.append(wal, PARTITION, 16);
            }
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                List<Long> first = new ArrayList<>();
                int n = cursor.nextBatch(30, r -> {
                    first.add(r.lsn());
                    return true;
                });
                assertEquals(30, n);
                assertEquals(30, first.size());
                assertEquals(1L, first.get(0).longValue());
                assertEquals(30L, first.get(29).longValue());

                List<Long> second = new ArrayList<>();
                int m = cursor.nextBatch(1000, r -> {
                    second.add(r.lsn());
                    return r.lsn() < 35L;
                });
                assertEquals(5, m, "visitor 在 lsn=35 后返回 false，应恰好读 5 条");
                assertEquals(31L, second.get(0).longValue());
                assertEquals(35L, second.get(second.size() - 1).longValue());
            }
        }
    }

    // ==================================================================
    // 断点续读（checkpoint）
    // ==================================================================

    @Test
    @DisplayName("断点续读：关闭游标后从 checkpoint 继续，不丢不重")
    void resumeFromCheckpoint(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 200; i++) {
                TestWal.append(wal, PARTITION, 16);
            }
            List<Long> firstHalf = new ArrayList<>();
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                for (int i = 0; i < 100; i++) {
                    WalRecord record = cursor.next();
                    assertNotNull(record);
                    firstHalf.add(record.lsn());
                }
            }
            assertEquals(100, firstHalf.size());

            // 新的游标：从 checkpoint 续读
            CursorOptions resume = CursorOptions.builder()
                    .fromLsn(1L)
                    .resumeFromCheckpoint(true)
                    .useMappedWindows(false)
                    .build();
            List<Long> secondHalf = new ArrayList<>();
            try (WalCursor cursor = wal.cursor(PARTITION, resume)) {
                WalRecord record;
                while ((record = cursor.next()) != null) {
                    secondHalf.add(record.lsn());
                }
            }
            assertEquals(100, secondHalf.size(), "续读必须恰好读到剩余 100 条");
            assertEquals(101L, secondHalf.get(0).longValue(), "续读必须从第 101 条开始（不重复）");
            assertEquals(200L, secondHalf.get(secondHalf.size() - 1).longValue());

            // 两段拼起来必须是无洞的 1..200
            List<Long> all = new ArrayList<>(firstHalf);
            all.addAll(secondHalf);
            for (int i = 0; i < 200; i++) {
                assertEquals(i + 1L, all.get(i).longValue());
            }
        }
    }

    @Test
    @DisplayName("checkpoint 原子写：内容含 CRC，且不残留 .tmp")
    void checkpointIsAtomicAndCrcProtected(@TempDir Path dir) throws Exception {
        CursorCheckpointStore store = new CursorCheckpointStore(dir);
        store.persist(PARTITION, 1234L);

        Path file = SegmentNaming.cursorPath(dir, PARTITION);
        assertTrue(Files.isRegularFile(file), "checkpoint 文件必须存在");
        String text = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(text.contains("lsn=1234"), "内容必须包含 lsn=1234，实际 = " + text);
        assertTrue(text.contains("crc32c="), "内容必须包含 crc32c 校验行");

        Path tmp = file.resolveSibling(file.getFileName() + SegmentNaming.TMP_EXT);
        assertFalse(Files.exists(tmp), "ATOMIC_MOVE 之后不得残留临时文件");

        assertEquals(1234L, store.restore(PARTITION));

        // 覆盖写：新值必须完整替换旧值
        store.persist(PARTITION, 5678L);
        assertEquals(5678L, store.restore(PARTITION));
        assertFalse(Files.exists(tmp));

        // 半截文件（模拟断电）：CRC 失效 → 视为"没有 checkpoint"
        Files.writeString(file, "lsn=99", StandardCharsets.UTF_8);
        assertEquals(0L, store.restore(PARTITION),
                "截断的 checkpoint 必须被判为无效，而不是返回错误位置");

        // CRC 被篡改：同样视为无效
        store.persist(PARTITION, 42L);
        String valid = Files.readString(file, StandardCharsets.UTF_8);
        Files.writeString(file, valid.replace("lsn=42", "lsn=43"), StandardCharsets.UTF_8);
        assertEquals(0L, store.restore(PARTITION), "CRC 不符必须被判为无效");

        // 删除后恢复为 0
        store.persist(PARTITION, 7L);
        store.delete(PARTITION);
        assertEquals(0L, store.restore(PARTITION));
    }

    @Test
    @DisplayName("resumeFromCheckpoint 时 fromLsn 更大则优先用 fromLsn")
    void explicitFromLsnWinsOverCheckpoint(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            for (int i = 0; i < 100; i++) {
                TestWal.append(wal, PARTITION, 16);
            }
            // 手工造一个很靠前的 checkpoint
            CursorCheckpointStore store = new CursorCheckpointStore(dir);
            try {
                store.persist(PARTITION, 10L);
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            CursorOptions options = CursorOptions.builder()
                    .fromLsn(80L)
                    .resumeFromCheckpoint(true)
                    .useMappedWindows(false)
                    .build();
            try (WalCursor cursor = wal.cursor(PARTITION, options)) {
                WalRecord record = cursor.next();
                assertNotNull(record);
                assertEquals(80L, record.lsn(),
                        "显式 fromLsn(80) 比 checkpoint(10) 更新，必须从 80 开始");
            }
        }
    }

    // ==================================================================
    // 轮转事件 / 活跃写入可见 / 关闭行为
    // ==================================================================

    @Test
    @DisplayName("跨段时回调 rollover 监听器")
    void rolloverListenerFires(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 1024L, b -> {
        })) {
            long last = 0L;
            for (int i = 0; i < 200; i++) {
                last = TestWal.append(wal, PARTITION, 16);
            }
            wal.flushAndWait(PARTITION, last, 10_000L);

            List<Long> rollovers = new ArrayList<>();
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                cursor.onSegmentRollover((pid, firstLsn) -> rollovers.add(firstLsn));
                while (cursor.next() != null) {
                    // 只为驱动跨段
                }
            }
            assertFalse(rollovers.isEmpty(), "跨段读必须触发 rollover 回调");
            for (int i = 1; i < rollovers.size(); i++) {
                assertTrue(rollovers.get(i) > rollovers.get(i - 1),
                        "rollover 回调的段边界必须递增");
            }
        }
    }

    @Test
    @DisplayName("游标能看到同一进程内新追加的记录（追写）")
    void cursorSeesLiveAppends(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, PARTITION, 16);
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                WalRecord first = cursor.next();
                assertNotNull(first);
                assertEquals(1L, first.lsn());
                assertNull(cursor.next(), "此时还没有第二条");

                TestWal.append(wal, PARTITION, 16);
                WalRecord second = cursor.next();
                assertNotNull(second, "新追加的记录必须能被游标看到");
                assertEquals(2L, second.lsn());
            }
        }
    }

    @Test
    @DisplayName("游标关闭幂等；关闭后 next/seekTo 抛 ClosedException")
    void closeIsIdempotentAndGuards(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            TestWal.append(wal, PARTITION, 16);
            WalCursor cursor = wal.cursor(PARTITION, from(1L));
            cursor.close();
            cursor.close();
            assertThrows(WalException.ClosedException.class, cursor::next);
            assertThrows(WalException.ClosedException.class, () -> cursor.seekTo(1L));
            assertThrows(WalException.ClosedException.class, () -> cursor.nextBatch(1, r -> true));
        }
    }

    @Test
    @DisplayName("空分区：游标可创建但读不到记录")
    void emptyPartitionYieldsNothing(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            try (WalCursor cursor = wal.cursor(PARTITION, from(1L))) {
                assertNull(cursor.next());
                assertFalse(cursor.seekTo(5L));
            }
        }
    }

    @Test
    @DisplayName("不存在的分区：cursor 抛 PartitionException")
    void unknownPartitionRejected(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            assertThrows(WalException.PartitionException.class,
                    () -> wal.cursor(999, from(1L)));
        }
    }
}
