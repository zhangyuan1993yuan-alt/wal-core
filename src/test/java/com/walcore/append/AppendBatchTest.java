package com.walcore.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code appendBatch} 的契约测试（T08 验收点 ⑥）。
 *
 * <h2>为什么批量接口需要单独的契约</h2>
 * {@code appendBatch} 不是"循环调用 append 的语法糖"，它有三个只有批量才成立的承诺：
 * <ol>
 *   <li><b>LSN 连续</b>：返回数组里的 LSN 必须是连续整数段，否则调用方无法用
 *       {@code lsns[0]..lsns[n-1]} 表达"这一批都持久了"；</li>
 *   <li><b>同一把锁内完成</b>：整批在分区写锁内一次做完，中间不会插入其他线程的记录，
 *       因此文件顺序 == LSN 顺序；</li>
 *   <li><b>失败即回滚尾部</b>：中途某条失败时已写入的帧不能留下"半个批次还占着 LSN"
 *       （本类验证异常路径不破坏后续写入的连续性）。</li>
 * </ol>
 */
@DisplayName("appendBatch 批量追加")
class AppendBatchTest {

    @Test
    @DisplayName("空列表返回长度 0 的数组，且不消耗 LSN")
    void emptyBatchIsNoOp(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long[] lsns = wal.appendBatch(0, List.of());
            assertEquals(0, lsns.length, "空批次必须返回空数组");

            long first = wal.append(0, TestWal.payload(8), AppendOptions.DEFAULTS);
            assertEquals(1L, first, "空批次不得消耗 LSN（下一条仍应是 1）");
        }
    }

    @Test
    @DisplayName("返回的 LSN 数组是连续整数段")
    void returnedLsnsAreContiguous(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            List<ByteBuffer> batch = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                batch.add(TestWal.payload(64, i));
            }
            long[] lsns = wal.appendBatch(0, batch);
            assertEquals(50, lsns.length, "返回数组长度必须等于入参条数");
            for (int i = 1; i < lsns.length; i++) {
                assertEquals(lsns[i - 1] + 1L, lsns[i],
                        "第 " + i + " 个 LSN 必须紧接前一个（批量接口的核心承诺）");
            }
            assertEquals(1L, lsns[0], "第一批的第一个 LSN 应为 1");
        }
    }

    @Test
    @DisplayName("批量写入的记录能被游标逐条读回，内容字节级一致")
    void batchIsReadableByteForByte(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            List<byte[]> originals = new ArrayList<>();
            List<ByteBuffer> batch = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                byte[] bytes = ("batch-" + i).getBytes(StandardCharsets.UTF_8);
                originals.add(bytes);
                batch.add(ByteBuffer.wrap(bytes));
            }
            long[] lsns = wal.appendBatch(0, batch);
            assertTrue(wal.flushAndWait(0, lsns[lsns.length - 1], 10_000L), "必须可刷盘");

            List<WalRecord> read = readAll(wal, 0);
            assertEquals(originals.size(), read.size(), "读回条数必须等于写入条数");
            for (int i = 0; i < read.size(); i++) {
                assertEquals(lsns[i], read.get(i).lsn(), "第 " + i + " 条的 LSN 必须与返回值一致");
                assertArrayEquals(originals.get(i), read.get(i).payloadBytes(),
                        "第 " + i + " 条 payload 必须字节级一致");
            }
        }
    }

    @Test
    @DisplayName("批量接口的 options 固定为 DEFAULTS：recordType=DATA 且无幂等键")
    void batchUsesDefaultAppendOptions(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long[] lsns = wal.appendBatch(0, List.of(TestWal.payload(16)));
            assertTrue(wal.flushAndWait(0, lsns[0], 10_000L), "必须可刷盘");
            WalRecord record = readAll(wal, 0).get(0);
            assertEquals(1, record.recordType(), "appendBatch 目前只支持 DATA 记录类型");
            assertEquals(0L, record.attributes(), "appendBatch 不携带 attributes");
            assertTrue(!record.hasIdempotentKey(), "appendBatch 不携带幂等键");
        }
    }

    @Test
    @DisplayName("单条 append 与 appendBatch 混用：LSN 全局连续无空洞")
    void mixedSingleAndBatchStayContiguous(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            List<Long> all = new ArrayList<>();
            all.add(wal.append(0, TestWal.payload(8), AppendOptions.DEFAULTS));
            all.addAll(toList(wal.appendBatch(0, List.of(TestWal.payload(8), TestWal.payload(8)))));
            all.add(wal.append(0, TestWal.payload(8), AppendOptions.DEFAULTS));
            all.addAll(toList(wal.appendBatch(0, List.of(TestWal.payload(8), TestWal.payload(8),
                    TestWal.payload(8)))));

            long last = all.get(all.size() - 1);
            assertTrue(wal.flushAndWait(0, last, 10_000L), "必须可刷盘");

            List<Long> fileOrder = new ArrayList<>();
            for (WalRecord record : readAll(wal, 0)) {
                fileOrder.add(record.lsn());
            }
            assertEquals(all, fileOrder,
                    "文件里的 LSN 顺序必须与调用顺序一致（批量不改变顺序语义）");
            for (int i = 0; i < fileOrder.size(); i++) {
                assertEquals(i + 1L, fileOrder.get(i).longValue(),
                        "混合调用后 LSN 仍必须是 1..N 连续无空洞");
            }
        }
    }

    @Test
    @DisplayName("跨段边界的大批次：轮转不影响 LSN 连续性与内容正确性")
    void batchAcrossSegmentBoundary(@TempDir Path dir) {
        int total = 120;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            List<ByteBuffer> batch = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                batch.add(TestWal.payload(256, i));
            }
            long[] lsns = wal.appendBatch(0, batch);
            assertTrue(wal.flushAndWait(0, lsns[total - 1], 10_000L), "必须可刷盘");

            List<WalRecord> read = readAll(wal, 0);
            assertEquals(total, read.size(), "跨段后条数不得丢失（读游标必须自动切段）");
            for (int i = 0; i < total; i++) {
                assertEquals(i + 1L, read.get(i).lsn());
                byte[] expected = TestWal.payload(256, i).array();
                assertArrayEquals(expected, read.get(i).payloadBytes(),
                        "第 " + i + " 条跨段后内容必须仍然正确");
            }
        }
    }

    @Test
    @DisplayName("入参含 null 元素：抛异常而不是写入半截帧")
    void nullElementIsRejected(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            List<ByteBuffer> batch = new ArrayList<>();
            batch.add(TestWal.payload(8));
            batch.add(null);
            assertThrows(RuntimeException.class, () -> wal.appendBatch(0, batch),
                    "含 null 的批次必须显式失败，不能静默写入一半");
        }
    }

    @Test
    @DisplayName("入参为 null 列表：抛异常")
    void nullListIsRejected(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            assertThrows(RuntimeException.class, () -> wal.appendBatch(0, null));
        }
    }

    @Test
    @DisplayName("单条超过 maxPayloadBytes：抛 PartitionException 且不影响后续写入")
    void oversizedElementRejectedThenWalStaysUsable(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 4096L,
                b -> b.maxPayloadBytes(1024))) {
            List<ByteBuffer> tooBig = List.of(TestWal.payload(2048));
            assertThrows(WalException.PartitionException.class,
                    () -> wal.appendBatch(0, tooBig),
                    "超过 maxPayloadBytes 必须报错并指出是第几个元素");

            long lsn = wal.append(0, TestWal.payload(64), AppendOptions.DEFAULTS);
            assertTrue(wal.flushAndWait(0, lsn, 10_000L),
                    "被拒的批次不得破坏实例可用性（后续单条写入仍必须成功）");
        }
    }

    @Test
    @DisplayName("已关闭实例上 appendBatch 抛 ClosedException（错误码 WAL_CLOSED）")
    void batchOnClosedWalThrowsClosed(@TempDir Path dir) {
        DefaultWal wal = TestWal.open(dir);
        wal.close();
        WalException.ClosedException e = assertThrows(WalException.ClosedException.class,
                () -> wal.appendBatch(0, List.of(TestWal.payload(8))));
        assertEquals("WAL_CLOSED", e.errorCode());
    }

    @Test
    @DisplayName("不存在的分区上 appendBatch 抛 PartitionException")
    void batchOnMissingPartitionThrows(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            assertThrows(WalException.PartitionException.class,
                    () -> wal.appendBatch(9, List.of(TestWal.payload(8))));
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * long[] → List&lt;Long&gt;。
     *
     * @param arr 数组
     * @return 列表
     */
    private static List<Long> toList(long[] arr) {
        List<Long> out = new ArrayList<>(arr.length);
        for (long v : arr) {
            out.add(v);
        }
        return out;
    }

    /**
     * 用游标读回分区全部记录副本。
     *
     * @param wal 实例
     * @param pid 分区
     * @return 记录副本
     */
    private static List<WalRecord> readAll(DefaultWal wal, int pid) {
        List<WalRecord> out = new ArrayList<>();
        try (WalCursor cursor = wal.cursor(pid,
                CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.copy());
            }
        }
        return out;
    }
}
