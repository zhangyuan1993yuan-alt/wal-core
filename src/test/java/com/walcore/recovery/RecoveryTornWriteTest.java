package com.walcore.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.walcore.api.AppendOptions;
import com.walcore.api.RecoveryReport;
import com.walcore.api.WalConfig;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.append.FrameLayout;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * P0-05 核心验收：<b>撕裂写 / 尾部损坏形态</b>恢复（ARCH §9.2 五条规则）。
 *
 * <h2>为什么不用真的 {@code kill -9}</h2>
 * 真 kill 只能在"恰好某个字节写到一半"的瞬间发生，不可重复、不可枚举。这里采用
 * <b>确定性坏字节注入</b>：先跑出一个真实可用的 WAL（{@code crashStopForTesting()} 让它
 * 停在"崩溃态"），再按字节精确构造 24 种物理损坏形态，然后重启。
 * 这比 kill -9 覆盖面更广：能构造出磁盘上永远不会出现的"最坏形态"。
 *
 * <h2>布局常量（决定了每个用例的期望值）</h2>
 * <pre>
 *   payload = 16B → frameLen = 64 + 16 = 80B（16 已 8 对齐，无 padding）
 *   20 条记录 → 数据区 = [0, 1600)，帧 i 的偏移 = 80 * i
 *   segmentSizeBytes = 4096（SPARSE 预分配，物理长度 4096）
 * </pre>
 *
 * <h2>每个用例统一断言的不变量</h2>
 * <ol>
 *   <li>重启成功（TAIL_TRUNCATE 下绝不因为尾部损坏而拒绝启动）；</li>
 *   <li>读回的是 {@code 1..k} 的<b>连续前缀</b>——没有空洞、没有重复；</li>
 *   <li>{@code report.nextLsn() == k + 1}（续写不会与幸存记录撞号）；</li>
 *   <li>恢复后<b>还能继续写</b>：append 得到 {@code k+1}，再重启能读到 {@code k+1} 条。</li>
 * </ol>
 * 这四条合起来证明"损坏尾部被正确截断，且 WAL 恢复为可用状态"——而不只是"没抛异常"。
 */
class RecoveryTornWriteTest {

    /** 写入记录条数。 */
    private static final int RECORDS = 20;

    /** 每条记录的 payload 字节数（16 → 帧长 80，8 对齐，无 padding）。 */
    private static final int PAYLOAD = 16;

    /** 帧长（由 payload 推出，测试里显式写出便于说明期望值）。 */
    private static final int FRAME = 80;

    /** 段大小。 */
    private static final long SEGMENT_SIZE = 4096L;

    /** 数据区结束偏移。 */
    private static final long DATA_END = (long) RECORDS * FRAME;

    /**
     * 一种尾部损坏形态。
     */
    @FunctionalInterface
    interface Damage {
        /**
         * 施加损坏。
         *
         * @param walFile 段文件路径
         * @throws Exception 施加失败
         */
        void apply(Path walFile) throws Exception;
    }

    // ==================================================================
    // 24 种形态
    // ==================================================================

    /**
     * 构造 24 种尾部损坏形态及其期望幸存记录数。
     *
     * @return 参数流：名称、损坏动作、期望幸存记录数
     */
    static Stream<Arguments> damageForms() {
        return Stream.of(
                // ---- 基线 ----
                Arguments.of("00-无损坏（预分配零尾部）", (Damage) file -> {
                }, (long) RECORDS),

                // ---- 规则①：帧头都没写完 / 文件被截短 ----
                Arguments.of("01-截断到最后一帧帧头中间(1583)", (Damage) f -> setLength(f, 1583L), 19L),
                Arguments.of("02-截断到最后一帧帧头刚好完整(1584)", (Damage) f -> setLength(f, 1584L), 19L),
                Arguments.of("03-截断到最后一帧 payload 中间(1590)", (Damage) f -> setLength(f, 1590L), 19L),
                Arguments.of("04-截断到倒数第二帧 payload 中间(1504)", (Damage) f -> setLength(f, 1504L), 18L),
                Arguments.of("05-截断到倒数第二帧帧首(1440)", (Damage) f -> setLength(f, 1440L), 18L),
                Arguments.of("06-截断到半个帧头(40)", (Damage) f -> setLength(f, 40L), 0L),
                Arguments.of("07-截断为空文件(0)", (Damage) f -> setLength(f, 0L), 0L),

                // ---- 规则②：magic 被破坏 ----
                Arguments.of("08-最后一帧 magic 置零", (Damage) f ->
                        patch(f, 19L * FRAME, le32(0)), 19L),
                Arguments.of("09-最后一帧整体清零", (Damage) f ->
                        zero(f, 19L * FRAME, FRAME), 19L),
                Arguments.of("10-最后一帧 magic 写成垃圾 0xDEADBEEF", (Damage) f ->
                        patch(f, 19L * FRAME, le32(0xDEADBEEF)), 19L),
                Arguments.of("11-中间第 5 帧 magic 写成垃圾", (Damage) f ->
                        patch(f, 5L * FRAME, le32(0x11223344)), 5L),

                // ---- 规则③：frameLen / headerLen 是垃圾值 ----
                Arguments.of("12-最后一帧 frameLen=0", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_FRAME_LEN, le32(0)), 19L),
                Arguments.of("13-最后一帧 frameLen=71（小于最小值）", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_FRAME_LEN, le32(71)), 19L),
                Arguments.of("14-最后一帧 frameLen=81（非 8 对齐）", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_FRAME_LEN, le32(81)), 19L),
                Arguments.of("15-最后一帧 frameLen=0x7FFFFFFF", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_FRAME_LEN, le32(0x7FFFFFFF)), 19L),
                Arguments.of("16-最后一帧 frameLen 超上限", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_FRAME_LEN,
                                le32(FrameLayout.MAX_FRAME_LEN + 8)), 19L),
                Arguments.of("17-最后一帧 headerLen=0", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_HEADER_LEN, new byte[]{0}), 19L),
                Arguments.of("18-最后一帧 headerLen=200（大于 frameLen）", (Damage) f ->
                        patch(f, 19L * FRAME + FrameLayout.OFFSET_HEADER_LEN, new byte[]{(byte) 200}), 19L),

                // ---- 规则④：帧头完整但文件不够长 ----
                Arguments.of("19-写合法帧头但文件长度不够放下该帧", (Damage) f -> {
                    patch(f, DATA_END, le32(FrameLayout.MAGIC));
                    patch(f, DATA_END + FrameLayout.OFFSET_FRAME_LEN, le32(FRAME));
                    patch(f, DATA_END + FrameLayout.OFFSET_HEADER_LEN, new byte[]{FrameLayout.HEADER_LEN});
                    setLength(f, DATA_END + FrameLayout.HEADER_LEN - 1L);
                }, (long) RECORDS),

                // ---- 规则⑤：CRC 不符（位翻转 / 坏块）----
                Arguments.of("20-最后一帧 payload 位翻转", (Damage) f ->
                        flip(f, 19L * FRAME + FrameLayout.HEADER_LEN + 3), 19L),
                Arguments.of("21-最后一帧 CRC 字段本身被改", (Damage) f ->
                        flip(f, 19L * FRAME + FrameLayout.OFFSET_CRC32C), 19L),
                Arguments.of("22-中间第 11 帧 payload 位翻转", (Damage) f ->
                        flip(f, 10L * FRAME + FrameLayout.HEADER_LEN + 5), 10L),

                // ---- 预分配尾部被垃圾覆盖 / 半个新帧 ----
                Arguments.of("23-数据区之后被垃圾字节覆盖", (Damage) f ->
                        fill(f, DATA_END, FrameLayout.HEADER_LEN, (byte) 0xA5), (long) RECORDS),
                Arguments.of("24-数据区之后出现半个新帧帧头", (Damage) f -> {
                    patch(f, DATA_END, le32(FrameLayout.MAGIC));
                    patch(f, DATA_END + FrameLayout.OFFSET_FRAME_LEN, le32(FRAME));
                }, (long) RECORDS)
        );
    }

    // ==================================================================
    // 参数化主用例
    // ==================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("damageForms")
    @DisplayName("尾部损坏形态：重启成功、幸存记录是连续前缀、可继续写")
    void tailDamageIsRecovered(String name, Damage damage, long expectedRecords, @TempDir Path dir)
            throws Exception {
        Path walFile = buildCrashedWal(dir);
        damage.apply(walFile);

        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        })) {
            RecoveryReport report = wal.openReport();
            assertNotNull(report, "恢复必须产出报告");

            List<Long> survivors = TestWal.readAllLsns(wal, 0);
            assertEquals(expectedRecords, survivors.size(),
                    "[" + name + "] 幸存记录数不符，实际 = " + survivors);
            for (int i = 0; i < survivors.size(); i++) {
                assertEquals(i + 1L, survivors.get(i).longValue(),
                        "[" + name + "] 幸存记录必须是 1..k 的连续前缀（不得有空洞），实际 = " + survivors);
            }
            assertEquals(expectedRecords + 1L, report.nextLsn(),
                    "[" + name + "] nextLsn 必须紧接最后一条幸存记录");

            // 恢复后必须是一个"还能用"的 WAL：续写 → 立刻刷盘 → 读回
            long next = wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
            assertEquals(expectedRecords + 1L, next,
                    "[" + name + "] 续写 LSN 必须紧接幸存记录（否则会与历史撞号）");
            assertTrue(wal.flushAndWait(0, next, 10_000L), "[" + name + "] 续写必须可刷盘");
        }

        // 再次重启：续写的那条必须还在，且整条链仍然连续
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        })) {
            List<Long> all = TestWal.readAllLsns(wal, 0);
            assertEquals(expectedRecords + 1L, all.size(),
                    "[" + name + "] 二次重启后记录数不符，实际 = " + all);
            for (int i = 0; i < all.size(); i++) {
                assertEquals(i + 1L, all.get(i).longValue(),
                        "[" + name + "] 二次重启后 LSN 链必须连续，实际 = " + all);
            }
        }
    }

    @Test
    @DisplayName("无损坏但 crashed（未干净关闭）时 report.clean 为 true 且 truncatedBytes 为 0")
    void crashedWithoutDamageIsClean(@TempDir Path dir) throws Exception {
        buildCrashedWal(dir);
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        })) {
            RecoveryReport report = wal.openReport();
            assertEquals(0L, report.truncatedBytes(),
                    "SPARSE 预分配的零尾部不能被算成 truncatedBytes");
            assertTrue(report.corruptions().isEmpty(), "无损坏时不得报告 corruption");
            assertEquals(RECORDS, TestWal.readAllLsns(wal, 0).size());
        }
    }

    @Test
    @DisplayName("损坏尾部产生的 truncatedBytes 恰为物理长度到最近完整帧的差值")
    void truncatedBytesIsReported(@TempDir Path dir) throws Exception {
        Path walFile = buildCrashedWal(dir);
        // 把最后一帧 magic 写成垃圾 → 逻辑长度应为 19*80 = 1520
        patch(walFile, 19L * FRAME, le32(0xDEADBEEF));
        long physical = Files.size(walFile);

        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        })) {
            RecoveryReport report = wal.openReport();
            assertEquals(physical - 19L * FRAME, report.truncatedBytes(),
                    "truncatedBytes 必须等于 physicalLength - lastGoodOffset");
            assertFalse(report.clean(),
                    "尾部截断属于「修复了一个问题」，必须让 report.clean=false 以便运维告警");
            assertEquals(19, TestWal.readAllLsns(wal, 0).size());
        }
    }

    // ==================================================================
    // 夹具与字节工具
    // ==================================================================

    /**
     * 造一个"崩溃态"的 WAL：20 条记录已持久化，但没走 close（{@code cleanShutdown=false}）。
     *
     * @param dir 日志目录
     * @return 分区 0 唯一段文件的路径
     * @throws Exception 构造失败
     */
    private static Path buildCrashedWal(Path dir) throws Exception {
        DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> {
        });
        long last = 0L;
        for (int i = 0; i < RECORDS; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD, i), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 10_000L), "夹具要求：写入必须已持久化");
        wal.crashStopForTesting();

        List<com.walcore.internal.segment.Segment> segments = wal.segmentManager().segments(0);
        assertEquals(1, segments.size(),
                "夹具要求：20 条 80B 记录必须落在同一个 4096B 段里（否则期望值全错）");
        Path file = segments.get(0).handle().path();
        assertEquals(SEGMENT_SIZE, Files.size(file),
                "夹具要求：SPARSE 预分配后物理长度必须是段容量");
        return file;
    }

    private static void patch(Path file, long offset, byte[] bytes) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            raf.write(bytes);
        }
    }

    private static void setLength(Path file, long length) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(length);
        }
    }

    private static void zero(Path file, long offset, int len) throws Exception {
        fill(file, offset, len, (byte) 0);
    }

    private static void fill(Path file, long offset, int len, byte value) throws Exception {
        byte[] bytes = new byte[len];
        java.util.Arrays.fill(bytes, value);
        patch(file, offset, bytes);
    }

    private static void flip(Path file, long offset) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 0xFF);
        }
    }

    private static byte[] le32(int value) {
        return new byte[]{
                (byte) (value & 0xFF), (byte) ((value >>> 8) & 0xFF),
                (byte) ((value >>> 16) & 0xFF), (byte) ((value >>> 24) & 0xFF)};
    }
}
