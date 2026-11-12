package com.walcore.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.append.FrameLayout;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AppendOptions} 的契约测试：值语义 + <b>端到端往返</b>。
 *
 * <h2>这一组极易被漏测的字段：{@code attributes} 与 {@code flags}</h2>
 * 它们是 WAL 帧头里唯一的"业务可写位"。P1-06 幂等去重的边界是
 * <b>框架只负责把幂等键摘要写进帧、重放时原样回吐，判定交给上层</b>。
 * 这条边界只有在"写进去 → 读回来 → 值一模一样"被验证过之后才成立；
 * 一旦 {@code effectiveAttributes()} 的优先级搞反（attributes 覆盖了幂等键），
 * 上层的去重会静默失效——记的是同一个 key，读回却是别人的 attributes。
 */
@DisplayName("AppendOptions 值语义与端到端往返")
class AppendOptionsContractTest {

    // ==================================================================
    // ① 纯值语义（不碰 IO，快）
    // ==================================================================

    @Test
    @DisplayName("DEFAULTS 是业务数据记录：type=1、无幂等键、非事务提交、flags=0")
    void defaultsArePlainDataRecord() {
        AppendOptions o = AppendOptions.DEFAULTS;
        assertEquals(1, o.recordType(), "默认记录类型是 DATA(1)");
        assertEquals(0L, o.attributes(), "默认无 attributes");
        assertEquals(0L, o.idempotentKeyHash(), "默认无幂等键");
        assertFalse(o.txnCommit(), "默认不是事务提交点");
        assertEquals(0L, o.effectiveAttributes(), "无幂等键时 attributes 终值为 0");
        assertEquals(0, o.flags(), "默认不应置位任何 flag");
        assertSame(AppendOptions.DEFAULTS, AppendOptions.defaults(), "defaults() 应返回同一常量");
    }

    @Test
    @DisplayName("TXN_COMMIT / CONTROL 两个预置常量语义正确")
    void presetConstants() {
        assertTrue(AppendOptions.TXN_COMMIT.txnCommit(), "TXN_COMMIT 必须置位事务提交");
        assertEquals(FrameLayout.Flags.TXN_COMMIT, AppendOptions.TXN_COMMIT.flags(),
                "TXN_COMMIT 的 flags 应当只有事务提交位");
        assertFalse(AppendOptions.CONTROL.txnCommit(), "CONTROL 不是事务提交点");
        assertEquals(FrameLayout.RecordTypes.CONTROL, AppendOptions.CONTROL.recordType(),
                "CONTROL 的记录类型必须是 CONTROL(4)");
        assertNotEquals(FrameLayout.RecordTypes.DATA, AppendOptions.CONTROL.recordType(),
                "CONTROL 不能与业务数据记录混淆");
        assertSame(AppendOptions.CONTROL, AppendOptions.of(FrameLayout.RecordTypes.CONTROL),
                "of(CONTROL) 必须返回共享常量");
    }

    @Test
    @DisplayName("of(DATA) 直接返回 DEFAULTS 常量（热路径不分配）")
    void ofDataReturnsSharedConstant() {
        assertSame(AppendOptions.DEFAULTS, AppendOptions.of((byte) 1),
                "of(DATA) 必须返回共享常量，否则每次 append 都要 new 一个 options");
    }

    @Test
    @DisplayName("of(自定义类型) 保留类型且其余字段为默认")
    void ofCustomType() {
        AppendOptions o = AppendOptions.of((byte) 200);
        assertEquals((byte) 200, o.recordType());
        assertEquals(0L, o.effectiveAttributes());
        assertEquals(0, o.flags());
    }

    @Test
    @DisplayName("幂等键优先级高于 attributes：不会被误写覆盖")
    void idempotentKeyWinsOverAttributes() {
        AppendOptions o = AppendOptions.idempotent(0xDEADBEEFL);
        assertEquals(0xDEADBEEFL, o.effectiveAttributes(),
                "带幂等键时 attributes 终值必须是键摘要");
        assertEquals(FrameLayout.Flags.IDEMPOTENT_KEY, o.flags(),
                "必须置位 IDEMPOTENT_KEY 标志，否则重放时无法识别");
    }

    @Test
    @DisplayName("显式 attributes 生效（无幂等键时）")
    void explicitAttributesTakeEffect() {
        AppendOptions o = AppendOptions.attributes(0x1122334455667788L);
        assertEquals(0x1122334455667788L, o.effectiveAttributes());
        assertEquals(0, o.flags(), "单纯 attributes 不应置位任何 flag（避免读者误判为幂等键）");
    }

    @Test
    @DisplayName("flags 永不置位保留位 bit6/bit7")
    void flagsNeverTouchReservedBits() {
        byte[] candidates = {
                AppendOptions.DEFAULTS.flags(),
                AppendOptions.TXN_COMMIT.flags(),
                AppendOptions.CONTROL.flags(),
                AppendOptions.idempotent(1L).flags(),
                AppendOptions.attributes(1L).flags(),
        };
        for (byte f : candidates) {
            assertEquals(0, f & 0xC0, "bit6/bit7 是保留位，当前版本必须恒为 0，实际 flags=0x"
                    + Integer.toHexString(f & 0xFF));
            assertEquals(0, f & ~FrameLayout.Flags.DEFINED_MASK,
                    "不得出现未定义位: 0x" + Integer.toHexString(f & 0xFF));
        }
    }

    @Test
    @DisplayName("toString 含关键字段（日志/断言失败时可直接读）")
    void toStringIsInformative() {
        String s = AppendOptions.idempotent(42L).toString();
        assertTrue(s.contains("idempotentKeyHash=42"), "toString 必须含幂等键: " + s);
        assertTrue(s.contains("recordType=1"), "toString 必须含记录类型: " + s);
    }

    // ==================================================================
    // ② 端到端往返：写进去 → 读回来 值不变
    // ==================================================================

    @Test
    @DisplayName("幂等键往返：attributes 与 hasIdempotentKey() 都能读回")
    void idempotentKeySurvivesRoundTrip(@TempDir Path dir) {
        long keyHash = 0x5DEECE66DL;
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, TestWal.payload(32), AppendOptions.idempotent(keyHash));
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");

            WalRecord record = readAll(wal, 0).get(0);
            assertEquals(keyHash, record.attributes(),
                    "幂等键必须原样回吐到 attributes（上层靠它做去重）");
            assertTrue(record.hasIdempotentKey(), "必须能从 flags 识别出这是幂等键");
            assertFalse(record.isTxnCommit(), "不应误置事务提交位");
        }
    }

    @Test
    @DisplayName("事务提交标志往返：isTxnCommit() 为 true 且 attributes=0")
    void txnCommitSurvivesRoundTrip(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, TestWal.payload(16), AppendOptions.TXN_COMMIT);
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");

            WalRecord record = readAll(wal, 0).get(0);
            assertTrue(record.isTxnCommit(), "事务提交位必须保留");
            assertFalse(record.hasIdempotentKey(), "事务提交不应被识别成幂等键");
            assertEquals(0L, record.attributes(), "未指定 attributes 时应为 0");
        }
    }

    @Test
    @DisplayName("框架内置记录类型往返：recordType 原样保留")
    void recordTypeSurvivesRoundTrip(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            byte type = FrameLayout.RecordTypes.IDX_ANCHOR;
            long lsn = wal.append(0, TestWal.payload(8), AppendOptions.of(type));
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");

            List<WalRecord> read = readAll(wal, 0);
            assertEquals(1, read.size(), "框架内置类型必须可被默认解码器读回");
            assertEquals(type, read.get(0).recordType(),
                    "内置记录类型必须原样保留，否则上层无法分流处理");
        }
    }

    @Test
    @DisplayName("用户自定义类型（>=128）无处理器时整帧跳过：写进去了但读不出来（前向兼容契约）")
    void userRecordTypeIsSkippedWithoutHandler(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, TestWal.payload(8), AppendOptions.of((byte) 0xF1));
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "写入必须成功");
            assertTrue(lsn > 0L, "写入必须真的分配了 LSN（不是被静默丢弃）");

            List<WalRecord> read = readAll(wal, 0);
            assertTrue(read.isEmpty(),
                    "recordType >= USER_MIN 且无处理器时必须整帧跳过（T02 ④ 的前向兼容契约），"
                            + "实际读回 " + read.size() + " 条");
            // 跳过不等于损坏：后续写入仍必须正常
            long next = wal.append(0, TestWal.payload(8), AppendOptions.DEFAULTS);
            assertEquals(lsn + 1L, next, "跳过未知类型不得影响 LSN 连续性");
        }
    }

    @Test
    @DisplayName("显式 attributes 往返：不与幂等键混淆")
    void explicitAttributesSurviveRoundTrip(@TempDir Path dir) {
        long attrs = 0x0123456789ABCDEFL;
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, TestWal.payload(8), AppendOptions.attributes(attrs));
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");

            WalRecord record = readAll(wal, 0).get(0);
            assertEquals(attrs, record.attributes(), "attributes 必须原样保留");
            assertFalse(record.hasIdempotentKey(),
                    "纯 attributes 不得被识别成幂等键（否则上层会把业务字段当去重键）");
        }
    }

    @Test
    @DisplayName("重启后 flags/attributes 仍可读回（帧头持久化正确）")
    void flagsSurviveReopen(@TempDir Path dir) {
        long keyHash = 777L;
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, TestWal.payload(16),
                    AppendOptions.idempotent(keyHash));
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");
        }
        try (DefaultWal wal = TestWal.open(dir)) {
            WalRecord record = readAll(wal, 0).get(0);
            assertEquals(keyHash, record.attributes(), "重启后幂等键必须仍可读回");
            assertTrue(record.hasIdempotentKey(), "重启后 flags 必须仍可读回");
        }
    }

    @Test
    @DisplayName("同一条记录 payload 字节级往返一致（payloadBytes 不依赖 array()）")
    void payloadBytesRoundTripExactly(@TempDir Path dir) {
        byte[] original = "order-42|BUY|100@3500".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (DefaultWal wal = TestWal.open(dir)) {
            long lsn = wal.append(0, java.nio.ByteBuffer.wrap(original), AppendOptions.DEFAULTS);
            assertTrue(wal.flushAndWait(0, lsn, 10_000L), "必须可刷盘");

            WalRecord record = readAll(wal, 0).get(0);
            assertArrayEquals(original, record.payloadBytes(),
                    "payload 必须字节级一致（payloadBytes 用 duplicate().get，不依赖 array()）");
            assertEquals(original.length, record.payloadLength(), "payloadLength 逻辑长度应一致");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 读回某分区全部记录的副本。
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
