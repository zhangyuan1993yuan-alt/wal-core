package com.walcore.append;

import com.walcore.internal.append.FrameLayout;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link FrameLayout} 的常量与派生计算断言。
 *
 * <p>本测试是"帧格式唯一真源"的守门人：任何人顺手改动一个偏移常量，这里会立刻红。
 * 断言值逐项抄自 ARCH-wal.md §4.1 的字节布局表，<b>不要</b>改成"从实现反推"的值。
 */
class FrameLayoutTest {

    @Test
    @DisplayName("帧头字段偏移与 ARCH §4.1 逐项相等")
    void headerOffsetsMatchArch() {
        assertEquals(0, FrameLayout.OFFSET_MAGIC);
        assertEquals(4, FrameLayout.OFFSET_VERSION);
        assertEquals(5, FrameLayout.OFFSET_HEADER_LEN);
        assertEquals(6, FrameLayout.OFFSET_RECORD_TYPE);
        assertEquals(7, FrameLayout.OFFSET_FLAGS);
        assertEquals(8, FrameLayout.OFFSET_FRAME_LEN);
        assertEquals(12, FrameLayout.OFFSET_PAYLOAD_LEN);
        assertEquals(16, FrameLayout.OFFSET_STORED_LEN);
        assertEquals(20, FrameLayout.OFFSET_CRC32C);
        assertEquals(24, FrameLayout.OFFSET_LSN);
        assertEquals(32, FrameLayout.OFFSET_TIMESTAMP_NANOS);
        assertEquals(40, FrameLayout.OFFSET_PARTITION_ID);
        assertEquals(44, FrameLayout.OFFSET_CODEC_ID);
        assertEquals(48, FrameLayout.OFFSET_ATTRIBUTES);
        assertEquals(56, FrameLayout.OFFSET_RESERVED0);
        assertEquals(60, FrameLayout.OFFSET_RESERVED1);
    }

    @Test
    @DisplayName("魔数与版本")
    void magicAndVersion() {
        // "WAL1" 的 ASCII 小端读法
        assertEquals(0x57414C31, FrameLayout.MAGIC);
        assertEquals('W', (FrameLayout.MAGIC >>> 24) & 0xFF);
        assertEquals('A', (FrameLayout.MAGIC >>> 16) & 0xFF);
        assertEquals('L', (FrameLayout.MAGIC >>> 8) & 0xFF);
        assertEquals('1', FrameLayout.MAGIC & 0xFF);
        assertEquals(1, FrameLayout.FORMAT_VERSION);
    }

    @Test
    @DisplayName("大小常量：16_777_216 / 16_777_288 / 72 / 64")
    void sizeConstants() {
        assertEquals(64, FrameLayout.HEADER_LEN);
        assertEquals(16_777_216, FrameLayout.MAX_PAYLOAD_LEN);
        assertEquals(16_777_216, FrameLayout.MAX_PAYLOAD_BYTES);
        assertEquals(16_777_288, FrameLayout.MAX_FRAME_LEN);
        assertEquals(72, FrameLayout.MIN_FRAME_LEN);
        assertEquals(328, FrameLayout.TYPICAL_FRAME_LEN);
        assertEquals(8, FrameLayout.MIN_PAYLOAD_LEN);
        assertEquals(8, FrameLayout.ALIGNMENT);

        // MAX_FRAME_LEN == MAX_PAYLOAD_LEN + HEADER_LEN + ALIGNMENT 的算术自证
        assertEquals(FrameLayout.MAX_PAYLOAD_LEN + 64 + 8, FrameLayout.MAX_FRAME_LEN);
        // MIN_FRAME_LEN == HEADER_LEN + MIN_PAYLOAD_LEN（8B payload 天然 8 对齐，无填充）
        assertEquals(FrameLayout.HEADER_LEN + FrameLayout.MIN_PAYLOAD_LEN, FrameLayout.MIN_FRAME_LEN);
    }

    @Test
    @DisplayName("CRC 覆盖跳过区间为 [20, 24)")
    void crcSkipRange() {
        assertEquals(FrameLayout.OFFSET_CRC32C, FrameLayout.CRC_SKIP_START);
        assertEquals(FrameLayout.OFFSET_LSN, FrameLayout.CRC_SKIP_END);
        assertEquals(20, FrameLayout.CRC_SKIP_START);
        assertEquals(24, FrameLayout.CRC_SKIP_END);
        // 跳过的正好是 crc 字段自身的 4 字节
        assertEquals(4, FrameLayout.CRC_SKIP_END - FrameLayout.CRC_SKIP_START);
    }

    @Test
    @DisplayName("recordType 常量")
    void recordTypes() {
        assertEquals(1, FrameLayout.RecordTypes.DATA);
        assertEquals(2, FrameLayout.RecordTypes.CHECKPOINT);
        assertEquals(3, FrameLayout.RecordTypes.SNAPSHOT_MARK);
        assertEquals(4, FrameLayout.RecordTypes.CONTROL);
        assertEquals(5, FrameLayout.RecordTypes.IDX_ANCHOR);
        assertEquals(6, FrameLayout.RecordTypes.TOMBSTONE);
        assertEquals(128, FrameLayout.RecordTypes.USER_MIN);
    }

    @Test
    @DisplayName("flags 位定义与 bit6/bit7 保留")
    void flags() {
        assertEquals(0x01, FrameLayout.Flags.COMPRESSED);
        assertEquals(0x02, FrameLayout.Flags.ENCRYPTED);
        assertEquals(0x04, FrameLayout.Flags.IDEMPOTENT_KEY);
        assertEquals(0x08, FrameLayout.Flags.TXN_COMMIT);
        assertEquals(0x10, FrameLayout.Flags.PARTITION_FIRST);
        assertEquals(0x20, FrameLayout.Flags.PARTITION_LAST);
        assertEquals(0x3F, FrameLayout.Flags.DEFINED_MASK);

        assertTrue(FrameLayout.Flags.hasOnlyDefinedBits((byte) 0x3F));
        assertFalse(FrameLayout.Flags.hasOnlyDefinedBits((byte) 0x40));
        assertFalse(FrameLayout.Flags.hasOnlyDefinedBits((byte) 0x80));
        assertFalse(FrameLayout.Flags.hasOnlyDefinedBits((byte) 0xC0));
    }

    @Test
    @DisplayName("frameLen 恒为 8 的倍数，且 frameLen == 64 + storedLen + padding")
    void frameLenArithmetic() {
        for (int storedLen = 0; storedLen <= 4104; storedLen++) {
            int padding = FrameLayout.paddingFor(storedLen);
            assertTrue(padding >= 0 && padding <= 7, "padding 越界: " + padding);
            int frameLen = FrameLayout.frameLenFor(storedLen);
            assertEquals(FrameLayout.HEADER_LEN + storedLen + padding, frameLen);
            assertEquals(0, frameLen & 7, "frameLen 必须是 8 的倍数: " + frameLen);
            assertTrue(FrameLayout.isValidFrameLen(frameLen) || frameLen < FrameLayout.MIN_FRAME_LEN,
                    "frameLen 应合法: " + frameLen);
        }
    }

    @Test
    @DisplayName("典型 payload 长度对应的 frameLen")
    void typicalFrameLens() {
        // 128B payload -> 64 + 128 + 0 = 192
        assertEquals(192, FrameLayout.frameLenFor(128));
        // 256B payload -> 64 + 256 + 0 = 320（64+256 已 8 对齐，无需填充）
        //
        // 注意：FrameLayout.TYPICAL_FRAME_LEN 常量按 ARCH §4.1 取 328，
        // 那是"帧头 + payload + 满 8B 填充"的保守压测口径（相当于按 264B 估）。
        // 实际 frameLen 是 320；压测时按 328 估算只会略微高估字节数，不影响正确性。
        assertEquals(320, FrameLayout.frameLenFor(256));
        assertEquals(328, FrameLayout.TYPICAL_FRAME_LEN);
        // 1KB payload -> 64 + 1024 + 0 = 1088
        assertEquals(1088, FrameLayout.frameLenFor(1024));
        // 4KB payload -> 64 + 4096 + 0 = 4160
        assertEquals(4160, FrameLayout.frameLenFor(4096));
        // 8B payload -> 64 + 8 + 0 = 72（最小帧）
        assertEquals(FrameLayout.MIN_FRAME_LEN, FrameLayout.frameLenFor(8));
        // 1B payload -> 64 + 1 + 7 = 72
        assertEquals(72, FrameLayout.frameLenFor(1));
    }

    @Test
    @DisplayName("最大帧：16MB payload")
    void maxFrame() {
        int frameLen = FrameLayout.frameLenFor(FrameLayout.MAX_PAYLOAD_LEN);
        // 64 + 16777216 = 16777280，已是 8 的倍数，padding = 0
        assertEquals(0, FrameLayout.paddingFor(FrameLayout.MAX_PAYLOAD_LEN));
        assertEquals(16_777_280, frameLen);
        assertTrue(FrameLayout.isValidFrameLen(frameLen));
    }

    @Test
    @DisplayName("isValidFrameLen 的边界与对齐校验")
    void frameLenValidation() {
        assertFalse(FrameLayout.isValidFrameLen(0));
        assertFalse(FrameLayout.isValidFrameLen(71));
        assertTrue(FrameLayout.isValidFrameLen(72));
        assertFalse(FrameLayout.isValidFrameLen(73), "非 8 倍数必须非法");
        assertTrue(FrameLayout.isValidFrameLen(80));
        assertTrue(FrameLayout.isValidFrameLen(FrameLayout.MAX_FRAME_LEN));
        assertFalse(FrameLayout.isValidFrameLen(FrameLayout.MAX_FRAME_LEN + 8));
        assertFalse(FrameLayout.isValidFrameLen(-8));
    }

    @Test
    @DisplayName("payload / 分区 / headerLen 校验")
    void otherValidations() {
        assertTrue(FrameLayout.isValidPayloadLen(0));
        assertTrue(FrameLayout.isValidPayloadLen(FrameLayout.MAX_PAYLOAD_LEN));
        assertFalse(FrameLayout.isValidPayloadLen(FrameLayout.MAX_PAYLOAD_LEN + 1));
        assertFalse(FrameLayout.isValidPayloadLen(-1));

        assertTrue(FrameLayout.isValidPartitionId(0, 4096));
        assertTrue(FrameLayout.isValidPartitionId(4095, 4096));
        assertFalse(FrameLayout.isValidPartitionId(4096, 4096));
        assertFalse(FrameLayout.isValidPartitionId(-1, 4096));

        assertTrue(FrameLayout.isReadableHeaderLen(64));
        assertTrue(FrameLayout.isReadableHeaderLen(96), "未来版本 96B 帧头必须可读");
        assertFalse(FrameLayout.isReadableHeaderLen(63));
        assertFalse(FrameLayout.isReadableHeaderLen(0));
    }

    @Test
    @DisplayName("工具类构造器为 private（禁止实例化）")
    void constructorIsPrivate() throws Exception {
        java.lang.reflect.Constructor<FrameLayout> ctor =
                FrameLayout.class.getDeclaredConstructor();
        assertTrue(java.lang.reflect.Modifier.isPrivate(ctor.getModifiers()),
                "FrameLayout 的构造器必须为 private");
    }
}
