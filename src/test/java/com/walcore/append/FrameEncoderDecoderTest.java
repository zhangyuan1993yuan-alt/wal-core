package com.walcore.append;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalRecord;
import com.walcore.internal.append.FrameEncoder;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.metrics.LatencyHistogram;
import com.walcore.internal.read.DecodedFrame;
import com.walcore.internal.read.FrameDecoder;
import com.walcore.internal.spiimpl.RawRecordCodec;
import com.walcore.internal.util.Crc32cUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 帧编解码往返、CRC 覆盖区间、前向兼容的验收测试。
 *
 * <p>对应 TASKS-wal.md T02 的 7 条验收点。本文件是全项目"最容易悄悄错"的地方的守门人：
 * 帧偏移差 4 字节、CRC 多算或少算 4 字节这类错误，只有在"逐字节篡改"的测试下才会暴露。
 */
class FrameEncoderDecoderTest {

    /** 被测编码器。 */
    private final RawRecordCodec codec = new RawRecordCodec();

    /** 被测编码器（帧级）。 */
    private final FrameEncoder encoder = new FrameEncoder(codec);

    /** 允许前向兼容读的解码器。 */
    private final FrameDecoder decoder = new FrameDecoder(codec, true);

    /**
     * 生成确定性的伪随机 payload（同样的 size 永远得到同样的字节，便于逐字节比对）。
     *
     * @param size 字节数
     * @return 堆内缓冲区（position=0, limit=size）
     */
    private static ByteBuffer payload(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) ((i * 31 + 7) & 0xFF);
        }
        return ByteBuffer.wrap(b);
    }

    /**
     * 把 payload 编码成一帧，返回承载整帧的 Direct 缓冲区。
     *
     * @param payloadSize payload 字节数
     * @param lsn LSN
     * @param pid 分区
     * @param opts 选项
     * @return 长度为 {@code frameLen} 的 Direct 缓冲区（position=0, limit=capacity=frameLen）
     */
    private ByteBuffer encodeFrame(int payloadSize, long lsn, int pid, AppendOptions opts) {
        int cap = encoder.requiredFrameLen(payloadSize);
        ByteBuffer dst = ByteBuffer.allocateDirect(cap);
        int frameLen = encoder.encodeFrame(dst, 0, cap, lsn, pid, payload(payloadSize), opts);
        assertTrue(frameLen > 0, "编码失败");
        ByteBuffer exact = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer view = dst.duplicate();
        view.limit(frameLen);
        view.position(0);
        exact.put(view);
        exact.clear();
        return exact;
    }

    // ------------------------------------------------------------------
    // 验收点 ① 五种 payload 往返字节级一致
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "payload={0}B 往返字节级一致")
    @ValueSource(ints = {128, 256, 1024, 4096, 16 * 1024 * 1024})
    @DisplayName("① 128B/256B/1KB/4KB/16MB 编解码往返字节级完全一致")
    void roundTripByteIdentical(int payloadSize) {
        ByteBuffer src = payload(payloadSize);
        byte[] expected = new byte[payloadSize];
        src.duplicate().get(expected);

        long lsn = 1_048_577L + payloadSize;
        int pid = 3;
        ByteBuffer frame = encodeFrame(payloadSize, lsn, pid, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();

        DecodedFrame out = new DecodedFrame();
        int status = decoder.decode(frame, 0, frameLen, out, null);
        assertEquals(frameLen, status, "解码状态应为 frameLen");
        assertEquals(lsn, out.lsn);
        assertEquals(pid, out.partitionId);
        assertEquals(FrameLayout.RecordTypes.DATA, out.recordType);
        assertEquals(payloadSize, out.payloadLen);
        assertEquals(payloadSize, out.storedLen);
        assertEquals(0, out.codecId);
        assertFalse(out.skipped);
        assertNotNull(out.payload);

        byte[] actual = new byte[payloadSize];
        out.payload.duplicate().get(actual);
        assertArrayEquals(expected, actual, "payload 往返必须字节级一致");
    }

    // ------------------------------------------------------------------
    // 验收点 ② frameLen 恒为 8 的倍数，且 == 64 + storedLen + padding
    // ------------------------------------------------------------------

    @Test
    @DisplayName("② frameLen 恒为 8 的倍数且等于 64 + storedLen + padding")
    void frameLenInvariants() {
        for (int size : new int[]{1, 7, 8, 9, 128, 255, 256, 257, 1024, 4096, 65535}) {
            ByteBuffer frame = encodeFrame(size, 42L, 0, AppendOptions.DEFAULTS);
            int frameLen = frame.capacity();
            assertEquals(0, frameLen & 7, "frameLen 必须是 8 的倍数: " + frameLen);

            int storedLen = frame.getInt(FrameLayout.OFFSET_STORED_LEN);
            int payloadLen = frame.getInt(FrameLayout.OFFSET_PAYLOAD_LEN);
            assertEquals(size, payloadLen);
            assertEquals(size, storedLen);
            int padding = frameLen - FrameLayout.HEADER_LEN - storedLen;
            assertTrue(padding >= 0 && padding <= 7, "padding 必须在 0..7: " + padding);
            assertEquals(FrameLayout.frameLenFor(storedLen), frameLen);
        }
    }

    // ------------------------------------------------------------------
    // 验收点 ③ CRC 覆盖 [0,20) ∪ [24, frameLen)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("③ 改写 [20,24) 之外的任意一字节，解码必失败（抽样 100 个位置）")
    void crcCoversEverythingExceptCrcField() {
        ByteBuffer frame = encodeFrame(256, 777L, 5, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        byte[] original = new byte[frameLen];
        frame.duplicate().get(original);

        // 先确认未篡改时能正常解码
        DecodedFrame ok = new DecodedFrame();
        assertEquals(frameLen, decoder.decode(ByteBuffer.wrap(original.clone()), 0, frameLen, ok, null));

        int checked = 0;
        for (int i = 0; i < 100; i++) {
            int pos = (int) ((long) i * frameLen / 100L);
            if (pos >= FrameLayout.CRC_SKIP_START && pos < FrameLayout.CRC_SKIP_END) {
                continue; // 跳过 crc 字段自身
            }
            byte[] tampered = original.clone();
            tampered[pos] = (byte) (tampered[pos] ^ 0x01);
            DecodedFrame out = new DecodedFrame();
            int status = decoder.decode(ByteBuffer.wrap(tampered), 0, frameLen, out, null);
            assertTrue(status <= 0,
                    "篡改位置 " + pos + "（原值 0x" + Integer.toHexString(original[pos] & 0xFF)
                            + "）后解码仍然成功，说明 CRC 没有覆盖该字节");
            checked++;
        }
        assertTrue(checked >= 90, "抽样位置数过少: " + checked);
    }

    @Test
    @DisplayName("③-补充：翻转 crc 字段自身 1 bit 也必须被检出")
    void crcFieldItselfIsProtectedIndirectly() {
        ByteBuffer frame = encodeFrame(128, 9L, 1, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        byte[] bytes = new byte[frameLen];
        frame.duplicate().get(bytes);
        bytes[FrameLayout.OFFSET_CRC32C] ^= 0x01;
        DecodedFrame out = new DecodedFrame();
        int status = decoder.decode(ByteBuffer.wrap(bytes), 0, frameLen, out, null);
        assertEquals(FrameDecoder.STATUS_CRC_MISMATCH, status,
                "crc 字段被改动后，重算的 CRC 与原值不同，应判为 CRC 不匹配");
    }

    @Test
    @DisplayName("③-补充：payload 中部翻转 1 bit 必被检出")
    void payloadBitFlipDetected() {
        ByteBuffer frame = encodeFrame(4096, 11L, 2, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        byte[] bytes = new byte[frameLen];
        frame.duplicate().get(bytes);
        int middle = FrameLayout.HEADER_LEN + 2048;
        bytes[middle] ^= 0x40;
        DecodedFrame out = new DecodedFrame();
        assertEquals(FrameDecoder.STATUS_CRC_MISMATCH,
                decoder.decode(ByteBuffer.wrap(bytes), 0, frameLen, out, null));
    }

    // ------------------------------------------------------------------
    // 验收点 ④ headerLen = 96 的伪造帧被正确跳过未知头部
    // ------------------------------------------------------------------

    @Test
    @DisplayName("④ headerLen=96 的伪造帧被正确解码（payload 从 headerLen 起，不硬编码 64）")
    void forwardCompatibleLongerHeader() {
        int payloadSize = 200;
        byte[] payloadBytes = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payloadBytes[i] = (byte) (i & 0xFF);
        }
        ByteBuffer forged = buildFrameWithHeaderLen96(payloadBytes, 555_555L, 7);
        int frameLen = forged.capacity();

        DecodedFrame out = new DecodedFrame();
        int status = decoder.decode(forged, 0, frameLen, out, null);
        assertEquals(frameLen, status, "headerLen=96 的帧必须能被读取");
        assertEquals(96, out.headerLen);
        assertEquals(555_555L, out.lsn);
        assertEquals(7, out.partitionId);
        assertEquals(payloadSize, out.payloadLen);
        byte[] actual = new byte[payloadSize];
        out.payload.duplicate().get(actual);
        assertArrayEquals(payloadBytes, actual,
                "payload 起点必须是 frameOffset + headerLen（96），而不是 64");
    }

    /**
     * 构造一个 {@code headerLen=96} 的"未来版本"帧：96B 帧头 + payload + 8B 对齐填充。
     *
     * <p>前 64 字节的字段布局与本版本完全一致，64..96 是"未来新增的未知区域"（这里填 0xAB
     * 以便一眼看出它被跳过）。CRC 覆盖 {@code [0,20) ∪ [24, frameLen)}，因此未知区域
     * 也在校验范围内。
     *
     * @param payloadBytes payload
     * @param lsn LSN
     * @param pid 分区
     * @return 整帧
     */
    private static ByteBuffer buildFrameWithHeaderLen96(byte[] payloadBytes, long lsn, int pid) {
        int headerLen = 96;
        int frameLen = headerLen + payloadBytes.length;
        int pad = (8 - (frameLen & 7)) & 7;
        int total = frameLen + pad;
        ByteBuffer buf = ByteBuffer.allocate(total);
        // 未知头部区域填 0xAB，验证解码器真的按 headerLen 跳过
        for (int i = FrameLayout.HEADER_LEN; i < headerLen; i++) {
            buf.put(i, (byte) 0xAB);
        }
        buf.putInt(FrameLayout.OFFSET_MAGIC, FrameLayout.MAGIC);
        buf.put(FrameLayout.OFFSET_VERSION, FrameLayout.FORMAT_VERSION);
        buf.put(FrameLayout.OFFSET_HEADER_LEN, (byte) headerLen);
        buf.put(FrameLayout.OFFSET_RECORD_TYPE, FrameLayout.RecordTypes.DATA);
        buf.put(FrameLayout.OFFSET_FLAGS, (byte) 0);
        buf.putInt(FrameLayout.OFFSET_FRAME_LEN, total);
        buf.putInt(FrameLayout.OFFSET_PAYLOAD_LEN, payloadBytes.length);
        buf.putInt(FrameLayout.OFFSET_STORED_LEN, payloadBytes.length);
        buf.putInt(FrameLayout.OFFSET_CRC32C, 0);
        buf.putLong(FrameLayout.OFFSET_LSN, lsn);
        buf.putLong(FrameLayout.OFFSET_TIMESTAMP_NANOS, 1_700_000_000_000_000_000L);
        buf.putInt(FrameLayout.OFFSET_PARTITION_ID, pid);
        buf.putInt(FrameLayout.OFFSET_CODEC_ID, 0);
        buf.putLong(FrameLayout.OFFSET_ATTRIBUTES, 0L);
        buf.putInt(FrameLayout.OFFSET_RESERVED0, 0);
        buf.putInt(FrameLayout.OFFSET_RESERVED1, 0);
        for (int i = 0; i < payloadBytes.length; i++) {
            buf.put(headerLen + i, payloadBytes[i]);
        }
        for (int i = frameLen; i < total; i++) {
            buf.put(i, (byte) 0);
        }
        int crc = Crc32cUtil.computeFrame(buf, 0, total);
        buf.putInt(FrameLayout.OFFSET_CRC32C, crc);
        return buf;
    }

    // ------------------------------------------------------------------
    // 验收点 ⑤ recordType = 200 且无处理器 -> 跳过整帧不抛异常
    // ------------------------------------------------------------------

    @Test
    @DisplayName("⑤ recordType=200 未知类型：跳过整帧、不抛异常、不报损坏")
    void unknownRecordTypeIsSkipped() {
        // 构造 3 帧连续写入：已知帧 -> recordType=200 -> 已知帧
        int size = 64;
        ByteBuffer known1 = encodeFrame(size, 1L, 0, AppendOptions.DEFAULTS);
        ByteBuffer unknown = encodeFrame(size, 2L, 0, AppendOptions.of((byte) 200));
        ByteBuffer known3 = encodeFrame(size, 3L, 0, AppendOptions.DEFAULTS);

        int total = known1.capacity() + unknown.capacity() + known3.capacity();
        ByteBuffer stream = ByteBuffer.allocateDirect(total);
        int off = 0;
        for (ByteBuffer f : new ByteBuffer[]{known1, unknown, known3}) {
            ByteBuffer v = f.duplicate();
            Memory_copy(stream, off, v);
            off += f.capacity();
        }

        DecodedFrame out = new DecodedFrame();
        int off2 = 0;
        int st = decoder.decode(stream, off2, total - off2, out, null);
        assertEquals(known1.capacity(), st);
        assertFalse(out.skipped);
        assertEquals(1L, out.lsn);
        off2 += st;

        st = decoder.decode(stream, off2, total - off2, out, null);
        assertEquals(unknown.capacity(), st, "未知类型必须按 frameLen 跳过（返回帧长而非错误）");
        assertTrue(out.skipped, "未知类型必须标记 skipped=true");
        assertNull(out.payload);
        assertEquals(2L, out.lsn, "被跳过的帧仍应回吐可读的公共字段");
        off2 += st;

        st = decoder.decode(stream, off2, total - off2, out, null);
        assertEquals(known3.capacity(), st);
        assertFalse(out.skipped);
        assertEquals(3L, out.lsn);
    }

    /**
     * 把一帧从 {@code src} 拷贝到 {@code dst} 的绝对偏移处。
     *
     * @param dst 目标
     * @param dstOffset 目标偏移
     * @param frame 帧（读其 remaining）
     */
    private static void Memory_copy(ByteBuffer dst, int dstOffset, ByteBuffer frame) {
        com.walcore.internal.io.MemoryUtils.copy(dst, dstOffset, frame, frame.position(), frame.remaining());
    }

    // ------------------------------------------------------------------
    // 验收点 ⑥ flags bit6/bit7 置位时解码不失败
    // ------------------------------------------------------------------

    @Test
    @DisplayName("⑥ flags 的 bit6/bit7 置位时解码不失败（读者必须忽略未知位）")
    void unknownFlagBitsAreIgnored() {
        ByteBuffer frame = encodeFrame(100, 88L, 4, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        // 直接改写帧内的 flags 字节，并重算 CRC（模拟"未来版本写了新位"的合法帧）
        ByteBuffer tampered = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer v = frame.duplicate();
        v.position(0);
        v.limit(frameLen);
        tampered.put(v);
        byte flags = tampered.get(FrameLayout.OFFSET_FLAGS);
        tampered.put(FrameLayout.OFFSET_FLAGS, (byte) (flags | 0xC0));
        int crc = Crc32cUtil.computeFrame(tampered, 0, frameLen);
        tampered.putInt(FrameLayout.OFFSET_CRC32C, crc);

        DecodedFrame out = new DecodedFrame();
        assertEquals(frameLen, decoder.decode(tampered, 0, frameLen, out, null),
                "flags bit6/bit7 必须被忽略，不得拒绝该帧");
        assertEquals(0xC0, out.flags & 0xC0);
    }

    @Test
    @DisplayName("⑥-补充：version=2 的帧在 allowForwardCompatibleRead=true 时可读，false 时被拒")
    void forwardCompatibleVersion() {
        ByteBuffer frame = encodeFrame(64, 5L, 0, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        ByteBuffer upgraded = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer v = frame.duplicate();
        v.position(0);
        v.limit(frameLen);
        upgraded.put(v);
        upgraded.put(FrameLayout.OFFSET_VERSION, (byte) 2);
        upgraded.putInt(FrameLayout.OFFSET_CRC32C, Crc32cUtil.computeFrame(upgraded, 0, frameLen));

        DecodedFrame out = new DecodedFrame();
        assertEquals(frameLen, decoder.decode(upgraded, 0, frameLen, out, null));
        assertEquals(2, out.version);

        FrameDecoder strict = new FrameDecoder(codec, false);
        assertEquals(FrameDecoder.STATUS_INCOMPATIBLE_VERSION,
                strict.decode(upgraded, 0, frameLen, out, null));
    }

    // ------------------------------------------------------------------
    // 补充：不完整帧、magic 错误、AppendOptions 语义
    // ------------------------------------------------------------------

    @Test
    @DisplayName("不完整帧返回 STATUS_INCOMPLETE，magic 错返回 STATUS_BAD_HEADER")
    void incompleteAndBadHeader() {
        ByteBuffer frame = encodeFrame(256, 1L, 0, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        DecodedFrame out = new DecodedFrame();

        for (int avail : new int[]{0, 1, 63, 64, frameLen - 1}) {
            assertEquals(FrameDecoder.STATUS_INCOMPLETE,
                    decoder.decode(frame, 0, avail, out, null),
                    "剩余 " + avail + " 字节应判为不完整");
        }
        byte[] bytes = new byte[frameLen];
        frame.duplicate().get(bytes);
        bytes[0] = 0;
        assertEquals(FrameDecoder.STATUS_BAD_HEADER,
                decoder.decode(ByteBuffer.wrap(bytes), 0, frameLen, out, null));
    }

    @Test
    @DisplayName("frameLen 非 8 倍数 / 越过上限 一律判为帧头非法")
    void invalidFrameLenRejected() {
        ByteBuffer frame = encodeFrame(64, 1L, 0, AppendOptions.DEFAULTS);
        int frameLen = frame.capacity();
        DecodedFrame out = new DecodedFrame();

        ByteBuffer bad1 = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer v1 = frame.duplicate();
        v1.position(0);
        v1.limit(frameLen);
        bad1.put(v1);
        bad1.putInt(FrameLayout.OFFSET_FRAME_LEN, frameLen + 1);
        bad1.putInt(FrameLayout.OFFSET_CRC32C, Crc32cUtil.computeFrame(bad1, 0, frameLen));
        assertEquals(FrameDecoder.STATUS_BAD_HEADER,
                decoder.decode(bad1, 0, frameLen, out, null), "非 8 倍数必须拒绝");

        ByteBuffer bad2 = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer v2 = frame.duplicate();
        v2.position(0);
        v2.limit(frameLen);
        bad2.put(v2);
        bad2.putInt(FrameLayout.OFFSET_FRAME_LEN, FrameLayout.MAX_FRAME_LEN + 8);
        bad2.putInt(FrameLayout.OFFSET_CRC32C, Crc32cUtil.computeFrame(bad2, 0, frameLen));
        assertEquals(FrameDecoder.STATUS_BAD_HEADER,
                decoder.decode(bad2, 0, frameLen, out, null), "超过上限必须拒绝");
    }

    @Test
    @DisplayName("AppendOptions：幂等键写入 attributes 并置位 flags.IDEMPOTENT_KEY")
    void appendOptionsFlagsAndAttributes() {
        long keyHash = 0x0123456789ABCDEFL;
        AppendOptions opts = AppendOptions.idempotent(keyHash);
        ByteBuffer frame = encodeFrame(32, 1L, 0, opts);
        int frameLen = frame.capacity();

        assertEquals(FrameLayout.Flags.IDEMPOTENT_KEY, frame.get(FrameLayout.OFFSET_FLAGS));
        assertEquals(keyHash, frame.getLong(FrameLayout.OFFSET_ATTRIBUTES));

        DecodedFrame out = new DecodedFrame();
        assertEquals(frameLen, decoder.decode(frame, 0, frameLen, out, null));
        assertEquals(keyHash, out.attributes);

        // WalRecord 侧的语义
        WalRecord record = new WalRecord();
        record.fill(out.lsn, out.timestampNanos, out.partitionId, (byte) out.recordType,
                (byte) out.flags, out.codecId, out.attributes, out.payloadLen, out.storedLen, out.payload);
        assertTrue(record.hasIdempotentKey());
        assertFalse(record.isTxnCommit());
        assertEquals(keyHash, record.attributes());
    }

    @Test
    @DisplayName("AppendOptions 默认值不置位任何 flags")
    void defaultOptionsHaveNoFlags() {
        assertEquals(0, AppendOptions.DEFAULTS.flags());
        assertSame(AppendOptions.DEFAULTS, AppendOptions.defaults());
        assertEquals(0L, AppendOptions.DEFAULTS.effectiveAttributes());
        ByteBuffer frame = encodeFrame(16, 1L, 0, AppendOptions.DEFAULTS);
        assertEquals(0, frame.get(FrameLayout.OFFSET_FLAGS));
    }

    @Test
    @DisplayName("RAW codec 的 maxStoredSize 为恒等映射，容量不足返回 -1")
    void rawCodecCapacityContract() {
        assertEquals(0, codec.codecId());
        assertEquals("builtin:raw", codec.id());
        assertEquals(1234, codec.maxStoredSize(1234));
        assertEquals(0, codec.maxStoredSize(0));

        ByteBuffer src = payload(100);
        ByteBuffer dst = ByteBuffer.allocate(200);
        assertEquals(-1, codec.encode(src, 0, 100, dst, 0, 99), "容量不足应返回 -1");
        assertEquals(100, codec.encode(src, 0, 100, dst, 0, 100));
    }

    @Test
    @DisplayName("FrameDecoder.validateHeaderCondition 只做结构校验")
    void headerConditionValidation() {
        ByteBuffer frame = encodeFrame(64, 1L, 0, AppendOptions.DEFAULTS);
        assertTrue(decoder.validateHeaderCondition(frame));

        byte[] bytes = new byte[frame.capacity()];
        frame.duplicate().get(bytes);
        bytes[0] = 1;
        assertFalse(decoder.validateHeaderCondition(ByteBuffer.wrap(bytes)));
    }

    @Test
    @DisplayName("LatencyHistogram 分位数在桶内插值，误差可控")
    void latencyHistogramPercentiles() {
        LatencyHistogram h = new LatencyHistogram();
        // 1000 个样本全部落在 5µs..10µs 桶内（5001..10000ns），p50 应落在这附近
        for (int i = 1; i <= 1000; i++) {
            h.record(5_000L + i * 5L);
        }
        assertEquals(1000L, h.count());
        long p50 = h.p50();
        assertTrue(p50 >= 5_000L && p50 <= 10_000L, "p50 越界: " + p50);
        assertTrue(Math.abs(p50 - 7_500L) < 400L, "p50 插值误差过大: " + p50);
        assertEquals(0L, new LatencyHistogram().p50(), "无样本时返回 0");
    }
}
