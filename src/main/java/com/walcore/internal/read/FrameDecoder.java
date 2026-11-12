package com.walcore.internal.read;

import com.walcore.api.WalException;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.internal.util.Crc32cUtil;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.RecordCodec;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 帧解码器：从字节流中解析出一帧，并完成<b>全部</b>安全校验。
 *
 * <h2>前向兼容的四个承诺（逐条落在代码里）</h2>
 * <ol>
 *   <li><b>所有偏移都从 {@link FrameLayout} 取</b>，本文件里不出现硬编码数字偏移。
 *       这样才能保证"改一处真源，全链路一致"。</li>
 *   <li><b>{@code headerLen > 64} 时按 {@code headerLen} 跳过未知头部</b>：
 *       payload 起点是 {@code frameOffset + headerLen}，<b>不是</b>
 *       {@code frameOffset + 64}。未来版本在保留字段区加字段时，老读者仍能读。</li>
 *   <li><b>{@code recordType >= 128} 且无处理器时整帧跳过</b>：返回 {@code frameLen}
 *       （不是错误码），{@link DecodedFrame#skipped} 置 true。不抛异常、不算损坏。</li>
 *   <li><b>{@code flags} 的 bit6~7 与未来位一律忽略</b>：绝不用"未知 flags"拒绝一帧。</li>
 * </ol>
 *
 * <h2>校验顺序（先验"边界"，再验"内容"，最后才"信任字段"）</h2>
 * magic → headerLen → frameLen 合法性/对齐 → 完整性 → CRC → 版本 → payload/stored 一致性 → 填字段。
 * 之所以在 CRC 之前只做"结构性"校验，是因为前四项不依赖任何可疑字段（magic 是常量、
 * headerLen/frameLen 只做区间判断），而 payloadLen/storedLen 之类的字段只有在 CRC 通过后
 * 才值得信任。
 */
public final class FrameDecoder {

    /** 剩余字节不足一帧（需要更多数据）。 */
    public static final int STATUS_INCOMPLETE = 0;
    /** 帧头非法（magic 错 / headerLen 错 / frameLen 越界或不齐 / payloadLen 与 storedLen 矛盾）。 */
    public static final int STATUS_BAD_HEADER = -1;
    /** CRC32C 不匹配（数据位翻转、介质损坏、半写）。 */
    public static final int STATUS_CRC_MISMATCH = -2;
    /** 未知 codecId，且不满足"降级按 RAW 读"的条件。 */
    public static final int STATUS_UNKNOWN_CODEC = -3;
    /** 帧版本高于本核心库且不允许前向兼容读。 */
    public static final int STATUS_INCOMPATIBLE_VERSION = -4;
    /** 需要 codec 解码但调用方未提供足够大的 {@code payloadSink}。 */
    public static final int STATUS_PAYLOAD_SINK_TOO_SMALL = -5;

    private static final ThreadLocal<ByteBuffer> HEADER_SCRATCH =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(FrameLayout.HEADER_LEN));

    private final RecordCodec codec;
    private final int codecId;
    private final boolean allowForwardCompatibleRead;

    /**
     * 构造解码器。
     *
     * @param codec 主编码器（{@code codecId} 与之相等时走 codec 解码路径）
     * @param allowForwardCompatibleRead 是否允许读 {@code version > 1} 的帧
     */
    public FrameDecoder(RecordCodec codec, boolean allowForwardCompatibleRead) {
        this.codec = Preconditions.checkNotNull(codec, "codec");
        this.codecId = codec.codecId();
        this.allowForwardCompatibleRead = allowForwardCompatibleRead;
    }

    /**
     * 主编码器。
     *
     * @return 编码器
     */
    public RecordCodec codec() {
        return codec;
    }

    /**
     * 仅校验帧头结构（magic / headerLen / frameLen），不做 CRC、不看 payload。
     *
     * <p>供恢复期"只做帧边界校验"的快速路径使用（{@code recoveryVerifyMode=NONE} 或
     * {@code cleanShutdown=true}）。注意：本方法<b>不保证</b>帧内容正确，只保证
     * "按这个 frameLen 推进不会走进垃圾区"。
     *
     * @param hdr 帧头所在缓冲区，帧起始于绝对索引 0，且至少可读 {@code HEADER_LEN} 字节
     * @return 帧头结构可用返回 true
     */
    public boolean validateHeaderCondition(ByteBuffer hdr) {
        if (hdr == null || hdr.limit() < FrameLayout.HEADER_LEN) {
            return false;
        }
        if (hdr.getInt(FrameLayout.OFFSET_MAGIC) != FrameLayout.MAGIC) {
            return false;
        }
        int headerLen = MemoryUtils.getUnsignedByte(hdr, FrameLayout.OFFSET_HEADER_LEN);
        if (!FrameLayout.isReadableHeaderLen(headerLen)) {
            return false;
        }
        int frameLen = hdr.getInt(FrameLayout.OFFSET_FRAME_LEN);
        return FrameLayout.isValidFrameLen(frameLen) && frameLen >= headerLen;
    }

    // ------------------------------------------------------------------
    // 主解码入口：5 个参数（TASKS-wal.md 铁律 7 明确要求的签名形态）
    // ------------------------------------------------------------------

    /**
     * 解码 {@code src} 中从 {@code frameOffset} 开始的一帧。
     *
     * <p><b>无分配，可复用</b>：结果写进调用方提供的 {@code out}，返回值是状态码或帧长。
     * 热路径（游标顺序读、恢复扫描）必须用本重载。
     *
     * @param src 帧所在缓冲区（只按绝对索引读，不改 position/limit）
     * @param frameOffset 帧起始绝对偏移
     * @param available 从 {@code frameOffset} 起可读的字节数
     * @param out 结果容器
     * @param payloadSink 非 RAW 编码时的解码目标缓冲区；RAW 路径可为 null（零拷贝）
     * @return 成功时返回 {@code frameLen}（&gt; 0）；失败/不完整返回下列负值或 0：
     *         {@link #STATUS_INCOMPLETE}、{@link #STATUS_BAD_HEADER}、
     *         {@link #STATUS_CRC_MISMATCH}、{@link #STATUS_UNKNOWN_CODEC}、
     *         {@link #STATUS_INCOMPATIBLE_VERSION}、{@link #STATUS_PAYLOAD_SINK_TOO_SMALL}
     */
    public int decode(ByteBuffer src, int frameOffset, int available,
                      DecodedFrame out, ByteBuffer payloadSink) {
        Preconditions.checkNotNull(src, "src");
        Preconditions.checkNotNull(out, "out");
        out.reset();

        int readable = Math.min(available, src.limit() - frameOffset);
        if (readable < FrameLayout.HEADER_LEN) {
            return STATUS_INCOMPLETE;
        }

        // ---- ① 结构性校验（不依赖任何可疑字段）----
        if (src.getInt(frameOffset + FrameLayout.OFFSET_MAGIC) != FrameLayout.MAGIC) {
            return STATUS_BAD_HEADER;
        }
        int version = MemoryUtils.getUnsignedByte(src, frameOffset + FrameLayout.OFFSET_VERSION);
        int headerLen = MemoryUtils.getUnsignedByte(src, frameOffset + FrameLayout.OFFSET_HEADER_LEN);
        if (!FrameLayout.isReadableHeaderLen(headerLen)) {
            return STATUS_BAD_HEADER;
        }
        if (headerLen > readable) {
            return STATUS_INCOMPLETE;
        }
        int frameLen = src.getInt(frameOffset + FrameLayout.OFFSET_FRAME_LEN);
        if (!FrameLayout.isValidFrameLen(frameLen) || frameLen < headerLen) {
            return STATUS_BAD_HEADER;
        }
        if (frameLen > readable) {
            return STATUS_INCOMPLETE;
        }

        // ---- ② 完整性已保证，校验 CRC ----
        int expectedCrc = src.getInt(frameOffset + FrameLayout.OFFSET_CRC32C);
        int actualCrc = Crc32cUtil.computeFrame(src, frameOffset, frameLen);
        if (expectedCrc != actualCrc) {
            return STATUS_CRC_MISMATCH;
        }

        // ---- ③ 版本兼容 ----
        if (version > FrameLayout.FORMAT_VERSION && !allowForwardCompatibleRead) {
            return STATUS_INCOMPATIBLE_VERSION;
        }
        if (version <= 0) {
            return STATUS_BAD_HEADER;
        }

        // ---- ④ payload / stored 一致性（CRC 已通过，可以信任这些字段）----
        int payloadLen = src.getInt(frameOffset + FrameLayout.OFFSET_PAYLOAD_LEN);
        int storedLen = src.getInt(frameOffset + FrameLayout.OFFSET_STORED_LEN);
        if (!FrameLayout.isValidPayloadLen(payloadLen) || !FrameLayout.isValidPayloadLen(storedLen)) {
            return STATUS_BAD_HEADER;
        }
        int padding = frameLen - headerLen - storedLen;
        if (padding < 0 || padding > FrameLayout.ALIGNMENT - 1) {
            return STATUS_BAD_HEADER;
        }

        int recordType = MemoryUtils.getUnsignedByte(src, frameOffset + FrameLayout.OFFSET_RECORD_TYPE);
        int flags = MemoryUtils.getUnsignedByte(src, frameOffset + FrameLayout.OFFSET_FLAGS);
        int frameCodecId = src.getInt(frameOffset + FrameLayout.OFFSET_CODEC_ID);

        // ---- ⑤ 未知 recordType：整帧跳过，不报错、不算损坏 ----
        if (recordType >= FrameLayout.RecordTypes.USER_MIN) {
            out.frameLen = frameLen;
            out.headerLen = headerLen;
            out.version = version;
            out.recordType = recordType;
            out.flags = flags;
            out.codecId = frameCodecId;
            out.payloadLen = payloadLen;
            out.storedLen = storedLen;
            out.lsn = src.getLong(frameOffset + FrameLayout.OFFSET_LSN);
            out.timestampNanos = src.getLong(frameOffset + FrameLayout.OFFSET_TIMESTAMP_NANOS);
            out.partitionId = src.getInt(frameOffset + FrameLayout.OFFSET_PARTITION_ID);
            out.attributes = src.getLong(frameOffset + FrameLayout.OFFSET_ATTRIBUTES);
            out.skipped = true;
            out.payload = null;
            return frameLen;
        }

        // ---- ⑥ payload 还原 ----
        // 关键：payload 起点是 frameOffset + headerLen，而不是 frameOffset + 64
        int payloadFrom = frameOffset + headerLen;
        boolean rawCompatible = frameCodecId == com.walcore.internal.spiimpl.RawRecordCodec.CODEC_ID
                && storedLen == payloadLen;
        if (!rawCompatible && frameCodecId == codecId) {
            if (payloadSink == null) {
                return STATUS_PAYLOAD_SINK_TOO_SMALL;
            }
            if (payloadSink.capacity() < payloadLen) {
                return STATUS_PAYLOAD_SINK_TOO_SMALL;
            }
            int written = codec.decode(src, payloadFrom, storedLen, payloadLen, payloadSink, 0);
            if (written != payloadLen) {
                return STATUS_BAD_HEADER;
            }
            out.payload = MemoryUtils.readOnlySlice(payloadSink, 0, payloadLen);
        } else if (rawCompatible) {
            out.payload = MemoryUtils.readOnlySlice(src, payloadFrom, payloadFrom + storedLen);
        } else if (storedLen == payloadLen
                && (flags & FrameLayout.Flags.COMPRESSED) == 0) {
            // 未知 codecId 的降级：未压缩且长度相等 -> 按 RAW 读（ARCH §4.2 最后一行）
            out.payload = MemoryUtils.readOnlySlice(src, payloadFrom, payloadFrom + storedLen);
        } else {
            return STATUS_UNKNOWN_CODEC;
        }

        out.frameLen = frameLen;
        out.headerLen = headerLen;
        out.version = version;
        out.recordType = recordType;
        out.flags = flags;
        out.codecId = frameCodecId;
        out.payloadLen = payloadLen;
        out.storedLen = storedLen;
        out.lsn = src.getLong(frameOffset + FrameLayout.OFFSET_LSN);
        out.timestampNanos = src.getLong(frameOffset + FrameLayout.OFFSET_TIMESTAMP_NANOS);
        out.partitionId = src.getInt(frameOffset + FrameLayout.OFFSET_PARTITION_ID);
        out.attributes = src.getLong(frameOffset + FrameLayout.OFFSET_ATTRIBUTES);
        out.skipped = false;
        return frameLen;
    }

    // ------------------------------------------------------------------
    // 段句柄解码（类图签名，便捷重载，每次分配一个 DecodedFrame）
    // ------------------------------------------------------------------

    /**
     * 从段句柄的指定偏移处读并解码一帧。
     *
     * <p>本重载为书写方便而分配一个 {@link DecodedFrame}，<b>不适用于</b>每秒数十万帧的
     * 热路径；热路径请用 {@link #decode(ByteBuffer, int, int, DecodedFrame, ByteBuffer)}。
     *
     * @param handle 段句柄
     * @param offset 帧起始文件偏移
     * @param work 工作缓冲区，容量必须 ≥ 该帧的 {@code frameLen}
     * @return 解码结果；剩余字节不足一帧时返回 null
     * @throws IOException IO 失败
     * @throws WalException.CorruptedException 帧头非法
     */
    public DecodedFrame decode(SegmentHandle handle, long offset, ByteBuffer work) throws IOException {
        Preconditions.checkNotNull(handle, "handle");
        Preconditions.checkNotNull(work, "work");

        ByteBuffer header = HEADER_SCRATCH.get();
        header.clear();
        header.limit(FrameLayout.HEADER_LEN);
        int readHeader = handle.read(offset, header, FrameLayout.HEADER_LEN);
        if (readHeader < FrameLayout.HEADER_LEN) {
            return null;
        }
        if (!validateHeaderCondition(header)) {
            throw new WalException.CorruptedException(
                    "帧头非法: segment=" + handle.path() + ", offset=" + offset
                            + ", magic=0x" + Integer.toHexString(header.getInt(FrameLayout.OFFSET_MAGIC)));
        }
        int frameLen = header.getInt(FrameLayout.OFFSET_FRAME_LEN);
        if (work.capacity() < frameLen) {
            throw new IllegalArgumentException(
                    "work 容量不足: need=" + frameLen + ", capacity=" + work.capacity());
        }
        work.clear();
        int read = handle.read(offset, work, frameLen);
        if (read < frameLen) {
            return null;
        }
        DecodedFrame out = new DecodedFrame();
        int status = decode(work, 0, frameLen, out, null);
        if (status == STATUS_CRC_MISMATCH) {
            throw new WalException.CorruptedException(
                    "CRC32C 不匹配: segment=" + handle.path() + ", offset=" + offset
                            + ", frameLen=" + frameLen);
        }
        if (status <= 0) {
            throw new WalException.CorruptedException(
                    "帧解码失败(status=" + status + "): segment=" + handle.path()
                            + ", offset=" + offset + ", frameLen=" + frameLen);
        }
        return out;
    }

    @Override
    public String toString() {
        return "FrameDecoder{codec=" + codec.id() + ", codecId=" + codecId
                + ", allowForwardCompatibleRead=" + allowForwardCompatibleRead + '}';
    }
}
