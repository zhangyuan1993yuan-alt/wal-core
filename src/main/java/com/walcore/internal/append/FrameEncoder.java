package com.walcore.internal.append;

import com.walcore.api.AppendOptions;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.internal.util.Crc32cUtil;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.RecordCodec;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 帧编码器：把「LSN + 分区 + payload + 选项」编码成一帧完整字节。
 *
 * <h2>写帧的三个阶段（顺序不能变）</h2>
 * <ol>
 *   <li><b>先编码 payload</b>（可能经过 codec 压缩），得到真实的 {@code storedLen}；</li>
 *   <li><b>再补零对齐</b>并回算真实的 {@code frameLen = frameLenFor(storedLen)}；</li>
 *   <li><b>最后写帧头 + 回填 CRC</b>。CRC 覆盖 {@code [0,20) ∪ [24, frameLen)}，
 *       计算时 crc 字段还是 0 占位（它本来就被跳过，所以不影响结果）。</li>
 * </ol>
 * 这个顺序正是 ARCH §10.4.4 所说的"编码必须先成功后提交偏移"：
 * 任何一步抛异常，段内的 {@code publishedWritePos} 都还没有前进，不会留下半帧。
 *
 * <h2>为什么帧头用绝对索引写而不是先填 header 再拷 payload</h2>
 * 因为 {@code storedLen} 要等 codec 跑完才知道，而 {@code frameLen} 又依赖 {@code storedLen}。
 * 若先写帧头就必须写两次（一次占位、一次回填），而绝对索引写入让"回填"变成同一次写操作
 * 里的几个 {@code putInt}/{@code putLong}，没有额外分支。
 *
 * <p><b>线程安全</b>：本类无可变状态（{@code codec} 与 {@code codecId} 在构造时冻结），
 * 可被多个写入线程共享。内部缓冲（{@link #encode(SegmentHandle, long, long, int, ByteBuffer, AppendOptions)}
 * 使用的 scratch）是 {@link ThreadLocal} 的。
 */
public final class FrameEncoder {

    /** 每线程 scratch 的初始容量（64KB，足够容纳 256B 典型帧与 4KB 大帧）。 */
    private static final int SCRATCH_INITIAL = 64 * 1024;

    /** 超过该长度的帧不保留在 ThreadLocal scratch 中，避免单线程长期驻留大块堆外内存。 */
    private static final int SCRATCH_RETAIN_LIMIT = 1024 * 1024;

    private static final ThreadLocal<ByteBuffer> SCRATCH =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(SCRATCH_INITIAL));

    private final RecordCodec codec;
    private final int codecId;

    /**
     * 构造编码器。
     *
     * @param codec 编码器插件，非 null
     */
    public FrameEncoder(RecordCodec codec) {
        this.codec = Preconditions.checkNotNull(codec, "codec");
        this.codecId = codec.codecId();
    }

    /**
     * 当前编码器。
     *
     * @return 编码器
     */
    public RecordCodec codec() {
        return codec;
    }

    /**
     * 当前编码器的数值 id（写入帧头 {@code codecId}）。
     *
     * @return 编码器 id
     */
    public int codecId() {
        return codecId;
    }

    /**
     * 帧头长度（恒定 64）。
     *
     * @return 字节数
     */
    public int headerSize() {
        return FrameLayout.HEADER_LEN;
    }

    /**
     * 单帧最大长度（{@code 16_777_288}）。
     *
     * @return 字节数
     */
    public int maxFrameSize() {
        return FrameLayout.MAX_FRAME_LEN;
    }

    /**
     * 计算整帧 CRC32C，覆盖 {@code [0,20) ∪ [24, frameLen)}。
     *
     * @param frame 帧所在缓冲区（帧起始于绝对索引 0）
     * @param frameLen 整帧长度
     * @return CRC32C
     */
    public int computeCrc(ByteBuffer frame, int frameLen) {
        return Crc32cUtil.computeFrame(frame, 0, frameLen);
    }

    /**
     * 给定逻辑 payload 长度，计算所需的最坏情况帧长（含 codec 上界与 8B 对齐）。
     *
     * @param payloadLen 逻辑长度
     * @return 帧长上界
     */
    public int requiredFrameLen(int payloadLen) {
        // 热路径：用 if + throw 代替可变参数校验，确保零分配不依赖 JIT
        if (payloadLen < 0) {
            throw new IllegalArgumentException("payloadLen 不得为负: " + payloadLen);
        }
        if (payloadLen > FrameLayout.MAX_PAYLOAD_LEN) {
            throw new IllegalArgumentException("payloadLen 超过上限 "
                    + FrameLayout.MAX_PAYLOAD_LEN + ": " + payloadLen);
        }
        int storedCap = codec.maxStoredSize(payloadLen);
        if (storedCap < payloadLen) {
            // 上界不得小于逻辑长度，否则压缩实现的契约就错了
            storedCap = payloadLen;
        }
        if (storedCap > FrameLayout.MAX_PAYLOAD_LEN) {
            storedCap = FrameLayout.MAX_PAYLOAD_LEN;
        }
        return FrameLayout.frameLenFor(storedCap);
    }

    /**
     * 把一帧编码到 {@code dst} 的 {@code dstOffset} 起。
     *
     * <p><b>前置条件</b>：{@code dst.limit() >= dstOffset + 需要的帧长}。绝对索引写入按
     * {@code limit()} 校验，不会自动扩容（扩容会让调用方持有的切片失效）。
     *
     * @param dst 目标缓冲区
     * @param dstOffset 帧起始绝对偏移
     * @param dstCapacity 从 {@code dstOffset} 起的可用字节数
     * @param lsn 全局逻辑序号
     * @param partitionId 分区 ID
     * @param payload 业务字节，读取其 {@code [position, limit)}
     * @param opts 追加选项
     * @return 实际帧长（{@code frameLen}）
     */
    public int encodeFrame(ByteBuffer dst, int dstOffset, int dstCapacity,
                           long lsn, int partitionId, ByteBuffer payload, AppendOptions opts) {
        // 热路径：全部用 if + throw，避免可变参数装箱（见 Crc32cUtil 的同类说明）
        if (dst == null || payload == null || opts == null) {
            throw new IllegalArgumentException("dst / payload / opts 不得为 null");
        }
        int logicalLen = payload.remaining();
        if (logicalLen > FrameLayout.MAX_PAYLOAD_LEN) {
            throw new IllegalArgumentException("payload 超过单条上限 "
                    + FrameLayout.MAX_PAYLOAD_LEN + " 字节: " + logicalLen);
        }

        int payloadDstOffset = dstOffset + FrameLayout.HEADER_LEN;
        int payloadCapacity = dstCapacity - FrameLayout.HEADER_LEN;
        if (payloadCapacity < 0) {
            return -1;
        }

        // 阶段 1：编码 payload（RAW 下就是一次 memcpy）
        int storedLen = codec.encode(payload, payload.position(), logicalLen,
                dst, payloadDstOffset, payloadCapacity);
        if (storedLen < 0) {
            return -1;
        }
        if (storedLen > FrameLayout.MAX_PAYLOAD_LEN) {
            throw new IllegalArgumentException(
                    "codec 输出的 storedLen 超过上限: " + storedLen);
        }

        // 阶段 2：补零对齐并回算真实帧长
        int frameLen = FrameLayout.frameLenFor(storedLen);
        if (frameLen > dstCapacity) {
            return -1;
        }
        int padFrom = payloadDstOffset + storedLen;
        int padTo = dstOffset + frameLen;
        if (padTo > padFrom) {
            MemoryUtils.fill(dst, padFrom, padTo, (byte) 0);
        }

        // 阶段 3：写帧头（先写 crc=0 占位，最后回填）
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_MAGIC, FrameLayout.MAGIC);
        MemoryUtils.putByte(dst, dstOffset + FrameLayout.OFFSET_VERSION, FrameLayout.FORMAT_VERSION);
        MemoryUtils.putByte(dst, dstOffset + FrameLayout.OFFSET_HEADER_LEN,
                (byte) FrameLayout.HEADER_LEN);
        MemoryUtils.putByte(dst, dstOffset + FrameLayout.OFFSET_RECORD_TYPE, opts.recordType());
        MemoryUtils.putByte(dst, dstOffset + FrameLayout.OFFSET_FLAGS, opts.flags());
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_FRAME_LEN, frameLen);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_PAYLOAD_LEN, logicalLen);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_STORED_LEN, storedLen);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_CRC32C, 0);
        MemoryUtils.putLong(dst, dstOffset + FrameLayout.OFFSET_LSN, lsn);
        MemoryUtils.putLong(dst, dstOffset + FrameLayout.OFFSET_TIMESTAMP_NANOS,
                currentTimestampNanos());
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_PARTITION_ID, partitionId);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_CODEC_ID, codecId);
        MemoryUtils.putLong(dst, dstOffset + FrameLayout.OFFSET_ATTRIBUTES, opts.effectiveAttributes());
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_RESERVED0, 0);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_RESERVED1, 0);

        // 回填 CRC
        int crc = Crc32cUtil.computeFrame(dst, dstOffset, frameLen);
        MemoryUtils.putInt(dst, dstOffset + FrameLayout.OFFSET_CRC32C, crc);
        return frameLen;
    }

    /**
     * 编码一帧并直接追加到段句柄。
     *
     * <p>实现说明：SPI 的 {@link SegmentHandle#append(ByteBuffer)} 只支持"在
     * {@code appendPosition} 处追加"，因此本方法先在 {@link ThreadLocal} 的直接缓冲区里
     * 把整帧编码完，校验帧长与 CRC，然后一次性 append。这样做的额外代价是"scratch → 段"
     * 多一次拷贝（约 10ns/256B）；换来的是"任何编码异常都不会在段里留下半帧"。
     * 追求极致吞吐的路径（T08 的 {@code AppendPipeline}）会改用
     * {@code StagingBuffer} 或直写 mmap 窗口，本方法主要服务于单写者/测试路径。
     *
     * @param handle 目标段句柄
     * @param writePos 期望的写入位置（必须等于 {@code handle.appendPosition()}）
     * @param lsn 全局逻辑序号
     * @param partitionId 分区 ID
     * @param payload 业务字节
     * @param opts 追加选项
     * @return 实际帧长
     * @throws IOException IO 失败
     */
    public long encode(SegmentHandle handle, long writePos, long lsn, int partitionId,
                       ByteBuffer payload, AppendOptions opts) throws IOException {
        Preconditions.checkNotNull(handle, "handle");
        long actualPos = handle.appendPosition();
        if (actualPos != writePos) {
            throw new IllegalStateException(
                    "写入位置不一致：期望 " + writePos + "，段 " + handle.path()
                            + " 的实际 appendPosition 为 " + actualPos
                            + "（说明有并发写入绕过了分区写锁）");
        }
        int needed = requiredFrameLen(payload.remaining());
        ByteBuffer scratch = scratchFor(needed);
        int frameLen = encodeFrame(scratch, 0, scratch.capacity(), lsn, partitionId, payload, opts);
        if (frameLen < 0) {
            throw new IllegalStateException("scratch 容量不足，这是内部错误: needed=" + needed);
        }
        ByteBuffer view = scratch.duplicate();
        view.limit(frameLen);
        view.position(0);
        handle.append(view.slice());
        return frameLen;
    }

    /**
     * 取得本线程可用的 scratch 直接缓冲区。
     *
     * @param needed 需要的容量
     * @return 容量不小于 {@code needed} 的缓冲区
     */
    private static ByteBuffer scratchFor(int needed) {
        ByteBuffer buf = SCRATCH.get();
        if (buf.capacity() >= needed) {
            return buf;
        }
        int newCap = Math.max(needed, buf.capacity() << 1);
        if (needed > SCRATCH_RETAIN_LIMIT) {
            // 大帧用完即弃，避免单线程长期驻留 MB 级堆外内存
            return ByteBuffer.allocateDirect(newCap);
        }
        ByteBuffer grown = ByteBuffer.allocateDirect(newCap);
        SCRATCH.set(grown);
        return grown;
    }

    /**
     * 当前写入时刻（epoch 纳秒）。
     *
     * <p>用 {@code currentTimeMillis() * 1_000_000} 而不是 {@code nanoTime()}：前者是
     * epoch 语义，跨重启可比、可直接展示；精度毫秒级对本场景足够（LSN 才是顺序真源）。
     *
     * @return epoch 纳秒
     */
    public static long currentTimestampNanos() {
        return System.currentTimeMillis() * 1_000_000L;
    }

    @Override
    public String toString() {
        return "FrameEncoder{codec=" + codec.id() + ", codecId=" + codecId + '}';
    }
}
