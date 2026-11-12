package com.walcore.api;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 一条已解码记录的只读值对象。
 *
 * <h2>可复用语义（务必看清，否则会踩"读到一半内容变了"的坑）</h2>
 * 为了满足"顺序读路径零分配"，{@code nextBatch(maxRecords, visitor)} 会<b>复用同一个</b>
 * {@code WalRecord} 实例逐条喂给 visitor，并在下一次 {@code next()} 时覆盖其内容。
 * 因此：
 * <ul>
 *   <li>在 {@code visitor.accept(record)} 内部读取字段是安全的；</li>
 *   <li>若要在回调之外持有，必须调用 {@link #copy()} —— 它会连同 payload 一起深拷贝；</li>
 *   <li>{@link #payload()} 返回的是<b>只读视图</b>，其底层存储可能是段文件的 mmap 片段，
 *       游标前进/段被 unmap 后该视图失效。</li>
 * </ul>
 *
 * <p>{@code setter} 系列标注为"框架内部使用"：它们是 public 的（因为解码器在
 * {@code com.walcore.internal.read} 包），但<b>不属于公共 API 契约</b>，使用方不应调用。
 */
public final class WalRecord {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private long lsn;
    private long timestampNanos;
    private int partitionId;
    private byte recordType;
    private byte flags;
    private int codecId;
    private long attributes;
    private int payloadLength;
    private int storedLength;
    private ByteBuffer payload = EMPTY;

    /**
     * 构造空记录。解码器会复用实例并逐个字段填充。
     */
    public WalRecord() {
        // 全部字段已有安全的默认值
    }

    // ------------------------------------------------------------------
    // 只读访问器（公共契约）
    // ------------------------------------------------------------------

    /**
     * 全局唯一、严格递增的逻辑序号。
     *
     * @return LSN
     */
    public long lsn() {
        return lsn;
    }

    /**
     * 写入时刻（epoch 纳秒，即 {@code System.currentTimeMillis() * 1_000_000}）。
     *
     * <p>用 epoch 语义而非 {@code nanoTime} 是为了跨重启可比、可展示。
     * LSN 与时钟完全解耦，因此时钟回拨不影响顺序性（ARCH §14 #14）。
     *
     * @return epoch 纳秒
     */
    public long timestampNanos() {
        return timestampNanos;
    }

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * 记录类型（{@code 1=DATA ... 128~255 用户自定义}）。
     *
     * @return 记录类型
     */
    public byte recordType() {
        return recordType;
    }

    /**
     * 帧头标志位原始字节。bit6~7 保留，读者应使用 {@code FrameLayout.Flags} 中的掩码判断。
     *
     * @return flags 字节
     */
    public byte flags() {
        return flags;
    }

    /**
     * 编码器 id（{@code 0 = RAW}）。
     *
     * @return codecId
     */
    public int codecId() {
        return codecId;
    }

    /**
     * 帧头复用字段：带幂等键时为 key 的 64 位 FNV-1a 摘要，事务场景为 txnId，否则为 0。
     *
     * @return 属性值
     */
    public long attributes() {
        return attributes;
    }

    /**
     * 逻辑 payload 长度（codec 解码前）。
     *
     * @return 字节数
     */
    public int payloadLength() {
        return payloadLength;
    }

    /**
     * 实际存储 payload 长度（codec 编码后，不含尾部填充）。
     *
     * @return 字节数
     */
    public int storedLength() {
        return storedLength;
    }

    /**
     * payload 的<b>只读</b>视图。生命周期见类注释。
     *
     * @return 只读缓冲区，无 payload 时为空缓冲区
     */
    public ByteBuffer payload() {
        return payload;
    }

    /**
     * 是否置位了幂等键标志。
     *
     * @return 置位返回 true
     */
    public boolean hasIdempotentKey() {
        return (flags & com.walcore.internal.append.FrameLayout.Flags.IDEMPOTENT_KEY) != 0;
    }

    /**
     * 是否为事务提交点。
     *
     * @return 是则返回 true
     */
    public boolean isTxnCommit() {
        return (flags & com.walcore.internal.append.FrameLayout.Flags.TXN_COMMIT) != 0;
    }

    // ------------------------------------------------------------------
    // 便捷方法
    // ------------------------------------------------------------------

    /**
     * 把 payload 拷贝成新数组（调试/测试/需要长期持有时使用）。
     *
     * @return 新数组
     */
    public byte[] payloadBytes() {
        ByteBuffer p = payload;
        byte[] out = new byte[p.remaining()];
        p.duplicate().get(out);
        return out;
    }

    /**
     * 把 payload 按 UTF-8 解码为字符串（调试用）。
     *
     * @return 字符串
     */
    public String payloadAsUtf8() {
        return new String(payloadBytes(), StandardCharsets.UTF_8);
    }

    /**
     * 深拷贝本条记录（含 payload 字节），可安全长期持有。
     *
     * @return 独立副本
     */
    public WalRecord copy() {
        WalRecord r = new WalRecord();
        r.lsn = lsn;
        r.timestampNanos = timestampNanos;
        r.partitionId = partitionId;
        r.recordType = recordType;
        r.flags = flags;
        r.codecId = codecId;
        r.attributes = attributes;
        r.payloadLength = payloadLength;
        r.storedLength = storedLength;
        if (payload == EMPTY || !payload.hasRemaining()) {
            r.payload = EMPTY;
        } else {
            byte[] bytes = payloadBytes();
            r.payload = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
        }
        return r;
    }

    // ------------------------------------------------------------------
    // 框架内部使用的填充方法（不属于公共契约）
    // ------------------------------------------------------------------

    /**
     * 清空为初始状态。<b>框架内部使用。</b>
     */
    public void reset() {
        lsn = 0L;
        timestampNanos = 0L;
        partitionId = 0;
        recordType = 0;
        flags = 0;
        codecId = 0;
        attributes = 0L;
        payloadLength = 0;
        storedLength = 0;
        payload = EMPTY;
    }

    /**
     * 填充字段。<b>框架内部使用。</b>
     *
     * @param newLsn LSN
     * @param newTimestampNanos 写入时刻
     * @param newPartitionId 分区 ID
     * @param newRecordType 记录类型
     * @param newFlags 标志位
     * @param newCodecId 编码器 id
     * @param newAttributes 属性字段
     * @param newPayloadLength 逻辑长度
     * @param newStoredLength 存储长度
     * @param newPayload payload 只读视图
     */
    public void fill(long newLsn, long newTimestampNanos, int newPartitionId,
                     byte newRecordType, byte newFlags, int newCodecId, long newAttributes,
                     int newPayloadLength, int newStoredLength, ByteBuffer newPayload) {
        this.lsn = newLsn;
        this.timestampNanos = newTimestampNanos;
        this.partitionId = newPartitionId;
        this.recordType = newRecordType;
        this.flags = newFlags;
        this.codecId = newCodecId;
        this.attributes = newAttributes;
        this.payloadLength = newPayloadLength;
        this.storedLength = newStoredLength;
        this.payload = newPayload == null ? EMPTY : newPayload;
    }

    @Override
    public String toString() {
        return "WalRecord{lsn=" + lsn
                + ", pid=" + partitionId
                + ", type=" + (recordType & 0xFF)
                + ", codecId=" + codecId
                + ", payloadLen=" + payloadLength
                + ", attributes=" + attributes
                + ", timestampNanos=" + timestampNanos
                + '}';
    }
}
