package com.walcore.internal.read;

import java.nio.ByteBuffer;

/**
 * 解码结果的<b>可复用</b>容器。
 *
 * <p>读取路径（游标顺序读 / 恢复扫描 / tailer）是全项目第二热的路径（仅次于 append），
 * 每帧都 new 一个小对象在 300MB/s 的顺序读下会变成 GC 压力来源。
 * 因此解码器不返回新对象，而是把结果写进调用方提供的 {@link DecodedFrame} 里。
 *
 * <p>字段直接 public：这些字段是给同包/内部模块读的，加 getter 只增加一次方法调用，
 * 而"内部数据载体"并不需要封装（对比 {@code WalRecord} 是对外 API，所以坚持用访问器）。
 */
public final class DecodedFrame {

    /** 整帧长度（帧头 + 存储 payload + 填充）。 */
    public int frameLen;

    /** 帧头长度（可能是 64，也可能是未来版本的 96）。 */
    public int headerLen;

    /** 格式版本。 */
    public int version;

    /** 记录类型。 */
    public int recordType;

    /** 原始 flags 字节。 */
    public int flags;

    /** 编码器 id。 */
    public int codecId;

    /** 逻辑 payload 长度。 */
    public int payloadLen;

    /** 实际存储 payload 长度（不含填充）。 */
    public int storedLen;

    /** 全局逻辑序号。 */
    public long lsn;

    /** 写入时刻（epoch 纳秒）。 */
    public long timestampNanos;

    /** 分区 ID。 */
    public int partitionId;

    /** 属性字段。 */
    public long attributes;

    /**
     * 是否因"未知 recordType"而被整帧跳过。
     *
     * <p>前向兼容承诺：{@code recordType >= 128} 且无注册处理器时，读者按 {@code frameLen}
     * 跳过整帧，<b>不报错、不算损坏</b>（ARCH §4.2）。
     */
    public boolean skipped;

    /**
     * payload 的只读视图。
     *
     * <p>RAW 编码下这是源缓冲区的零拷贝切片（不复制）；自定义 codec 下指向调用方提供的
     * {@code payloadSink}。生命周期：到下一次解码或源缓冲区失效为止。
     */
    public ByteBuffer payload;

    /**
     * 全部字段复位。
     */
    public void reset() {
        frameLen = 0;
        headerLen = 0;
        version = 0;
        recordType = 0;
        flags = 0;
        codecId = 0;
        payloadLen = 0;
        storedLen = 0;
        lsn = 0L;
        timestampNanos = 0L;
        partitionId = 0;
        attributes = 0L;
        skipped = false;
        payload = null;
    }

    /**
     * 尾部填充字节数（用于诊断；正常情况下恒在 0..7）。
     *
     * @return 填充字节数
     */
    public int paddingLen() {
        return frameLen - headerLen - storedLen;
    }

    @Override
    public String toString() {
        return "DecodedFrame{frameLen=" + frameLen
                + ", headerLen=" + headerLen
                + ", type=" + recordType
                + ", lsn=" + lsn
                + ", pid=" + partitionId
                + ", codecId=" + codecId
                + ", payloadLen=" + payloadLen
                + ", storedLen=" + storedLen
                + ", skipped=" + skipped
                + '}';
    }
}
