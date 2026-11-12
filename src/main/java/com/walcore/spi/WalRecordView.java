package com.walcore.spi;

import java.nio.ByteBuffer;

/**
 * 复制端看到的<b>只读记录视图</b>。
 *
 * <p>为什么不直接把 {@code WalRecord} 交出去：{@code WalRecord} 是<b>可复用</b>对象
 * （同一个实例会被后续记录覆盖），复制实现若把它存进队列，稍后读到的就是别的记录的内容——
 * 这是一个只在"复制有积压"时才出现的隐蔽 bug。用只读视图 + 明确的生命周期约束，
 * 把"必须自己拷贝"这件事写在类型上。
 */
public interface WalRecordView {

    /**
     * 全局 LSN。
     *
     * @return LSN
     */
    long lsn();

    /**
     * 写入时刻（{@code System.nanoTime} 口径）。
     *
     * @return 纳秒
     */
    long timestampNanos();

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    int partitionId();

    /**
     * 业务记录类型。
     *
     * @return 类型码
     */
    byte recordType();

    /**
     * payload 逻辑长度。
     *
     * @return 字节数
     */
    int payloadLength();

    /**
     * payload 只读视图。
     *
     * <p><b>生命周期仅限 {@link ReplicationSink#onRecord} 调用期间</b>，需要保留请自行拷贝。
     *
     * @return 缓冲区（只读）
     */
    ByteBuffer payload();

    /**
     * 8 字节业务属性（如幂等 key 的哈希）。
     *
     * @return 属性值
     */
    long attributes();

    /**
     * 编码器数值 ID。
     *
     * @return codec id
     */
    int codecId();
}
