package com.walcore.spi;

import java.nio.ByteBuffer;

/**
 * ① 记录编码器 SPI。
 *
 * <p>编码器负责"业务字节 -&gt; 存储字节"的双向变换（压缩、加密、序列化均可）。
 * 每一帧的帧头都带 {@code codecId}，因此<b>换编码器不影响读旧数据</b>：
 * 旧帧仍按旧 {@code codecId} 解码（见 ARCH §10.4.5 的"能热切换"清单）。
 *
 * <h2>dstCapacity 的语义（容易看错）</h2>
 * {@link #encode} 的 {@code dstCapacity} 是"从 {@code dstOffset} 起可用的字节数"，
 * 不是 {@code dst.capacity()}。返回 {@code -1} 表示容量不足，框架会扩容后重试一次；
 * 因此实现<b>不得</b>在返回 -1 之前写入超过 {@code dstCapacity} 的字节（可写入部分数据，
 * 但必须保证不越界）。
 */
public interface RecordCodec extends WalPlugin {

    /**
     * 编解码器数值 ID，写入帧头 {@code codecId} 字段。
     *
     * <p>约定：{@code 0 = RAW}（保留，必须是恒等映射）；{@code 1~127} 内置保留；
     * {@code 128~255} 第三方。发布后<b>不得</b>复用或改动已分配的 id，否则会把历史数据解错。
     *
     * @return 编码器 id
     */
    int codecId();

    /**
     * 编码：把 {@code src} 的 {@code [srcOffset, srcOffset+srcLen)} 编码后写到
     * {@code dst} 的 {@code dstOffset} 起。
     *
     * @param src 源缓冲区（按绝对索引读取，实现不得依赖 position）
     * @param srcOffset 源起始绝对偏移
     * @param srcLen 源字节数
     * @param dst 目标缓冲区
     * @param dstOffset 目标起始绝对偏移
     * @param dstCapacity 从 {@code dstOffset} 起可用字节数
     * @return 实际写入字节数；容量不足返回 {@code -1}
     */
    int encode(ByteBuffer src, int srcOffset, int srcLen,
               ByteBuffer dst, int dstOffset, int dstCapacity);

    /**
     * 解码：读取 {@code src} 的 {@code [srcOffset, srcOffset+storedLen)}，还原出
     * {@code logicalLen} 字节写入 {@code dst} 的 {@code dstOffset} 起。
     *
     * @param src 源缓冲区
     * @param srcOffset 源起始绝对偏移
     * @param storedLen 已存储字节数
     * @param logicalLen 期望还原出的逻辑字节数
     * @param dst 目标缓冲区
     * @param dstOffset 目标起始绝对偏移
     * @return 实际写入字节数，<b>必须等于</b> {@code logicalLen}
     */
    int decode(ByteBuffer src, int srcOffset, int storedLen, int logicalLen,
               ByteBuffer dst, int dstOffset);

    /**
     * 给定逻辑长度，返回最坏情况下的存储长度上界。
     *
     * <p>框架据此预检缓冲区容量，避免"先编码失败再二次拷贝"的额外开销。
     * 对恒等映射返回 {@code logicalLen}；对压缩实现建议返回 {@code logicalLen}
     * （压缩后一般更小，上界取原长即可），对带固定头部的实现返回 {@code logicalLen + 头部长度}。
     *
     * @param logicalLen 逻辑 payload 长度
     * @return 存储长度上界
     */
    int maxStoredSize(int logicalLen);

    /**
     * 初始化时自检（如压缩库是否可用）。默认空实现。
     *
     * @throws PluginException 自检失败
     */
    default void selfCheck() throws PluginException {
        // 默认无需自检
    }
}
