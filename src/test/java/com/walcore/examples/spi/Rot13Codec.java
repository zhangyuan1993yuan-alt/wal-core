package com.walcore.examples.spi;

import com.walcore.api.WalVersion;
import com.walcore.spi.PluginContext;
import com.walcore.spi.RecordCodec;

import java.nio.ByteBuffer;

/**
 * 示例插件 ②：<b>ROT13 编码器</b>——真正做变换的编码器（不是恒等映射）。
 *
 * <h2>这个例子要教会什么</h2>
 * <ul>
 *   <li><b>编码 ≠ 压缩</b>：{@code storedLen} 与 {@code logicalLen} 可以不同。
 *       本实现是"逐字节 +13"，长度不变；但思路同样适用于"长度会变的编码"——
 *       只要 {@code maxStoredSize} 如实给出上界即可。</li>
 *   <li><b>幂等性与可逆性</b>：ROT13 的 encode/decode 是同一个变换（对字母自逆）。
 *       框架要求 {@code decode(encode(x)) == x}，因此这里显式写成对称实现。</li>
 *   <li><b>危险动作演示</b>：这个编码器会让<b>磁盘上的日志不可直接读</b>。
 *       一旦写入，之后不带同一插件打开就解不出来。因此生产环境在切换 codec 前
 *       必须确认"所有读取方都能加载该插件"——这是"codec 是持久化格式的一部分"的直接后果。</li>
 * </ul>
 *
 * <p>用途仅限演示：ROT13 不是加密，也不提供任何完整性保护（完整性由帧头 CRC32C 负责）。
 */
public final class Rot13Codec implements RecordCodec {

    /** 插件 id。 */
    public static final String ID = "example:codec-rot13";

    /**
     * 无参构造（{@code ServiceLoader} 要求）。
     */
    public Rot13Codec() {
        // 无状态，可被多线程并发使用
    }

    @Override
    public String id() {
        return ID;
    }

    /**
     * 帧头数值 id：第三方区间（128~255）内的另一个值。
     *
     * @return 129
     */
    @Override
    public int codecId() {
        return 129;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext pluginContext) {
        // ROT13 无需配置；保留空实现是为了让示例结构与其他插件一致
    }

    @Override
    public void close() {
        // 无资源
    }

    @Override
    public int encode(ByteBuffer src, int srcOffset, int srcLen,
                      ByteBuffer dst, int dstOffset, int dstCapacity) {
        if (srcLen < 0) {
            throw new IllegalArgumentException("srcLen 不得为负: " + srcLen);
        }
        if (dstCapacity < srcLen) {
            return -1;
        }
        for (int i = 0; i < srcLen; i++) {
            dst.put(dstOffset + i, rotate(src.get(srcOffset + i)));
        }
        return srcLen;
    }

    @Override
    public int decode(ByteBuffer src, int srcOffset, int storedLen, int logicalLen,
                      ByteBuffer dst, int dstOffset) {
        if (storedLen != logicalLen) {
            throw new IllegalArgumentException(
                    "ROT13 长度不变，storedLen(" + storedLen + ") 必须等于 logicalLen("
                            + logicalLen + ")");
        }
        for (int i = 0; i < storedLen; i++) {
            dst.put(dstOffset + i, rotate(src.get(srcOffset + i)));
        }
        return logicalLen;
    }

    @Override
    public int maxStoredSize(int logicalLen) {
        return logicalLen;
    }

    /**
     * 对 ASCII 字母做 +13（非字母原样返回）。
     *
     * @param b 输入字节
     * @return 变换后的字节
     */
    private static byte rotate(byte b) {
        if (b >= 'a' && b <= 'z') {
            return (byte) ('a' + (b - 'a' + 13) % 26);
        }
        if (b >= 'A' && b <= 'Z') {
            return (byte) ('A' + (b - 'A' + 13) % 26);
        }
        return b;
    }

    @Override
    public String toString() {
        return "Rot13Codec{" + ID + '}';
    }
}
