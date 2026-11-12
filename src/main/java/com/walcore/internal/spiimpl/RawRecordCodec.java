package com.walcore.internal.spiimpl;

import com.walcore.api.WalVersion;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.RecordCodec;

import java.nio.ByteBuffer;

/**
 * 内置 RAW 编码器：恒等映射，{@code codecId = 0}。
 *
 * <p>它是默认编码器，也是"解码降级"的落点：当读到未知 {@code codecId}
 * 但 {@code storedLen == payloadLen} 且未置位 {@code flags.COMPRESSED} 时，
 * 解码器会按 RAW 读取（ARCH §4.2 最后一行），避免因插件缺失而整段日志不可读。
 *
 * <p>实现上只做 {@link MemoryUtils#copy}，无分支、无校验循环，因此对 256B payload
 * 的编码/解码各约 10~20ns。
 */
public final class RawRecordCodec implements RecordCodec {

    /** 插件 id（配置里写 {@code wal.spi.codec=builtin:raw} 即可强制选中）。 */
    public static final String ID = "builtin:raw";

    /** RAW 的数值 id，帧头 {@code codecId} 字段的保留值。 */
    public static final int CODEC_ID = 0;

    private volatile PluginContext context;

    /**
     * 无参构造（{@code ServiceLoader} 要求）。
     */
    public RawRecordCodec() {
        // 无状态
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext pluginContext) throws PluginException {
        this.context = pluginContext;
    }

    @Override
    public void close() {
        this.context = null;
    }

    /**
     * 插件上下文（仅用于自诊断，可能为 null）。
     *
     * @return 上下文
     */
    public PluginContext context() {
        return context;
    }

    @Override
    public int codecId() {
        return CODEC_ID;
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
        if (srcLen == 0) {
            return 0;
        }
        MemoryUtils.copy(dst, dstOffset, src, srcOffset, srcLen);
        return srcLen;
    }

    @Override
    public int decode(ByteBuffer src, int srcOffset, int storedLen, int logicalLen,
                      ByteBuffer dst, int dstOffset) {
        if (storedLen != logicalLen) {
            // RAW 是恒等映射，两者不等说明帧头被破坏或 codecId 标错了
            throw new IllegalArgumentException(
                    "RAW codec 要求 storedLen == logicalLen，实际 storedLen=" + storedLen
                            + ", logicalLen=" + logicalLen);
        }
        if (logicalLen == 0) {
            return 0;
        }
        MemoryUtils.copy(dst, dstOffset, src, srcOffset, logicalLen);
        return logicalLen;
    }

    @Override
    public int maxStoredSize(int logicalLen) {
        return logicalLen;
    }

    @Override
    public String toString() {
        return "RawRecordCodec{id=" + ID + ", codecId=" + CODEC_ID + '}';
    }
}
