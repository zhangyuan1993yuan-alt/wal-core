package com.walcore.plugin.fixtures;

import com.walcore.api.WalVersion;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.Priority;
import com.walcore.spi.RecordCodec;

import java.nio.ByteBuffer;

/**
 * 测试插件：<b>恒等映射</b>的编码器（优先级 10，最低）。
 *
 * <p>做成恒等映射是刻意的：它让"插件真的被采用了"这件事可以被端到端验证
 * （写入 → 读回 → 字节完全一致），而不会引入任何额外的编解码语义。
 * {@code codecId = 200} 落在"第三方 128~255"区间，不与内置 0 冲突。
 */
@Priority(10)
public class PassThroughCodec implements RecordCodec {

    /** 插件 id。 */
    public static final String ID = "test:codec-low";

    /** 编码器数值 id。 */
    public static final int CODEC_ID = 200;

    private boolean initialized;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        this.initialized = true;
    }

    /**
     * 是否已被框架初始化（测试断言用）。
     *
     * @return 已初始化返回 true
     */
    public boolean initialized() {
        return initialized;
    }

    @Override
    public int codecId() {
        return CODEC_ID;
    }

    @Override
    public int encode(ByteBuffer src, int srcOffset, int srcLen,
                      ByteBuffer dst, int dstOffset, int dstCapacity) {
        if (srcLen > dstCapacity) {
            return -1;
        }
        for (int i = 0; i < srcLen; i++) {
            dst.put(dstOffset + i, src.get(srcOffset + i));
        }
        return srcLen;
    }

    @Override
    public int decode(ByteBuffer src, int srcOffset, int storedLen, int logicalLen,
                      ByteBuffer dst, int dstOffset) {
        int n = Math.min(storedLen, logicalLen);
        for (int i = 0; i < n; i++) {
            dst.put(dstOffset + i, src.get(srcOffset + i));
        }
        return n;
    }

    @Override
    public int maxStoredSize(int logicalLen) {
        return logicalLen;
    }

    @Override
    public void close() {
        // 幂等：无资源
    }
}
