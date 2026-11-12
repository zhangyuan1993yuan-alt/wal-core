package com.walcore.plugin.fixtures;

import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;

/**
 * 测试插件：{@code init()} 必定抛异常，用来验证"坏插件只淘汰自己、绝不拖垮整体"。
 */
public class BrokenInitCodec extends PassThroughCodec {

    /** 插件 id。 */
    public static final String ID = "test:codec-broken";

    /** 编码器数值 id。 */
    public static final int CODEC_ID = 203;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int codecId() {
        return CODEC_ID;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        throw new PluginException("故意失败：测试用坏插件", ID, null);
    }
}
