package com.walcore.plugin.fixtures;

import com.walcore.spi.PluginException;
import com.walcore.spi.PluginContext;

/**
 * 测试插件：声明了一个<b>未来版本</b>的 {@code apiVersion}，必须被框架剔除。
 *
 * <p>这条保护对应"插件比核心新"的现实场景：老核心读到新插件的服务文件时，
 * 必须<b>拒绝</b>而不是"尽力调用"——后者会以难以定位的 {@code NoSuchMethodError}
 * 形式在运行时炸开。
 */
public class IncompatibleCodec extends PassThroughCodec {

    /** 插件 id。 */
    public static final String ID = "test:codec-future";

    /** 编码器数值 id。 */
    public static final int CODEC_ID = 202;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return 9999;
    }

    @Override
    public int codecId() {
        return CODEC_ID;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        throw new PluginException("版本不兼容的插件不该被 init",
                "test:codec-future", null);
    }
}
