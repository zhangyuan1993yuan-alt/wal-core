package com.walcore.plugin.fixtures;

import com.walcore.spi.Priority;

/**
 * 测试插件：与 {@link PassThroughCodec} 行为完全相同，但优先级更高（900）。
 *
 * <p>它唯一的存在意义是验证"<b>优先级仲裁</b>与 ServiceLoader 的声明顺序无关"：
 * 服务文件里它排在后面，却必须被选中。
 */
@Priority(900)
public class HighPriorityCodec extends PassThroughCodec {

    /** 插件 id。 */
    public static final String ID = "test:codec-high";

    /** 编码器数值 id。 */
    public static final int CODEC_ID = 201;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int codecId() {
        return CODEC_ID;
    }
}
