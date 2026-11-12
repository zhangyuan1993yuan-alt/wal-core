package com.walcore.spi;

/**
 * 插件相关的运行时异常：实例化失败、{@code init()} 失败、版本不兼容、热切换失败等。
 *
 * <p>约定：本异常<b>不用于</b>表达"业务调用失败"。例如 {@code RecordCodec.encode} 内部
 * 抛出的任何异常都会被框架包装成 {@code WalException.CodecException}（错误码 {@code WAL_CODEC}），
 * 因为那是"某一次 append 失败"，不是"插件体系失败"。
 */
public class PluginException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** 出问题的插件 id，可为 null（发现阶段就失败时还拿不到 id）。 */
    private final String pluginId;

    /**
     * 构造异常。
     *
     * @param message 描述信息
     */
    public PluginException(String message) {
        this(message, null, null);
    }

    /**
     * 构造异常。
     *
     * @param message 描述信息
     * @param cause 根因
     */
    public PluginException(String message, Throwable cause) {
        this(message, null, cause);
    }

    /**
     * 构造带插件 id 的异常。
     *
     * @param message 描述信息
     * @param pluginId 插件 id，可为 null
     * @param cause 根因，可为 null
     */
    public PluginException(String message, String pluginId, Throwable cause) {
        super(pluginId == null ? message : message + " [plugin=" + pluginId + ']', cause);
        this.pluginId = pluginId;
    }

    /**
     * 出问题的插件 id。
     *
     * @return 插件 id，可能为 null
     */
    public String pluginId() {
        return pluginId;
    }
}
