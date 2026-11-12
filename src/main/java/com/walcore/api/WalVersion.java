package com.walcore.api;

/**
 * 格式与 API 版本常量。
 *
 * <p>两级版本各有含义，不要混用：
 * <ul>
 *   <li>{@link #API_VERSION} —— <b>插件契约版本</b>。插件通过 {@code WalPlugin.apiVersion()}
 *       声明，必须与之相等才能被加载（见 ARCH §10.4.3）。加字段/加默认方法可保持同版本，
 *       改方法签名必须递增。</li>
 *   <li>{@link #FORMAT_VERSION} —— <b>磁盘帧格式版本</b>。写入帧头的 {@code version} 字节。
 *       老读者读到更高版本时，若 {@code allowForwardCompatibleRead=true} 仍可只读。</li>
 * </ul>
 */
public final class WalVersion {

    /** 插件契约版本。任何 SPI 接口的破坏性变更都必须递增此值。 */
    public static final int API_VERSION = 1;

    /** 磁盘帧格式主版本。 */
    public static final int FORMAT_VERSION = 1;

    /** 库的语义版本（随构建更新，仅用于诊断输出）。 */
    public static final String LIBRARY_VERSION = "1.0.0";

    private WalVersion() {
    }

    /**
     * 语义版本编码：{@code major * 10000 + minor}，供未来"同 major 兼容"检查使用。
     *
     * @param major 主版本
     * @param minor 次版本
     * @return 编码值
     */
    public static int encode(int major, int minor) {
        return major * 10_000 + minor;
    }

    /**
     * 从编码值取出主版本。
     *
     * @param encoded 编码值
     * @return 主版本
     */
    public static int majorOf(int encoded) {
        return encoded / 10_000;
    }

    /**
     * 从编码值取出次版本。
     *
     * @param encoded 编码值
     * @return 次版本
     */
    public static int minorOf(int encoded) {
        return encoded % 10_000;
    }

    /**
     * 一行式版本摘要。
     *
     * @return 例如 {@code "wal-core 1.0.0 (api=1, format=1)"}
     */
    public static String describe() {
        return "wal-core " + LIBRARY_VERSION + " (api=" + API_VERSION + ", format=" + FORMAT_VERSION + ")";
    }
}
