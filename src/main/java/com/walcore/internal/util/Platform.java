package com.walcore.internal.util;

import java.util.Locale;

/**
 * 运行平台探测（OS / CPU 架构 / 页面大小 / mmap 能力）。
 *
 * <p>本类只做<b>只读探测</b>，不加载任何本地库、不做 JNI 调用，因此在任何平台都能安全加载。
 *
 * <p><b>为什么要探测 x86_64 + Linux</b>：mmap 后端的 {@code close()} 依赖
 * {@code sun.misc.Unsafe.invokeCleaner} 才能及时 unmap（Windows 上未 unmap 的文件无法删除）。
 * Linux x86_64 是承诺性能的主平台，默认走 mmap；其余平台默认走 FileChannel 兜底，
 * 只承诺正确性不承诺性能（见 ARCH §16.4）。
 */
public final class Platform {

    /** 默认页大小（字节）。{@code mmap} 窗口只需按页对齐，这里作为信息性常量暴露。 */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static final String OS_NAME =
            System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT);
    private static final String OS_ARCH =
            System.getProperty("os.arch", "unknown").toLowerCase(Locale.ROOT);

    private Platform() {
    }

    /**
     * 操作系统名称（小写），用于诊断输出。
     *
     * @return 例如 {@code "linux"} / {@code "windows 11"}
     */
    public static String osName() {
        return OS_NAME;
    }

    /**
     * CPU 架构名称（小写）。
     *
     * @return 例如 {@code "amd64"} / {@code "aarch64"}
     */
    public static String osArch() {
        return OS_ARCH;
    }

    /**
     * 是否为 Windows。
     *
     * @return Windows 返回 true
     */
    public static boolean isWindows() {
        return OS_NAME.startsWith("windows");
    }

    /**
     * 是否为 Linux。
     *
     * @return Linux 返回 true
     */
    public static boolean isLinux() {
        return OS_NAME.startsWith("linux");
    }

    /**
     * 是否为 macOS。
     *
     * @return macOS 返回 true
     */
    public static boolean isMac() {
        return OS_NAME.startsWith("mac") || OS_NAME.startsWith("darwin");
    }

    /**
     * 是否为 x86_64（含 {@code amd64} / {@code x86_64} 两种写法）。
     *
     * @return x86_64 返回 true
     */
    public static boolean isX86_64() {
        return "amd64".equals(OS_ARCH) || "x86_64".equals(OS_ARCH);
    }

    /**
     * 是否为 Linux x86_64 —— mmap 后端的默认启用条件。
     *
     * <p>在 Windows 开发机上必须返回 {@code false}（TASKS-wal.md T01 ⑥ 的验收点）。
     *
     * @return Linux x86_64 返回 true
     */
    public static boolean isLinuxX86_64() {
        return isLinux() && isX86_64();
    }

    /**
     * 页面大小（字节）。仅作信息性与对齐提示，不参与正确性判断。
     *
     * @return 页大小，默认 4096
     */
    public static int pageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    /**
     * JVM 是否支持 {@link java.nio.MappedByteBuffer} 的文件映射。
     *
     * <p>所有受支持的 JVM 实现都支持；保留此方法是为了让上层有统一的探测入口，
     * 避免各处直接假设。
     *
     * @return 支持返回 true
     */
    public static boolean supportsMappedByteBuffer() {
        return true;
    }

    /**
     * 默认存储后端插件 ID：Linux x86_64 用 mmap，其余平台用 FileChannel。
     *
     * @return 插件 ID
     */
    public static String defaultStorageBackendId() {
        return isLinuxX86_64()
                ? com.walcore.internal.io.StorageBackendFactory.ID_MAPPED
                : com.walcore.internal.io.StorageBackendFactory.ID_FILE_CHANNEL;
    }

    /**
     * 可用处理器数，供组提交线程数默认值校验使用。
     *
     * @return 处理器数，至少 1
     */
    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    /**
     * 一行式平台摘要，用于启动日志与故障排查。
     *
     * @return 例如 {@code "linux/amd64 (x86_64=true, pageSize=4096)"}
     */
    public static String describe() {
        return OS_NAME + "/" + OS_ARCH
                + " (isLinuxX86_64=" + isLinuxX86_64()
                + ", pageSize=" + pageSize() + ")";
    }
}
