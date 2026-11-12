package com.walcore.internal.io;

import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.Platform;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.PluginContext;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;

import java.io.IOException;
import java.util.Locale;
import java.util.function.BooleanSupplier;

/**
 * 存储后端选择器：按平台探测或插件 id 创建具体后端。
 *
 * <h2>默认选择规则（ARCH §16.1 的"双后端策略"）</h2>
 * <ul>
 *   <li>{@code Platform.isLinuxX86_64()} → {@link #ID_MAPPED}（mmap，零系统调用追加）；</li>
 *   <li>其余平台（Windows / macOS / ARM）→ {@link #ID_FILE_CHANNEL}（可移植兜底）。</li>
 * </ul>
 * 用户可用 {@code WalBuilder.storageBackend("builtin:storage-fc")} 强制切换。
 * 三者共享同一套契约测试，因此"Windows 上默认用 FC"不是降级正确性，只是不承诺性能。
 *
 * <h2>可测性设计</h2>
 * 默认构造器用 {@link Platform#isLinuxX86_64()} 作为探测函数；测试可注入
 * {@code BooleanSupplier} 覆盖"Linux / 非 Linux"两条分支，无需 mock 静态方法。
 */
public final class StorageBackendFactory {

    /** mmap 后端插件 id。 */
    public static final String ID_MAPPED = "builtin:storage-mapped";

    /** FileChannel 后端插件 id。 */
    public static final String ID_FILE_CHANNEL = "builtin:storage-fc";

    /** 纯堆内后端插件 id（测试用）。 */
    public static final String ID_HEAP = "builtin:storage-heap";

    private final BooleanSupplier linuxX64Probe;

    /**
     * 使用真实平台探测构造工厂。
     */
    public StorageBackendFactory() {
        this(Platform::isLinuxX86_64);
    }

    /**
     * 注入探测函数构造工厂（测试用）。
     *
     * @param linuxX64Probe 返回"是否 Linux x86_64"的探测函数，非 null
     */
    public StorageBackendFactory(BooleanSupplier linuxX64Probe) {
        this.linuxX64Probe = Preconditions.checkNotNull(linuxX64Probe, "linuxX64Probe");
    }

    /**
     * 当前平台下应使用的默认后端 id。
     *
     * @return 插件 id
     */
    public String defaultBackendId() {
        return linuxX64Probe.getAsBoolean() ? ID_MAPPED : ID_FILE_CHANNEL;
    }

    /**
     * 平台默认后端 id 的<b>静态</b>便捷入口。
     *
     * <p>供 {@code PluginManager} 在"某类 SPI 无可用实现、需要回退内置"时使用——
     * 那里没有工厂实例，也不应该为了拿一个 id 就 new 一个工厂。
     *
     * @return 插件 id
     */
    public static String defaultBackendIdStatic() {
        return Platform.isLinuxX86_64() ? ID_MAPPED : ID_FILE_CHANNEL;
    }

    /**
     * 创建并初始化一个存储后端（不注入 {@link PluginContext}）。
     *
     * @param pluginId 后端插件 id；null 或空表示按平台默认选择
     * @param config 存储配置
     * @return 已初始化的后端
     * @throws IOException 初始化失败
     */
    public StorageBackend create(String pluginId, StorageConfig config) throws IOException {
        return create(pluginId, config, null);
    }

    /**
     * 创建并初始化一个存储后端。
     *
     * @param pluginId 后端插件 id；null 或空表示按平台默认选择
     * @param config 存储配置
     * @param context 插件上下文，可为 null（T11 的 PluginManager 就位后由它注入）
     * @return 已初始化的后端
     * @throws IOException 初始化失败
     */
    public StorageBackend create(String pluginId, StorageConfig config, PluginContext context)
            throws IOException {
        Preconditions.checkNotNull(config, "config");
        String id = (pluginId == null || pluginId.isEmpty()) ? defaultBackendId() : pluginId;
        StorageBackend backend = instantiate(id);
        if (context != null) {
            backend.init(context);
        }
        backend.init(config);
        return backend;
    }

    /**
     * 按 id 实例化一个后端（未初始化）。
     *
     * @param pluginId 插件 id
     * @return 后端实例
     */
    public StorageBackend instantiate(String pluginId) {
        Preconditions.checkNotNull(pluginId, "pluginId");
        switch (pluginId.toLowerCase(Locale.ROOT)) {
            case ID_MAPPED:
                requireMappable();
                return new MappedStorageBackend();
            case ID_FILE_CHANNEL:
                return new FileChannelStorageBackend();
            case ID_HEAP:
                return new HeapStorageBackend();
            default:
                throw new IllegalArgumentException(
                        "未知的存储后端插件 id: " + pluginId
                                + "（内置可选：" + ID_MAPPED + " / " + ID_FILE_CHANNEL
                                + " / " + ID_HEAP + "）");
        }
    }

    /**
     * 用平台默认后端创建并初始化（静态便捷入口）。
     *
     * @param config 存储配置
     * @return 已初始化的后端
     * @throws IOException 初始化失败
     */
    public static StorageBackend createDefault(StorageConfig config) throws IOException {
        return new StorageBackendFactory().create(null, config);
    }

    /**
     * mmap 后端在当前 JVM 上是否可用。
     *
     * @return 可用返回 true
     */
    private static void requireMappable() {
        if (!Platform.supportsMappedByteBuffer()) {
            throw new IllegalStateException(
                    "当前 JVM 不支持 MappedByteBuffer，无法使用 " + ID_MAPPED
                            + "，请改用 " + ID_FILE_CHANNEL);
        }
    }

    /**
     * 判断一个路径是否被任一内置后端识别为段文件。
     *
     * @param path 路径
     * @return 可识别返回 true
     */
    public static boolean isSegmentFile(java.nio.file.Path path) {
        return SegmentNaming.isWalFile(path);
    }
}
