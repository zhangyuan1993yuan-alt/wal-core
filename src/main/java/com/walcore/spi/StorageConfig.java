package com.walcore.spi;

import com.walcore.internal.util.Preconditions;

import java.nio.file.Path;

/**
 * 存储后端初始化配置（不可变）。
 *
 * <p>与 {@code WalConfig} 的关系：{@code WalConfig} 是"用户视角的全局配置"，
 * 本类是"存储层的参数投影"，只保留存储后端真正需要的那几个值。这样插件作者不必
 * 依赖整个 {@code api} 包就能写出后端。
 */
public final class StorageConfig {

    private final Path baseDir;
    private final long segmentSizeBytes;
    private final boolean preallocateFill;
    private final long mappedWindowBytes;
    private final int mappedWindowCount;
    private final boolean forceParentDirOnCreate;

    /**
     * 构造存储配置。
     *
     * @param baseDir 日志根目录
     * @param segmentSizeBytes 段容量
     * @param preallocateFill 是否用后台填零做 FILL 预分配
     * @param mappedWindowBytes 只读段窗口大小
     * @param mappedWindowCount 只读段窗口数量（同时打开的句柄上界）
     * @param forceParentDirOnCreate 创建/重命名段后是否对父目录 force(true)
     */
    public StorageConfig(Path baseDir, long segmentSizeBytes, boolean preallocateFill,
                         long mappedWindowBytes, int mappedWindowCount,
                         boolean forceParentDirOnCreate) {
        this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
        this.segmentSizeBytes = Preconditions.checkPositive(segmentSizeBytes, "segmentSizeBytes");
        this.preallocateFill = preallocateFill;
        this.mappedWindowBytes = Preconditions.checkPositive(mappedWindowBytes, "mappedWindowBytes");
        this.mappedWindowCount = Preconditions.checkInRange(mappedWindowCount, 1, 1024, "mappedWindowCount");
        this.forceParentDirOnCreate = forceParentDirOnCreate;
    }

    /**
     * 用默认值构造（段 256MB、窗口 32MB × 8、开启父目录 force）。
     *
     * @param baseDir 日志根目录
     * @return 配置
     */
    public static StorageConfig defaults(Path baseDir) {
        return new StorageConfig(baseDir, 268_435_456L, false,
                32L * 1024 * 1024, 8, true);
    }

    /**
     * 日志根目录。
     *
     * @return 根目录
     */
    public Path baseDir() {
        return baseDir;
    }

    /**
     * 段容量（字节）。
     *
     * @return 字节数
     */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    /**
     * 是否用填零方式预分配。
     *
     * @return FILL 模式返回 true
     */
    public boolean preallocateFill() {
        return preallocateFill;
    }

    /**
     * 只读段映射窗口大小（字节）。
     *
     * @return 字节数
     */
    public long mappedWindowBytes() {
        return mappedWindowBytes;
    }

    /**
     * 只读段映射窗口数量。
     *
     * @return 窗口数
     */
    public int mappedWindowCount() {
        return mappedWindowCount;
    }

    /**
     * 是否在创建/重命名段后对父目录 {@code force(true)}。
     *
     * @return 需要返回 true
     */
    public boolean forceParentDirOnCreate() {
        return forceParentDirOnCreate;
    }

    @Override
    public String toString() {
        return "StorageConfig{baseDir=" + baseDir
                + ", segmentSizeBytes=" + segmentSizeBytes
                + ", preallocateFill=" + preallocateFill
                + ", mappedWindowBytes=" + mappedWindowBytes
                + ", mappedWindowCount=" + mappedWindowCount
                + ", forceParentDirOnCreate=" + forceParentDirOnCreate
                + '}';
    }
}
