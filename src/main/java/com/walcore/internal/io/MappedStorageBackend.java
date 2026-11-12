package com.walcore.internal.io;

import com.walcore.api.WalVersion;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.PluginContext;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * mmap 后端（{@code builtin:storage-mapped}，Linux x86_64 默认）。
 *
 * <p>创建流程对齐 ARCH §6.2 的"两阶段零阻塞轮转"：
 * <ol>
 *   <li>写 {@code wal-pXXXX-LSN.wal.tmp}（SPARSE 预分配到 {@code segmentSizeBytes}）；</li>
 *   <li>{@code ATOMIC_MOVE} 重命名为 {@code .wal}（同一文件系统内是原子的）；</li>
 *   <li>对父目录 {@code force(true)}（Linux 需要它才能保证"重命名后崩溃仍可见"；
 *       Windows 上自动降级为 no-op）。</li>
 * </ol>
 *
 * <p>{@code close()} 会把"unmap 失败导致无法删除"的路径记进 {@link #pendingDelete()}，
 * 下次 {@link #create} / {@link #open} 时自动清理（ARCH §16.4 的兜底）。
 */
public final class MappedStorageBackend implements StorageBackend {

    /** 插件 id。 */
    public static final String ID = StorageBackendFactory.ID_MAPPED;

    private final AtomicLong createdHandles = new AtomicLong();
    private final Set<Path> pendingDelete = ConcurrentHashMap.newKeySet();

    private volatile StorageConfig config;
    private volatile PluginContext pluginContext;
    private volatile boolean closed;

    /**
     * 无参构造（{@code ServiceLoader} 要求）。
     */
    public MappedStorageBackend() {
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
    public void init(PluginContext context) {
        this.pluginContext = context;
    }

    @Override
    public void init(StorageConfig storageConfig) throws IOException {
        Preconditions.checkNotNull(storageConfig, "config");
        this.config = storageConfig;
        Files.createDirectories(storageConfig.baseDir());
        Files.createDirectories(storageConfig.baseDir().resolve(SegmentNaming.PARTITIONS_DIR));
        flushPendingDeletes();
    }

    @Override
    public boolean supportsZeroCopyAppend() {
        return true;
    }

    @Override
    public SegmentHandle create(int partitionId, long firstLsn, long segmentSizeBytes)
            throws IOException {
        requireInitialized();
        Preconditions.checkArgument(segmentSizeBytes <= Integer.MAX_VALUE,
                "mmap 后端要求 segmentSizeBytes <= 2GB，实际 = %s", segmentSizeBytes);
        flushPendingDeletes();
        Path target = SegmentNaming.walPath(config.baseDir(), partitionId, firstLsn);
        Path tmp = SegmentNaming.tmpPath(config.baseDir(), partitionId, firstLsn);
        Files.createDirectories(target.getParent());

        // 1) 先在 .tmp 上建好并预分配到满容量
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            MappedSegmentHandle.ensureFileLength(ch, segmentSizeBytes);
            if (config.preallocateFill()) {
                fillZeros(ch, segmentSizeBytes);
            }
        }
        // 2) 原子重命名
        atomicMove(tmp, target);
        // 3) 父目录 force（Linux 需要；Windows 上直接返回）
        forceParentDir(target.getParent());

        createdHandles.incrementAndGet();
        return new MappedSegmentHandle(target, firstLsn, segmentSizeBytes, true);
    }

    @Override
    public SegmentHandle open(Path path, long firstLsn, boolean writable) throws IOException {
        requireInitialized();
        return new MappedSegmentHandle(path, firstLsn,
                writable ? config.segmentSizeBytes() : Math.max(1L, Files.size(path)), writable);
    }

    @Override
    public void delete(Path path) throws IOException {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // Windows 上映射未完全释放时会命中这里：登记延后删除，不阻断调用方
            pendingDelete.add(path);
            throw e;
        }
        Path idx = sibling(path, SegmentNaming.IDX_EXT);
        if (idx != null) {
            try {
                Files.deleteIfExists(idx);
            } catch (IOException e) {
                pendingDelete.add(idx);
            }
        }
    }

    @Override
    public boolean accepts(Path path) {
        return SegmentNaming.isWalFile(path);
    }

    @Override
    public void close() {
        closed = true;
        flushPendingDeletes();
    }

    /**
     * 已创建句柄数（测试断言用）。
     *
     * @return 计数
     */
    public long createdHandleCount() {
        return createdHandles.get();
    }

    /**
     * 因 unmap 未及时完成而延后删除的路径集合（测试断言用）。
     *
     * @return 只读视图
     */
    public Set<Path> pendingDelete() {
        return java.util.Collections.unmodifiableSet(pendingDelete);
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 校验已初始化。
     */
    private void requireInitialized() {
        if (config == null) {
            throw new IllegalStateException(
                    "MappedStorageBackend 尚未 init(StorageConfig)，不能创建/打开段");
        }
        if (closed) {
            throw new IllegalStateException("MappedStorageBackend 已关闭");
        }
    }

    /**
     * 用 8MB 为单位把文件填零（{@code preallocateFill=true} 时才做）。
     *
     * @param ch 通道
     * @param total 总字节数
     * @throws IOException IO 失败
     */
    private static void fillZeros(FileChannel ch, long total) throws IOException {
        int chunk = 8 * 1024 * 1024;
        java.nio.ByteBuffer zeros = java.nio.ByteBuffer.allocate(chunk);
        long pos = 0L;
        while (pos < total) {
            int len = (int) Math.min((long) chunk, total - pos);
            zeros.clear();
            zeros.limit(len);
            int written = 0;
            while (written < len) {
                written += ch.write(zeros, pos + written);
            }
            pos += len;
        }
    }

    /**
     * 原子重命名。
     *
     * @param from 源
     * @param to 目标
     * @throws IOException IO 失败
     */
    private static void atomicMove(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * 对父目录 {@code force(true)}。
     *
     * <p>Linux 上需要 {@code FileChannel.open(dir)} + {@code force(true)} 技巧才能保证
     * "新建/重命名 + 崩溃后仍可见"；Windows 与部分文件系统不支持打开目录通道，
     * 直接降级为 no-op（ARCH §16.4）。
     *
     * @param dir 目录
     */
    private void forceParentDir(Path dir) {
        if (!config.forceParentDirOnCreate()) {
            return;
        }
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Windows / 不支持目录 fsync 的文件系统：明确降级，不做告警噪声
            if (pluginContext != null) {
                pluginContext.warn("wal.storage.dirforce.skipped",
                        "父目录 force(true) 被跳过（平台不支持）: " + dir, null);
            }
        }
    }

    /**
     * 计算同名的 {@code .idx} 路径。
     *
     * @param walPath 段路径
     * @param extension 目标扩展名
     * @return 路径，无法推导时返回 null
     */
    private static Path sibling(Path walPath, String extension) {
        String name = walPath.getFileName().toString();
        if (!name.endsWith(SegmentNaming.WAL_EXT)) {
            return null;
        }
        String idxName = name.substring(0, name.length() - SegmentNaming.WAL_EXT.length()) + extension;
        return walPath.resolveSibling(idxName);
    }

    /**
     * 尝试清理之前延后删除的文件。
     */
    private void flushPendingDeletes() {
        if (pendingDelete.isEmpty()) {
            return;
        }
        for (Path p : java.util.List.copyOf(pendingDelete)) {
            try {
                Files.deleteIfExists(p);
                pendingDelete.remove(p);
            } catch (IOException ignored) {
                // 仍删不掉，留到下次
            }
        }
    }

    @Override
    public String toString() {
        return "MappedStorageBackend{id=" + ID + ", created=" + createdHandles.get() + '}';
    }
}
