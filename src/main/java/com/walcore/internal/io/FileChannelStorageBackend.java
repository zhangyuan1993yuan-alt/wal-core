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

/**
 * FileChannel 后端（{@code builtin:storage-fc}，非 Linux x86_64 平台的默认兜底）。
 *
 * <p>行为与 {@link MappedStorageBackend} 逐项对齐（同样的 {@code .tmp → ATOMIC_MOVE}、
 * 同样的父目录 force、同样的 {@code delete} 语义），唯一差别是底层不用 mmap。
 * 两者必须通过<b>同一套</b> {@code SegmentHandleContractTest}。
 */
public final class FileChannelStorageBackend implements StorageBackend {

    /** 插件 id。 */
    public static final String ID = StorageBackendFactory.ID_FILE_CHANNEL;

    private volatile StorageConfig config;
    private volatile PluginContext pluginContext;
    private volatile boolean closed;

    /**
     * 无参构造（{@code ServiceLoader} 要求）。
     */
    public FileChannelStorageBackend() {
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
    }

    @Override
    public boolean supportsZeroCopyAppend() {
        return false;
    }

    @Override
    public SegmentHandle create(int partitionId, long firstLsn, long segmentSizeBytes)
            throws IOException {
        requireInitialized();
        Path target = SegmentNaming.walPath(config.baseDir(), partitionId, firstLsn);
        Path tmp = SegmentNaming.tmpPath(config.baseDir(), partitionId, firstLsn);
        Files.createDirectories(target.getParent());
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            MappedSegmentHandle.ensureFileLength(ch, segmentSizeBytes);
            if (config.preallocateFill()) {
                fillZeros(ch, segmentSizeBytes);
            }
        }
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        forceParentDir(target.getParent());
        return new FileChannelSegmentHandle(target, firstLsn, segmentSizeBytes, true);
    }

    @Override
    public SegmentHandle open(Path path, long firstLsn, boolean writable) throws IOException {
        requireInitialized();
        long capacity = writable ? config.segmentSizeBytes() : Math.max(1L, Files.size(path));
        return new FileChannelSegmentHandle(path, firstLsn, capacity, writable);
    }

    @Override
    public void delete(Path path) throws IOException {
        Files.deleteIfExists(path);
        Path idx = sibling(path, SegmentNaming.IDX_EXT);
        if (idx != null) {
            Files.deleteIfExists(idx);
        }
    }

    @Override
    public boolean accepts(Path path) {
        return SegmentNaming.isWalFile(path);
    }

    @Override
    public void close() {
        closed = true;
    }

    private void requireInitialized() {
        if (config == null) {
            throw new IllegalStateException(
                    "FileChannelStorageBackend 尚未 init(StorageConfig)，不能创建/打开段");
        }
        if (closed) {
            throw new IllegalStateException("FileChannelStorageBackend 已关闭");
        }
    }

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

    private void forceParentDir(Path dir) {
        if (!config.forceParentDirOnCreate()) {
            return;
        }
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            if (pluginContext != null) {
                pluginContext.warn("wal.storage.dirforce.skipped",
                        "父目录 force(true) 被跳过（平台不支持）: " + dir, null);
            }
        }
    }

    private static Path sibling(Path walPath, String extension) {
        String name = walPath.getFileName().toString();
        if (!name.endsWith(SegmentNaming.WAL_EXT)) {
            return null;
        }
        String idxName = name.substring(0, name.length() - SegmentNaming.WAL_EXT.length()) + extension;
        return walPath.resolveSibling(idxName);
    }

    @Override
    public String toString() {
        return "FileChannelStorageBackend{id=" + ID + '}';
    }
}
