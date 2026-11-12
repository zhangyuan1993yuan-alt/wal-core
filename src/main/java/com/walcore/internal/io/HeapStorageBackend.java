package com.walcore.internal.io;

import com.walcore.api.WalVersion;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.PluginContext;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 纯内存后端（{@code builtin:storage-heap}）：<b>完全不碰文件系统</b>。
 *
 * <p>与另外两个后端的唯一语义差别：{@link #create} 不会真的建文件，
 * {@link #delete} 只是把内存块从注册表移除。除此之外 {@code append / read / truncate /
 * appendPosition} 的行为完全一致，因此能通过同一套契约测试。
 *
 * <p>{@link #open} 会从注册表里取回同一个 {@link HeapSegmentHandle.HeapStore}，
 * 所以"create → append → close → open → read"这条链路在内存后端上也能跑通。
 */
public final class HeapStorageBackend implements StorageBackend {

    /** 插件 id。 */
    public static final String ID = StorageBackendFactory.ID_HEAP;

    private final Map<Path, HeapSegmentHandle.HeapStore> stores = new ConcurrentHashMap<>();

    private volatile StorageConfig config;
    private volatile PluginContext pluginContext;
    private volatile boolean closed;

    /**
     * 无参构造（{@code ServiceLoader} 要求）。
     */
    public HeapStorageBackend() {
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
    public void init(StorageConfig storageConfig) {
        this.config = Preconditions.checkNotNull(storageConfig, "config");
    }

    @Override
    public boolean supportsZeroCopyAppend() {
        return false;
    }

    @Override
    public SegmentHandle create(int partitionId, long firstLsn, long segmentSizeBytes)
            throws IOException {
        requireInitialized();
        Preconditions.checkPositive(segmentSizeBytes, "segmentSizeBytes");
        Path target = SegmentNaming.walPath(config.baseDir(), partitionId, firstLsn);
        HeapSegmentHandle.HeapStore store = new HeapSegmentHandle.HeapStore();
        stores.put(target, store);
        return new HeapSegmentHandle(target, firstLsn, segmentSizeBytes, true, store);
    }

    @Override
    public SegmentHandle open(Path path, long firstLsn, boolean writable) throws IOException {
        requireInitialized();
        HeapSegmentHandle.HeapStore store = stores.computeIfAbsent(
                path, p -> new HeapSegmentHandle.HeapStore());
        long capacity = config.segmentSizeBytes();
        if (!writable) {
            capacity = Math.max(1L, store.length());
        }
        return new HeapSegmentHandle(path, firstLsn, capacity, writable, store);
    }

    @Override
    public void delete(Path path) {
        stores.remove(path);
        String name = path.getFileName().toString();
        if (name.endsWith(SegmentNaming.WAL_EXT)) {
            String idxName = name.substring(0,
                    name.length() - SegmentNaming.WAL_EXT.length()) + SegmentNaming.IDX_EXT;
            stores.remove(path.resolveSibling(idxName));
        }
    }

    @Override
    public boolean accepts(Path path) {
        return SegmentNaming.isWalFile(path);
    }

    @Override
    public void close() {
        closed = true;
        stores.clear();
    }

    /**
     * 当前保活的段存储数（测试断言用，可验证"无文件系统副作用 + 删除生效"）。
     *
     * @return 数量
     */
    public int liveSegmentCount() {
        return stores.size();
    }

    /**
     * 已注册的段路径集合（测试断言用）。
     *
     * @return 只读视图
     */
    public Set<Path> liveSegments() {
        return Collections.unmodifiableSet(stores.keySet());
    }

    private void requireInitialized() {
        if (closed) {
            throw new IllegalStateException("HeapStorageBackend 已关闭");
        }
        if (config == null) {
            throw new IllegalStateException(
                    "HeapStorageBackend 尚未 init(StorageConfig)，不能创建/打开段");
        }
    }

    @Override
    public String toString() {
        return "HeapStorageBackend{id=" + ID + ", segments=" + stores.size() + '}';
    }
}
