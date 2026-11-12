package com.walcore.internal.spiimpl;

import com.walcore.api.WalVersion;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.segment.SparseIndex;
import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexHit;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内置稀疏索引插件（{@code builtin:index-sparse}）：<b>多段索引的管理壳</b>。
 *
 * <p>{@link IndexStore} 的契约是"一个实例服务多个段，用 {@code segmentFirstLsn} 区分"，
 * 而真正的数据结构在 {@link SparseIndex}（每段一份 {@code long[]} 二分数组 + 一个 {@code .idx} 文件）。
 * 本类负责：
 * <ol>
 *   <li>按 {@code segmentFirstLsn} 维护 {@code Map<Long, SparseIndex>}；</li>
 *   <li>把 {@code append / lookup / truncateAfter / rebuild} 路由到"当前段"的实例；</li>
 *   <li>把 {@code memoryBytes()} 汇总成整个索引层的堆内占用（供 512MB 预算校验）。</li>
 * </ol>
 *
 * <p>线程安全：{@code current} 段引用是 volatile；{@code Map} 是 {@link ConcurrentHashMap}；
 * 每个 {@link SparseIndex} 的方法自带 {@code synchronized}（频率只有 0.1%，值得）。
 */
public final class DefaultIndexStore implements IndexStore {

    /** 插件 id。 */
    public static final String ID = SparseIndex.ID;

    private final Map<Long, SparseIndex> segments = new ConcurrentHashMap<>();

    private Path baseDir;
    private long maxHeapBytes;
    private int indexIntervalRecords = 1000;
    private volatile PluginContext context;
    private volatile long currentSegmentFirstLsn;
    private volatile int partitionId;

    /**
     * 无参构造（{@code ServiceLoader} 要求）：纯内存、不落 {@code .idx}。
     */
    public DefaultIndexStore() {
        this(null, 0L);
    }

    /**
     * 构造带 {@code .idx} 落盘能力的索引管理器。
     *
     * @param baseDir 日志根目录，null 表示纯内存
     * @param maxHeapBytes 全索引层堆内预算（字节），0 表示不限
     */
    public DefaultIndexStore(Path baseDir, long maxHeapBytes) {
        this.baseDir = baseDir;
        this.maxHeapBytes = Math.max(0L, maxHeapBytes);
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
    public void init(PluginContext pluginContext) {
        this.context = pluginContext;
        if (pluginContext != null) {
            this.baseDir = pluginContext.baseDir();
            this.maxHeapBytes = pluginContext.config().maxIndexHeapBytes();
            this.indexIntervalRecords = pluginContext.config().indexIntervalRecords();
        }
    }

    @Override
    public void close() {
        for (SparseIndex idx : segments.values()) {
            idx.close();
        }
        segments.clear();
    }

    @Override
    public void openSegment(int partitionId, long segmentFirstLsn, long segmentCapacityBytes) {
        this.partitionId = partitionId;
        this.currentSegmentFirstLsn = segmentFirstLsn;
        SparseIndex idx = segments.computeIfAbsent(segmentFirstLsn, k -> {
            Path idxPath = baseDir == null
                    ? null
                    : SegmentNaming.idxPath(baseDir, partitionId, segmentFirstLsn);
            // 预算按段均摊：给每段一份预算，超限时该段降级为纯文件二分
            SparseIndex created = new SparseIndex(idxPath, maxHeapBytes);
            created.indexIntervalRecords(indexIntervalRecords);
            if (context != null) {
                created.init(context);
            }
            return created;
        });
        idx.indexIntervalRecords(indexIntervalRecords);
        idx.openSegment(partitionId, segmentFirstLsn, segmentCapacityBytes);
    }

    @Override
    public void append(long lsn, long fileOffset) {
        active().append(lsn, fileOffset);
    }

    @Override
    public IndexHit lookup(long lsn) {
        return active().lookup(lsn);
    }

    @Override
    public long lastIndexedLsn() {
        // 尚未 openSegment 时（例如恢复流程在扫描前做一致性探测）不能惰性创建索引：
        // 那时我们还不知道段名，用 0 当 key 会生成非法的 .idx 文件名。
        // 语义上"没有任何段被打开 ⇒ 还没有索引" ⇒ 返回 0 才是正确答复。
        if (currentSegmentFirstLsn <= 0L) {
            return 0L;
        }
        return active().lastIndexedLsn();
    }

    @Override
    public void truncateAfter(long watermarkLsn) {
        active().truncateAfter(watermarkLsn);
    }

    @Override
    public void rebuild(Iterator<IndexEntry> source) {
        active().rebuild(source);
    }

    @Override
    public long memoryBytes() {
        long total = 0L;
        for (SparseIndex idx : segments.values()) {
            total += idx.memoryBytes();
        }
        return total;
    }

    @Override
    public void closeSegment(long segmentFirstLsn) {
        SparseIndex idx = segments.remove(segmentFirstLsn);
        if (idx != null) {
            idx.closeSegment(segmentFirstLsn);
        }
    }

    // ------------------------------------------------------------------
    // 附加能力
    // ------------------------------------------------------------------

    /**
     * 取某个段的索引实例。
     *
     * @param segmentFirstLsn 段标识
     * @return 索引实例；不存在返回 null
     */
    public SparseIndex indexFor(long segmentFirstLsn) {
        return segments.get(segmentFirstLsn);
    }

    /**
     * 某个段是否需要重建索引。
     *
     * @param segmentFirstLsn 段标识
     * @return 需要重建返回 true
     */
    public boolean needsRebuild(long segmentFirstLsn) {
        SparseIndex idx = segments.get(segmentFirstLsn);
        return idx != null && idx.needsRebuild();
    }

    /**
     * 当前段标识。
     *
     * @return 段首 LSN
     */
    public long currentSegmentFirstLsn() {
        return currentSegmentFirstLsn;
    }

    /**
     * 已管理的段索引数。
     *
     * @return 数量
     */
    public int segmentCount() {
        return segments.size();
    }

    /**
     * 取当前段的索引实例（如尚未 {@code openSegment} 则惰性创建一个内存实例）。
     *
     * @return 索引实例
     */
    private SparseIndex active() {
        SparseIndex idx = segments.get(currentSegmentFirstLsn);
        if (idx != null) {
            return idx;
        }
        return segments.computeIfAbsent(currentSegmentFirstLsn, k -> {
            Path idxPath = baseDir == null
                    ? null
                    : SegmentNaming.idxPath(baseDir, partitionId, k);
            SparseIndex created = new SparseIndex(idxPath, maxHeapBytes);
            created.indexIntervalRecords(indexIntervalRecords);
            created.openSegment(partitionId, k, 0L);
            return created;
        });
    }

    @Override
    public String toString() {
        return "DefaultIndexStore{segments=" + segments.size()
                + ", current=" + currentSegmentFirstLsn
                + ", memoryBytes=" + memoryBytes() + '}';
    }
}
