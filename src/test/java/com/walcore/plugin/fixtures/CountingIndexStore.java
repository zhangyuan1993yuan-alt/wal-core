package com.walcore.plugin.fixtures;

import com.walcore.api.WalVersion;
import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexHit;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.Priority;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试插件：<b>只计数</b>的索引实现，用来证明 T11 引入的 SPI 契约修订
 * （{@code IndexStore.onAppended(...)}）真的被写路径调用。
 *
 * <h2>为什么必须专门测这一条</h2>
 * 在加这个回调之前，第三方 {@code IndexStore} 只会收到 {@code openSegment}，
 * 永远收不到逐条追加事件，于是它的 {@code lookup()} 恒返回 {@link IndexHit#NONE}，
 * 读取静默退化为"从段首顺序扫"——<b>正确但慢</b>，而且没有任何报错。
 * 这类"性能悄悄消失"的缺陷只能靠"数调用次数"来钉住。
 */
@Priority(500)
public class CountingIndexStore implements IndexStore {

    /** 插件 id。 */
    public static final String ID = "test:index-counting";

    private final AtomicLong appendedCount = new AtomicLong();
    private final AtomicLong openedSegments = new AtomicLong();
    private final List<IndexEntry> entries = new ArrayList<>();

    private volatile boolean initialized;

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        this.initialized = true;
    }

    /**
     * 是否已初始化。
     *
     * @return 已初始化返回 true
     */
    public boolean initialized() {
        return initialized;
    }

    /**
     * {@code onAppended} 被调用次数。
     *
     * @return 次数
     */
    public long appendedCount() {
        return appendedCount.get();
    }

    /**
     * {@code openSegment} 被调用次数。
     *
     * @return 次数
     */
    public long openedSegments() {
        return openedSegments.get();
    }

    @Override
    public void onAppended(int partitionId, long segmentFirstLsn, long lsn, long fileOffset) {
        appendedCount.incrementAndGet();
        // 这个回调就是"逐条追加事件"的唯一来源：真正的插件会在这里累积锚点，
        // 从而让 lookup() 变得可用。这里也照样记录，好让 lastIndexedLsn() 有意义。
        synchronized (entries) {
            entries.add(new IndexEntry(lsn, fileOffset));
        }
    }

    @Override
    public void openSegment(int partitionId, long segmentFirstLsn, long segmentCapacityBytes) {
        openedSegments.incrementAndGet();
    }

    @Override
    public void append(long lsn, long fileOffset) {
        synchronized (entries) {
            entries.add(new IndexEntry(lsn, fileOffset));
        }
    }

    @Override
    public IndexHit lookup(long lsn) {
        long bestLsn = Long.MIN_VALUE;
        long bestOffset = -1L;
        synchronized (entries) {
            for (IndexEntry e : entries) {
                if (e.lsn() <= lsn && e.lsn() > bestLsn) {
                    bestLsn = e.lsn();
                    bestOffset = e.fileOffset();
                }
            }
        }
        return bestOffset < 0L ? IndexHit.NONE : IndexHit.of(bestLsn, bestOffset);
    }

    @Override
    public long lastIndexedLsn() {
        synchronized (entries) {
            long max = 0L;
            for (IndexEntry e : entries) {
                max = Math.max(max, e.lsn());
            }
            return max;
        }
    }

    @Override
    public void truncateAfter(long watermarkLsn) {
        synchronized (entries) {
            entries.removeIf(e -> e.lsn() > watermarkLsn);
        }
    }

    @Override
    public void rebuild(Iterator<IndexEntry> source) {
        synchronized (entries) {
            entries.clear();
            while (source.hasNext()) {
                entries.add(source.next());
            }
        }
    }

    @Override
    public long memoryBytes() {
        synchronized (entries) {
            return entries.size() * 16L;
        }
    }

    @Override
    public void closeSegment(long segmentFirstLsn) {
        // 本实现不按段持有资源
    }

    @Override
    public void close() {
        synchronized (entries) {
            entries.clear();
        }
    }
}
