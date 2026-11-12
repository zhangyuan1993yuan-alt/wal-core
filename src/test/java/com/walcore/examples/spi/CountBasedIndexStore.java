package com.walcore.examples.spi;

import com.walcore.api.WalVersion;
import com.walcore.internal.spiimpl.DefaultIndexStore;
import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexHit;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 示例插件 ④：<b>按条数采样的索引</b>——每 N 条记录才写一个索引项，用更少的堆内存换稍慢的定位。
 *
 * <h2>这个例子要教会什么</h2>
 * <ol>
 *   <li><b>组合优于重写</b>：本类把真正的存储与二分委托给内置的 {@link DefaultIndexStore}，
 *       只接管"要不要为这条记录建索引"这一决策。绝大多数自定义索引需求都是这种形态
 *       （换采样密度、换键、加上限），没必要从头写二分与持久化。</li>
 *   <li><b>{@code onAppended} 的用法</b>：这是框架在每次 append 后调用的回调（默认空实现）。
 *       索引项的写入时机由插件自己决定，因此"每 N 条一次"这种策略只需在这里做计数。</li>
 *   <li><b>{@code lookup} 的契约</b>：必须返回 {@code <= lsn} 的最近索引项；
 *       采样变稀之后，"最近项"离目标更远，上层会从该偏移开始顺序扫——正确性不变，只是扫描变长。
 *       这就是"内存 ↔ 定位速度"的权衡点。</li>
 *   <li><b>{@code memoryBytes} 必须如实上报</b>：框架用它来守住 {@code maxIndexHeapBytes}，
 *       谎报会让索引层突破预算并最终 OOM。</li>
 * </ol>
 *
 * <h2>怎么用</h2>
 * <pre>{@code
 * new WalBuilder().indexStore(CountBasedIndexStore.ID)...
 * // 自定义采样密度：直接 new CountBasedIndexStore(64)（仅测试/同进程装配）
 * }</pre>
 */
public final class CountBasedIndexStore implements IndexStore {

    /** 插件 id。 */
    public static final String ID = "example:index-count-based";

    /** 默认采样间隔：每 64 条记录一个索引项（内置实现默认是 200 条一次，这里刻意更密）。 */
    public static final int DEFAULT_INTERVAL_RECORDS = 64;

    private final DefaultIndexStore delegate = new DefaultIndexStore();
    private final Map<Long, AtomicLong> counters = new ConcurrentHashMap<>();
    private final int intervalRecords;

    /**
     * 无参构造（{@code ServiceLoader} 要求），使用默认采样间隔。
     */
    public CountBasedIndexStore() {
        this(DEFAULT_INTERVAL_RECORDS);
    }

    /**
     * 指定采样间隔。
     *
     * @param intervalRecords 每多少条记录建一个索引项，必须为正
     */
    public CountBasedIndexStore(int intervalRecords) {
        if (intervalRecords <= 0) {
            throw new IllegalArgumentException("intervalRecords 必须为正: " + intervalRecords);
        }
        this.intervalRecords = intervalRecords;
    }

    // ------------------------------------------------------------------
    // WalPlugin
    // ------------------------------------------------------------------

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
        // 必须转交给委托对象：它需要用 PluginContext 里的 baseDir 决定 .idx 放哪里、
        // 用 WalConfig 拿到 maxIndexHeapBytes
        delegate.init(pluginContext);
    }

    @Override
    public void close() {
        delegate.close();
        counters.clear();
    }

    // ------------------------------------------------------------------
    // IndexStore
    // ------------------------------------------------------------------

    @Override
    public void openSegment(int partitionId, long segmentFirstLsn, long segmentCapacityBytes) {
        counters.put(segmentFirstLsn, new AtomicLong());
        delegate.openSegment(partitionId, segmentFirstLsn, segmentCapacityBytes);
    }

    @Override
    public void append(long lsn, long fileOffset) {
        // 框架/恢复路径可以直接调用 append：这里一律如实落盘（不做二次采样过滤），
        // 采样策略只作用在写路径的 onAppended 上。
        delegate.append(lsn, fileOffset);
    }

    @Override
    public IndexHit lookup(long lsn) {
        return delegate.lookup(lsn);
    }

    @Override
    public long lastIndexedLsn() {
        return delegate.lastIndexedLsn();
    }

    @Override
    public void truncateAfter(long watermarkLsn) {
        delegate.truncateAfter(watermarkLsn);
    }

    @Override
    public void rebuild(Iterator<IndexEntry> source) {
        delegate.rebuild(source);
    }

    @Override
    public long memoryBytes() {
        return delegate.memoryBytes();
    }

    @Override
    public void closeSegment(long segmentFirstLsn) {
        delegate.closeSegment(segmentFirstLsn);
        counters.remove(segmentFirstLsn);
    }

    /**
     * 写路径回调：每 {@code intervalRecords} 条记录才落一个索引项。
     *
     * <p>注意三点：
     * <ul>
     *   <li>本方法在<b>写线程栈上</b>被调用，所以这里只做一次 {@code AtomicLong.getAndIncrement}
     *       与一次取模，无分配、无 IO；</li>
     *   <li>计数器按 {@code segmentFirstLsn} 分桶，因为多个分区/多个段会并发到达；</li>
     *   <li>余数为 0 时落索引项 —— 这会调用委托对象的 {@code append}，属于框架允许的
     *       "插件可以做出块 IO 但不得超过 100µs" 的范畴（内置实现是写堆内数组）。</li>
     * </ul>
     *
     * @param partitionId 分区 ID
     * @param segmentFirstLsn 段标识
     * @param lsn 记录 LSN
     * @param fileOffset 帧在段内的偏移
     */
    @Override
    public void onAppended(int partitionId, long segmentFirstLsn, long lsn, long fileOffset) {
        AtomicLong counter = counters.get(segmentFirstLsn);
        if (counter == null) {
            counter = counters.computeIfAbsent(segmentFirstLsn, k -> new AtomicLong());
        }
        long n = counter.getAndIncrement();
        if (n % intervalRecords == 0L) {
            delegate.append(lsn, fileOffset);
        }
    }

    /**
     * 采样间隔。
     *
     * @return 条数
     */
    public int intervalRecords() {
        return intervalRecords;
    }

    /**
     * 已建的采样计数器个数（诊断用）。
     *
     * @return 个数
     */
    public int trackedSegments() {
        return counters.size();
    }

    @Override
    public String toString() {
        return "CountBasedIndexStore{interval=" + intervalRecords
                + ", segments=" + counters.size()
                + ", heapBytes=" + memoryBytes() + '}';
    }
}
