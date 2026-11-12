package com.walcore.spi;

import java.util.Iterator;

/**
 * ③ 索引 SPI：{@code lsn -> fileOffset} 的稀疏映射，支持 O(log n) 定位。
 *
 * <h2>契约</h2>
 * <ul>
 *   <li>一个实现实例可服务多个段，用 {@code segmentFirstLsn} 区分；切换段必须调用
 *       {@link #openSegment}。</li>
 *   <li>{@link #append} 的调用频率是 {@code 1/indexIntervalRecords}（默认 0.1%），
 *       实现可以做重活（包括小块 IO），但<b>不得</b>阻塞超过 100µs，因为它与写线程同栈。</li>
 *   <li>{@link #lookup} 必须返回 {@code <= lsn} 的最近索引项；若 {@code lsn} 小于段内首个
 *       索引项，返回 {@link IndexHit#NONE}，由上层做"从段首顺序扫"的兜底。</li>
 *   <li>{@link #memoryBytes} 必须如实上报堆内 + 堆外占用，因为整个索引层的上限是
 *       {@code WalConfig.maxIndexHeapBytes}（默认 192MB），超限后框架要求索引降级为
 *       纯文件二分而不是 OOM。</li>
 * </ul>
 */
public interface IndexStore extends WalPlugin {

    /**
     * 为一个段建立/打开索引。
     *
     * @param partitionId 分区 ID
     * @param segmentFirstLsn 段内首条记录 LSN（同时是段标识）
     * @param segmentCapacityBytes 段容量（用于预分配/校验）
     */
    void openSegment(int partitionId, long segmentFirstLsn, long segmentCapacityBytes);

    /**
     * 追加一条索引项。
     *
     * @param lsn 该记录 LSN
     * @param fileOffset 该帧在 {@code .wal} 中的起始字节偏移
     */
    void append(long lsn, long fileOffset);

    /**
     * 查找 {@code <= lsn} 的最近索引项。
     *
     * @param lsn 目标 LSN
     * @return 命中项；无则 {@link IndexHit#NONE}
     */
    IndexHit lookup(long lsn);

    /**
     * 已索引的最大 LSN。
     *
     * @return LSN；索引为空返回 0
     */
    long lastIndexedLsn();

    /**
     * 恢复期截断：丢弃 {@code lsn > watermarkLsn} 的条目。
     *
     * @param watermarkLsn 水位
     */
    void truncateAfter(long watermarkLsn);

    /**
     * 全量重建（源为顺序扫描的流）。
     *
     * @param source 索引项迭代器
     */
    void rebuild(Iterator<IndexEntry> source);

    /**
     * 索引占用的字节数（堆内 + 堆外），需计入堆外/堆内预算。
     *
     * @return 字节数
     */
    long memoryBytes();

    /**
     * 关闭某段索引并释放资源。
     *
     * @param segmentFirstLsn 段标识
     */
    void closeSegment(long segmentFirstLsn);

    /**
     * 每追加一条记录时被通知（<b>ARCH 契约修订，T11 落地</b>）。
     *
     * <h2>为什么加这个回调</h2>
     * 原契约只有 {@link #openSegment} / {@link #append} / {@link #lookup}，
     * 但框架内部的写入路径是"段持有自己的 {@code SparseIndex} 并直接调它的
     * {@code append}"，第三方 {@link IndexStore} 实现因此<b>永远收不到逐条追加事件</b>，
     * 其 {@link #lookup} 只能返回 {@code IndexHit.NONE}，读路径静默退化为顺序扫描——
     * 正确但慢，且完全无法察觉。这是"SPI 看起来能扩展、实际扩展了没用"的典型陷阱。
     *
     * <p>新增本方法后，框架在<b>每条记录</b>写入段之后调用一次，第三方实现据此维护
     * 自己的锚点，{@link #lookup} 才能真正生效。内置 {@code DefaultIndexStore}
     * 直接复用段内的 {@code SparseIndex}，因此默认实现是空方法（零开销）。
     *
     * <h2>调用约束（实现者必须遵守）</h2>
     * <ul>
     *   <li>本方法在<b>分区写锁内、append 热路径上</b>被调用：实现必须<b>无阻塞</b>、
     *       不做 IO、不分配大对象，否则直接拖垮写入吞吐；</li>
     *   <li>{@code partitionId} / {@code segmentFirstLsn} 用于定位段，
     *       {@code fileOffset} 是该帧在段文件中的起始偏移；</li>
     *   <li>允许不同分区的调用并发到达，实现需自行保证线程安全。</li>
     * </ul>
     *
     * <p>因为带默认实现，本方法<b>不破坏</b>已发布的 {@code apiVersion=1} 插件。
     *
     * @param partitionId 分区 ID
     * @param segmentFirstLsn 段文件名中的首条 LSN（注意可能大于段内真实首条 LSN）
     * @param lsn 本条记录的全局 LSN
     * @param fileOffset 该记录帧在段文件中的起始偏移
     */
    default void onAppended(int partitionId, long segmentFirstLsn, long lsn, long fileOffset) {
        // 内置实现无需处理（它通过段内 SparseIndex 直接写入）
    }
}
