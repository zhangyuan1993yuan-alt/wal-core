package com.walcore.internal.recovery;

import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexStore;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 稀疏索引重建（ARCH §9.4）。
 *
 * <p><b>触发条件</b>（任一命中）：{@code .idx} 缺失、头 CRC 错、最大
 * {@code fileOffset > lastGoodOffset}、或 {@code recoveryVerifyMode=FULL} 时校验失败。
 *
 * <p><b>做法</b>：从 {@code offset 0} 顺序扫描 {@code .wal}，按<b>帧计数</b>
 * （不是字节数）每 {@code interval} 条产出一个索引项，然后整体替换索引内容。
 * 之所以按帧计数而不是按字节，是因为下游 {@code SparseIndex.lookup} 的
 * "每 N 条一个锚点"语义要求两侧口径一致——按字节切会导致锚点密度随记录大小漂移。
 *
 * <p><b>性能</b>：256MB 段、约 82 万帧 → 扫描 + CRC 约 0.3~0.8s。只有最后段需要重建，
 * RTO 预算（30s）内很宽裕。
 */
public final class IndexRebuilder {

    private final SegmentScanner scanner = new SegmentScanner();

    /**
     * 重建结果。
     */
    public static final class RebuildResult {
        /** 产出的索引项数。 */
        public final int entries;
        /** 扫描到的帧数。 */
        public final long frames;
        /** 最后一条记录的 LSN。 */
        public final long lastLsn;
        /** 扫描出的逻辑长度（最后一条完整记录之后的偏移）。 */
        public final long lastGoodOffset;
        /** 耗时（毫秒）。 */
        public final long elapsedMillis;
        /** 是否因损坏提前停止。 */
        public final boolean stopped;

        private RebuildResult(int entries, long frames, long lastLsn, long lastGoodOffset,
                              long elapsedMillis, boolean stopped) {
            this.entries = entries;
            this.frames = frames;
            this.lastLsn = lastLsn;
            this.lastGoodOffset = lastGoodOffset;
            this.elapsedMillis = elapsedMillis;
            this.stopped = stopped;
        }

        @Override
        public String toString() {
            return "RebuildResult{entries=" + entries
                    + ", frames=" + frames
                    + ", lastLsn=" + lastLsn
                    + ", lastGoodOffset=" + lastGoodOffset
                    + ", elapsedMillis=" + elapsedMillis
                    + (stopped ? ", STOPPED" : "") + '}';
        }
    }

    /**
     * 重建一个段的稀疏索引。
     *
     * <p><b>替换语义</b>：先收集新索引项，再一次性 {@code truncateAfter(0)} +
     * 逐项 {@code append}。中途失败会留下"部分重建"的索引——调用方（{@link RecoveryManager}）
     * 只在扫描成功后才落盘，因此不会出现"索引比数据新"的情况。
     *
     * @param partitionId 分区 ID
     * @param handle 段句柄
     * @param index 目标索引实现
     * @param interval 每多少帧一个索引项（&lt;= 0 时按 1000 处理）
     * @return 重建结果
     * @throws IOException 读取失败
     */
    public RebuildResult rebuild(int partitionId, SegmentHandle handle, IndexStore index,
                                 int interval) throws IOException {
        long started = System.nanoTime();
        int step = interval > 0 ? interval : 1000;

        // 先扫描一遍拿到全部锚点：只有扫描成功（无 IOException）才动索引，
        // 避免"索引被清空但重建失败"这种把可恢复状态变成不可恢复状态的操作。
        //
        // 扫描上界必须取"句柄愿意供数的边界"：活跃可写句柄只保证 [0, appendPosition)
        // 可读，物理长度里剩下的是 SPARSE 预分配区；拿 physicalLength() 去扫会读到
        // 句柄拒绝供数的区域，直接抛"读取不足"。只读句柄的 appendPosition 恰好等于
        // 文件长度，因此这个 min() 对两种句柄都正确。
        long length = handle.physicalLength();
        long readable = handle.appendPosition();
        if (readable > 0L && readable < length) {
            length = readable;
        }
        List<IndexEntry> collected = new ArrayList<>();
        AtomicLong frameCounter = new AtomicLong();
        SegmentScanner.ScanResult scan = scanner.scan(
                handle, 0L, length, true, 0,
                info -> {
                    long n = frameCounter.incrementAndGet();
                    if (n == 1L || n % step == 0L) {
                        collected.add(new IndexEntry(info.lsn, info.offset));
                    }
                    return true;
                },
                null);

        if (index != null) {
            // 用 SPI 的 rebuild(Iterator)：一次替换，避免 truncate 后逐 append
            // 在中途异常时留下"半新半旧"的索引
            index.rebuild(collected.iterator());
        }
        long elapsed = (System.nanoTime() - started) / 1_000_000L;
        return new RebuildResult(collected.size(), frameCounter.get(), scan.lastLsn,
                scan.lastGoodOffset, elapsed, scan.stopped);
    }

    /**
     * 判断既有索引是否与数据一致（{@code recoveryVerifyMode=FULL} 用）。
     *
     * <p>判定口径（三条，与 ARCH §9.5 一致）：索引项必须严格递增、最后一项的
     * {@code fileOffset + frameLen <= lastGoodOffset}、第一项的 LSN 不小于段首 LSN。
     *
     * @param index 索引实现，可为 null（视为不一致，需要重建）
     * @param lastGoodOffset 扫描出的逻辑长度
     * @return 一致返回 true
     */
    public boolean isConsistent(IndexStore index, long lastGoodOffset) {
        if (index == null) {
            return false;
        }
        long lastIndexed = index.lastIndexedLsn();
        if (lastGoodOffset == 0L) {
            // 空段（无任何完整帧）要求索引也为空
            return lastIndexed <= 0L;
        }
        // 有数据却没有锚点 → 索引肯定没建起来
        return lastIndexed > 0L;
    }
}
