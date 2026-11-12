package com.walcore.internal.groupcommit;

import com.walcore.internal.append.DirtyTracker;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.util.Preconditions;

/**
 * 一个刷盘批次：<b>脏分区位图快照 + 字节数 + 每分区的水位目标 + 完成/失败通知</b>。
 *
 * <h2>为什么水位要在 force 之前捕获</h2>
 * {@link #maxLsnOf(int)} 的值是"批次开始时该分区已发布的 LSN"。这个值一定
 * <b>≤ 本次 force 真正刷下去的数据</b>（因为 force 会刷到调用时刻为止的全部字节，
 * 比捕获时刻更晚）。发布一个"偏小"的水位永远安全；
 * 反过来若用 force 之后读到的 publishedLsn，就可能把"尚未写完的数据"算作已持久——
 * 那是假持久性，PRD §5 明令禁止。
 *
 * <h2>为什么本类是"可复用"的而不是每批 new 一个</h2>
 * 批次频率是 {@code 1/200µs} = 5000 次/秒。若每批分配 {@code long[maxPartitions]}
 * （4096 分区 = 32KB），就是 160MB/s 的垃圾。因此每个 flusher 线程持有<b>一个</b>
 * 实例，靠 {@link #begin} 重置并把内部数组复用起来。对外仍然呈现"一个批次一个对象"的
 * 语义（{@link #mask()}、{@link #bytes()}、{@link #maxLsnOf(int)}）。
 */
public final class FlushBatch {

    /**
     * 批次结束通知（由 {@link GroupCommitCoordinator} 实现）。
     */
    public interface Listener {

        /**
         * 批次全部 force 成功。
         *
         * @param batch 批次
         */
        void onBatchCompleted(FlushBatch batch);

        /**
         * 批次中某个分区 force 失败。
         *
         * @param batch 批次
         * @param partitionId 失败分区
         * @param cause 失败原因
         */
        void onBatchFailed(FlushBatch batch, int partitionId, Throwable cause);
    }

    private final PartitionRegistry registry;
    private final DirtyTracker tracker;
    private final Listener listener;
    private final long[] maskWords;
    private final long[] maxLsnByPid;
    private final int maxPartitions;

    private long bytes;
    private long records;
    private int upperWord;
    private long maxLsn;
    private boolean completed;
    private boolean failed;
    private Throwable failure;
    private int failedPartition = -1;

    /**
     * 构造批次（每个 flusher 线程一个，长期复用）。
     *
     * @param registry 分区注册表（读取水位）
     * @param tracker 脏追踪器（读取待刷记录数）
     * @param listener 完成通知
     */
    public FlushBatch(PartitionRegistry registry, DirtyTracker tracker, Listener listener) {
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.tracker = Preconditions.checkNotNull(tracker, "tracker");
        this.listener = Preconditions.checkNotNull(listener, "listener");
        this.maxPartitions = registry.maxPartitions();
        this.maskWords = new long[tracker.wordCount()];
        this.maxLsnByPid = new long[maxPartitions];
    }

    /**
     * 开始一个新批次：把脏位图与计数快照进本对象，并捕获各分区的水位目标。
     *
     * <p>顺序很重要：<b>先捕获水位，再 force</b>。见类注释。
     *
     * @param drainedBytes 本轮取走的脏字节
     * @param maskUpperWord 非零 word 的上界（{@code DrainTracker.drain} 的返回值）
     */
    public void begin(long drainedBytes, int maskUpperWord) {
        this.bytes = drainedBytes;
        this.upperWord = maskUpperWord;
        this.completed = false;
        this.failed = false;
        this.failure = null;
        this.failedPartition = -1;
        this.records = 0L;
        this.maxLsn = 0L;
        long recordSum = 0L;
        for (int w = 0; w < maskUpperWord; w++) {
            long word = maskWords[w];
            if (word == 0L) {
                continue;
            }
            long bits = word;
            while (bits != 0L) {
                int bit = Long.numberOfTrailingZeros(bits);
                bits &= bits - 1L;
                int pid = (w << 6) + bit;
                if (pid >= maxPartitions) {
                    continue;
                }
                PartitionImpl p = registry.get(pid);
                long lsn = p == null ? 0L : p.publishedLsn();
                maxLsnByPid[pid] = lsn;
                if (lsn > maxLsn) {
                    maxLsn = lsn;
                }
                recordSum += tracker.pendingRecords(pid);
            }
        }
        this.records = recordSum;
    }

    /**
     * 内部位图数组（<b>仅供协调器写入</b>：由 {@code DirtyTracker.drain} 直接填充，
     * 避免每批一次 32KB 的数组分配）。
     *
     * @return 内部数组，长度等于 {@code DirtyTracker.wordCount()}
     */
    public long[] maskBuffer() {
        return maskWords;
    }

    /**
     * 该批次是否包含某分区。
     *
     * @param partitionId 分区 ID
     * @return 包含返回 true
     */
    public boolean contains(int partitionId) {
        int w = partitionId >>> 6;
        if (w < 0 || w >= upperWord) {
            return false;
        }
        return (maskWords[w] & DirtyTracker.bitOf(partitionId)) != 0L;
    }

    /**
     * 单 word 版掩码（仅 {@code maxPartitions <= 64} 时有意义）。
     *
     * @return 掩码
     */
    public long mask() {
        return maskWords.length == 0 ? 0L : maskWords[0];
    }

    /**
     * 掩码 word 数（本批次有效上界）。
     *
     * @return word 数
     */
    public int upperWord() {
        return upperWord;
    }

    /**
     * 读取掩码的某 word。
     *
     * @param word 下标
     * @return 位图
     */
    public long maskWord(int word) {
        return word >= 0 && word < maskWords.length ? maskWords[word] : 0L;
    }

    /**
     * 本轮脏字节数。
     *
     * @return 字节
     */
    public long bytes() {
        return bytes;
    }

    /**
     * 本轮脏记录数（用于 {@code batchMaxRecords} 触发判断的观测值）。
     *
     * @return 条数
     */
    public long records() {
        return records;
    }

    /**
     * 某分区在本批次中的水位目标（force 之前捕获）。
     *
     * @param partitionId 分区 ID
     * @return LSN；不在本批次返回 0
     */
    public long maxLsnOf(int partitionId) {
        if (partitionId < 0 || partitionId >= maxPartitions) {
            return 0L;
        }
        return maxLsnByPid[partitionId];
    }

    /**
     * 本批次的最大水位目标（跨分区）。
     *
     * @return LSN
     */
    public long maxLsn() {
        return maxLsn;
    }

    /**
     * 记录某分区已成功 force（由协调器调用，用于统计）。
     *
     * @param partitionId 分区 ID
     */
    void onPartitionForced(int partitionId) {
        if (partitionId >= 0 && partitionId < maxPartitions) {
            tracker.clearPendingRecords(partitionId);
        }
    }

    /**
     * 批次成功完成：通知等待者。
     */
    public void complete() {
        if (completed || failed) {
            return;
        }
        completed = true;
        listener.onBatchCompleted(this);
    }

    /**
     * 批次失败：通知等待者拿到 IO 异常（<b>水位不得推进</b>）。
     *
     * @param cause 失败原因
     */
    public void fail(Throwable cause) {
        fail(-1, cause);
    }

    /**
     * 某分区失败。
     *
     * @param partitionId 失败分区
     * @param cause 失败原因
     */
    public void fail(int partitionId, Throwable cause) {
        if (completed || failed) {
            return;
        }
        failed = true;
        failure = cause;
        failedPartition = partitionId;
        listener.onBatchFailed(this, partitionId, cause);
    }

    /**
     * 是否已成功完成。
     *
     * @return 完成返回 true
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * 是否失败。
     *
     * @return 失败返回 true
     */
    public boolean isFailed() {
        return failed;
    }

    /**
     * 失败原因。
     *
     * @return 原因；未失败返回 null
     */
    public Throwable failure() {
        return failure;
    }

    /**
     * 失败的分区 ID。
     *
     * @return 分区 ID；未失败返回 -1
     */
    public int failedPartition() {
        return failedPartition;
    }

    @Override
    public String toString() {
        return "FlushBatch{upperWord=" + upperWord
                + ", bytes=" + bytes
                + ", records=" + records
                + ", maxLsn=" + maxLsn
                + ", completed=" + completed
                + ", failed=" + failed + '}';
    }
}
