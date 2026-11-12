package com.walcore.internal.snapshot;

import com.walcore.api.ReplayListener;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SnapshotMeta;
import com.walcore.spi.SnapshotStore;

import java.io.InputStream;
import java.util.List;

/**
 * 快照与恢复的编排器：<b>打快照 → 算恢复起点 → 按需重放</b>（T15）。
 *
 * <h2>核心公式</h2>
 * <pre>
 * 恢复起点 = 最近快照.uptoLsn + 1（无快照则为 1）
 * 需要重放的记录数 = publishedLsn - 快照.uptoLsn
 * </pre>
 * 这就是"快照让重放量从 O(全部日志) 降到 O(快照之后的日志)"的全部内容。
 *
 * <h2>为什么快照点之后一条都不能少</h2>
 * 快照是"某个 LSN 处的状态镜像"，它<b>不含</b>该 LSN 之后的任何变更。
 * 因此：
 * <ul>
 *   <li>恢复必须从 {@code uptoLsn + 1} 开始重放，重复重放 {@code uptoLsn} 本身会导致
 *       幂等性差的业务重复执行；</li>
 *   <li>保留策略必须把 {@code uptoLsn} 当作不可删除边界（见 {@link #pinnedLsn}），
 *       否则"既不在快照里、也不在日志里"的记录会永久消失。</li>
 * </ul>
 */
public final class CheckpointManager implements AutoCloseable {

    /**
     * 游标工厂：由装配方注入，避免本类依赖 {@code SegmentManager} 的内部结构。
     */
    public interface CursorFactory {

        /**
         * 打开一个从 {@code fromLsn}（含）开始读的游标。
         *
         * @param partitionId 分区 ID
         * @param fromLsn 起始 LSN
         * @return 游标（调用方负责关闭）
         */
        WalCursor open(int partitionId, long fromLsn);
    }

    private final SnapshotStore store;
    private final CursorFactory cursors;
    private final MetricsRegistry metrics;

    /**
     * 构造编排器。
     *
     * @param store 快照存储
     * @param cursors 游标工厂
     * @param metrics 指标，可为 null
     */
    public CheckpointManager(SnapshotStore store, CursorFactory cursors, MetricsRegistry metrics) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.cursors = Preconditions.checkNotNull(cursors, "cursors");
        this.metrics = metrics;
    }

    /**
     * 打一份快照。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 覆盖到的最后一条 LSN（含）
     * @param state 状态内容流（本方法不负责关闭它）
     * @param stateBytes 内容字节数
     * @return 元数据
     */
    public SnapshotMeta checkpoint(int partitionId, long uptoLsn, InputStream state,
                                   long stateBytes) {
        SnapshotMeta meta = store.write(partitionId, uptoLsn, state, stateBytes);
        if (metrics != null) {
            metrics.increment(MetricsRegistry.SNAPSHOT_WRITES);
        }
        return meta;
    }

    /**
     * 打一份快照（字节数组便捷方法）。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 覆盖 LSN
     * @param state 内容
     * @return 元数据
     */
    public SnapshotMeta checkpoint(int partitionId, long uptoLsn, byte[] state) {
        return checkpoint(partitionId, uptoLsn, new java.io.ByteArrayInputStream(state),
                state.length);
    }

    /**
     * 恢复起点 LSN（含）。
     *
     * @param partitionId 分区 ID
     * @return LSN；无快照返回 1
     */
    public long resumeLsn(int partitionId) {
        return store.latest(partitionId).map(m -> m.uptoLsn() + 1L).orElse(1L);
    }

    /**
     * 不可删除边界 = 最近快照的 {@code uptoLsn}。
     *
     * @param partitionId 分区 ID
     * @return LSN；无快照返回 0
     */
    public long pinnedLsn(int partitionId) {
        return store.pinnedLsn(partitionId);
    }

    /**
     * 从快照点之后开始重放（状态恢复的正确姿势）。
     *
     * @param partitionId 分区 ID
     * @param listener 回调，可为 null（只统计条数）
     * @return 重放结果
     */
    public ReplayResult replaySinceSnapshot(int partitionId, ReplayListener listener) {
        return replay(partitionId, resumeLsn(partitionId), listener);
    }

    /**
     * 从第一条日志开始全量重放（对照用，也是没有快照时的默认行为）。
     *
     * @param partitionId 分区 ID
     * @param listener 回调，可为 null
     * @return 重放结果
     */
    public ReplayResult replayFull(int partitionId, ReplayListener listener) {
        return replay(partitionId, 1L, listener);
    }

    /**
     * 从指定 LSN 起重放。
     *
     * @param partitionId 分区 ID
     * @param fromLsn 起始 LSN（含）
     * @param listener 回调，可为 null
     * @return 重放结果
     */
    public ReplayResult replay(int partitionId, long fromLsn, ReplayListener listener) {
        long replayed = 0L;
        long lastLsn = fromLsn - 1L;
        try (WalCursor cursor = cursors.open(partitionId, fromLsn)) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                lastLsn = record.lsn();
                replayed++;
                if (listener != null) {
                    listener.onRecord(record);
                    if (replayed % 100_000L == 0L) {
                        listener.onProgress(partitionId, lastLsn, replayed);
                    }
                }
            }
        }
        if (listener != null) {
            listener.onProgress(partitionId, lastLsn, replayed);
        }
        return new ReplayResult(partitionId, fromLsn, lastLsn, replayed);
    }

    /**
     * 快照存储。
     *
     * @return 存储
     */
    public SnapshotStore store() {
        return store;
    }

    /**
     * 某分区的全部快照（升序）。
     *
     * @param partitionId 分区 ID
     * @return 列表
     */
    public List<SnapshotMeta> snapshots(int partitionId) {
        return store.list(partitionId);
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public String toString() {
        return "CheckpointManager{store=" + store.id() + '}';
    }

    /**
     * 一次重放的结果。
     */
    public static final class ReplayResult {

        private final int partitionId;
        private final long fromLsn;
        private final long lastLsn;
        private final long replayed;

        /**
         * 构造结果。
         *
         * @param partitionId 分区 ID
         * @param fromLsn 起始 LSN
         * @param lastLsn 最后重放的 LSN（无记录时为 {@code fromLsn - 1}）
         * @param replayed 重放条数
         */
        public ReplayResult(int partitionId, long fromLsn, long lastLsn, long replayed) {
            this.partitionId = partitionId;
            this.fromLsn = fromLsn;
            this.lastLsn = lastLsn;
            this.replayed = replayed;
        }

        /**
         * 分区 ID。
         *
         * @return 分区 ID
         */
        public int partitionId() {
            return partitionId;
        }

        /**
         * 起始 LSN。
         *
         * @return LSN
         */
        public long fromLsn() {
            return fromLsn;
        }

        /**
         * 最后重放的 LSN。
         *
         * @return LSN
         */
        public long lastLsn() {
            return lastLsn;
        }

        /**
         * 重放条数。
         *
         * @return 条数
         */
        public long replayed() {
            return replayed;
        }

        @Override
        public String toString() {
            return "ReplayResult{p" + partitionId + ", from=" + fromLsn
                    + ", last=" + lastLsn + ", replayed=" + replayed + '}';
        }
    }

    /**
     * 便捷：校验元数据与内容一致（实现方/测试用）。
     *
     * @param meta 元数据
     * @return 一致返回 true
     */
    public boolean verify(SnapshotMeta meta) {
        try (InputStream in = store.open(meta)) {
            in.readAllBytes();
            return true;
        } catch (WalException.CorruptedException e) {
            return false;
        } catch (java.io.IOException e) {
            throw new WalException.IoException("校验快照失败: " + meta.path(), e);
        }
    }
}
