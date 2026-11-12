package com.walcore.api;

import java.util.Collections;
import java.util.List;

/**
 * 崩溃恢复的结果报告（ARCH §9.5 / §17.2 #11）。
 *
 * <p>本类同时是 {@link WalException.RecoveryFailedException} 的附件：恢复失败时报告里
 * 每一条 {@link PartitionDiagnosis} 都会写明"卡在哪一段、哪个偏移、为什么"，
 * 这是排障时最有价值的信息。
 *
 * <p>不可变、线程安全。
 */
public final class RecoveryReport {

    private final boolean clean;
    private final long truncatedBytes;
    private final long recordsReplayed;
    private final List<PartitionDiagnosis> partitions;
    private final List<CorruptionEvent> corruptions;
    private final long elapsedMillis;
    private final long nextLsn;

    /**
     * 构造报告。
     *
     * @param clean 是否完全干净（无截断、无损坏、无跳过）
     * @param truncatedBytes 截断掉的总字节数
     * @param recordsReplayed 重放的记录条数
     * @param partitions 每分区诊断
     * @param corruptions 损坏事件
     * @param elapsedMillis 恢复耗时（毫秒）
     * @param nextLsn 恢复后分配器将要使用的下一个 LSN
     */
    public RecoveryReport(boolean clean, long truncatedBytes, long recordsReplayed,
                          List<PartitionDiagnosis> partitions, List<CorruptionEvent> corruptions,
                          long elapsedMillis, long nextLsn) {
        this.clean = clean;
        this.truncatedBytes = truncatedBytes;
        this.recordsReplayed = recordsReplayed;
        this.partitions = partitions == null
                ? Collections.emptyList() : List.copyOf(partitions);
        this.corruptions = corruptions == null
                ? Collections.emptyList() : List.copyOf(corruptions);
        this.elapsedMillis = elapsedMillis;
        this.nextLsn = nextLsn;
    }

    /**
     * 一个"空日志"的报告，用于首次开办（无分区、无写入）。
     *
     * @return 报告
     */
    public static RecoveryReport empty() {
        return new RecoveryReport(true, 0L, 0L,
                Collections.emptyList(), Collections.emptyList(), 0L, 1L);
    }

    /**
     * 是否完全干净。
     *
     * @return 干净返回 true
     */
    public boolean clean() {
        return clean;
    }

    /**
     * 尾部截断掉的总字节数。
     *
     * @return 字节数
     */
    public long truncatedBytes() {
        return truncatedBytes;
    }

    /**
     * 重放的记录条数。
     *
     * @return 条数
     */
    public long recordsReplayed() {
        return recordsReplayed;
    }

    /**
     * 每分区诊断。
     *
     * @return 不可变列表
     */
    public List<PartitionDiagnosis> partitions() {
        return partitions;
    }

    /**
     * 损坏事件。
     *
     * @return 不可变列表
     */
    public List<CorruptionEvent> corruptions() {
        return corruptions;
    }

    /**
     * 恢复耗时（毫秒）。
     *
     * @return 毫秒
     */
    public long elapsedMillis() {
        return elapsedMillis;
    }

    /**
     * 恢复完成后 {@code LsnAllocator} 的起点（= 全分区最大 LSN + 1）。
     *
     * @return LSN
     */
    public long nextLsn() {
        return nextLsn;
    }

    @Override
    public String toString() {
        return "RecoveryReport{clean=" + clean
                + ", truncatedBytes=" + truncatedBytes
                + ", recordsReplayed=" + recordsReplayed
                + ", partitions=" + partitions.size()
                + ", corruptions=" + corruptions.size()
                + ", elapsedMillis=" + elapsedMillis
                + ", nextLsn=" + nextLsn + '}';
    }

    /**
     * 单个分区的恢复诊断。
     */
    public static final class PartitionDiagnosis {

        private final int partitionId;
        private final int segmentCount;
        private final long lastSegmentFirstLsn;
        private final long lastGoodOffset;
        private final long lastLsn;
        private final long truncatedBytes;
        private final boolean indexRebuilt;
        private final String detail;

        /**
         * 构造分区诊断。
         *
         * @param partitionId 分区 ID
         * @param segmentCount 扫描的段数
         * @param lastSegmentFirstLsn 最后段名中的首条 LSN
         * @param lastGoodOffset 最后一条完整记录之后的偏移
         * @param lastLsn 该分区最后一条完好记录的 LSN
         * @param truncatedBytes 该分区截断的字节数
         * @param indexRebuilt 是否重建了 {@code .idx}
         * @param detail 人类可读的补充说明，可为 null
         */
        public PartitionDiagnosis(int partitionId, int segmentCount, long lastSegmentFirstLsn,
                                  long lastGoodOffset, long lastLsn, long truncatedBytes,
                                  boolean indexRebuilt, String detail) {
            this.partitionId = partitionId;
            this.segmentCount = segmentCount;
            this.lastSegmentFirstLsn = lastSegmentFirstLsn;
            this.lastGoodOffset = lastGoodOffset;
            this.lastLsn = lastLsn;
            this.truncatedBytes = truncatedBytes;
            this.indexRebuilt = indexRebuilt;
            this.detail = detail;
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
         * 扫描的段数。
         *
         * @return 段数
         */
        public int segmentCount() {
            return segmentCount;
        }

        /**
         * 最后一段文件名中的首条 LSN。
         *
         * <p><b>注意</b>：由于段名在"空分区轮转"时可能大于真实首条 LSN（T05 的
         * nameReservations 权衡），本值<b>不可</b>作为段内首条 LSN 使用，
         * 真实首条 LSN 以扫描结果 {@link #lastLsn()} 与首帧为准。
         *
         * @return LSN
         */
        public long lastSegmentFirstLsn() {
            return lastSegmentFirstLsn;
        }

        /**
         * 最后一条完整记录之后的文件偏移（恢复后 {@code appendPosition} 就是它）。
         *
         * @return 偏移
         */
        public long lastGoodOffset() {
            return lastGoodOffset;
        }

        /**
         * 该分区最后一条完好记录的 LSN，空分区为 0。
         *
         * @return LSN
         */
        public long lastLsn() {
            return lastLsn;
        }

        /**
         * 该分区被截断的字节数。
         *
         * @return 字节数
         */
        public long truncatedBytes() {
            return truncatedBytes;
        }

        /**
         * 是否重建了稀疏索引。
         *
         * @return 重建返回 true
         */
        public boolean indexRebuilt() {
            return indexRebuilt;
        }

        /**
         * 补充说明。
         *
         * @return 说明，可能为 null
         */
        public String detail() {
            return detail;
        }

        @Override
        public String toString() {
            return "PartitionDiagnosis{pid=" + partitionId
                    + ", segments=" + segmentCount
                    + ", lastSegmentFirstLsn=" + lastSegmentFirstLsn
                    + ", lastGoodOffset=" + lastGoodOffset
                    + ", lastLsn=" + lastLsn
                    + ", truncatedBytes=" + truncatedBytes
                    + ", indexRebuilt=" + indexRebuilt
                    + (detail == null ? "" : ", detail=" + detail) + '}';
        }
    }

    /**
     * 一处损坏事件（{@code SKIP_RECORD} 策略下每次重同步都会产出一条）。
     */
    public static final class CorruptionEvent {

        private final int partitionId;
        private final String segmentFile;
        private final long fileOffset;
        private final long skippedBytes;
        private final String rule;
        private final String detail;

        /**
         * 构造损坏事件。
         *
         * @param partitionId 分区 ID
         * @param segmentFile 段文件名（不含目录）
         * @param fileOffset 发现损坏的文件偏移
         * @param skippedBytes 为此跳过的字节数
         * @param rule 命中的 ARCH §9.2 规则标识，如 {@code "rule3.frameLen"}
         * @param detail 人类可读说明，可为 null
         */
        public CorruptionEvent(int partitionId, String segmentFile, long fileOffset,
                              long skippedBytes, String rule, String detail) {
            this.partitionId = partitionId;
            this.segmentFile = segmentFile;
            this.fileOffset = fileOffset;
            this.skippedBytes = skippedBytes;
            this.rule = rule;
            this.detail = detail;
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
         * 段文件名。
         *
         * @return 文件名
         */
        public String segmentFile() {
            return segmentFile;
        }

        /**
         * 发现损坏的文件偏移。
         *
         * @return 偏移
         */
        public long fileOffset() {
            return fileOffset;
        }

        /**
         * 跳过的字节数。
         *
         * @return 字节数
         */
        public long skippedBytes() {
            return skippedBytes;
        }

        /**
         * 命中的判定规则标识。
         *
         * @return 规则标识
         */
        public String rule() {
            return rule;
        }

        /**
         * 人类可读说明。
         *
         * @return 说明，可能为 null
         */
        public String detail() {
            return detail;
        }

        @Override
        public String toString() {
            return "CorruptionEvent{pid=" + partitionId
                    + ", file=" + segmentFile
                    + ", offset=" + fileOffset
                    + ", skipped=" + skippedBytes
                    + ", rule=" + rule
                    + (detail == null ? "" : ", detail=" + detail) + '}';
        }
    }
}
