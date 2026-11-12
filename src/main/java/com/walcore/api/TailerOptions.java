package com.walcore.api;

import com.walcore.internal.util.Preconditions;

/**
 * 实时订阅的开启选项（不可变值对象）。
 *
 * <p>用法：
 * <pre>{@code
 * WalTailer tailer = wal.tailer(TailerOptions.builder()
 *         .fromLsn(1L)                                  // 先追历史
 *         .bufferSize(65_536)                           // 最多落后 65536 条
 *         .overflowPolicy(WalConfig.TailerOverflowPolicy.DROP_TO_LATEST)
 *         .handler(record -> handle(record))
 *         .build());
 * }</pre>
 */
public final class TailerOptions {

    /** {@code bufferSize} 下限：小于这个值任何抖动都会立刻溢出，没有实用价值。 */
    public static final int MIN_BUFFER_SIZE = 8;

    /** {@code bufferSize} 上限：10 亿条足够任何场景，同时避免 {@code int} 溢出。 */
    public static final int MAX_BUFFER_SIZE = 1_000_000_000;

    private final int partitionId;
    private final long fromLsn;
    private final int bufferSize;
    private final WalConfig.TailerOverflowPolicy overflowPolicy;
    private final TailHandler handler;

    private TailerOptions(int partitionId, long fromLsn, int bufferSize,
                          WalConfig.TailerOverflowPolicy overflowPolicy, TailHandler handler) {
        this.partitionId = partitionId;
        this.fromLsn = fromLsn;
        this.bufferSize = bufferSize;
        this.overflowPolicy = overflowPolicy;
        this.handler = handler;
    }

    /**
     * 创建构建器。
     *
     * @return 构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 订阅的分区 ID。
     *
     * <p>为什么要放在选项里：{@code wal.tailer(options)} 只有这一个参数，
     * 而"合并订阅所有分区"无法给出严格递增的 LSN 序列（全局 LSN 单调，但跨分区的
     * <i>交付顺序</i>取决于并发写入时序），会产生"看似乱序"的假象。
     * 因此一个 tailer 明确绑定一个分区；需要多分区时开多个 tailer。
     *
     * @return 分区 ID
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * 起始 LSN（含）。{@code 1} 表示"从历史最早开始追"。
     *
     * @return LSN
     */
    public long fromLsn() {
        return fromLsn;
    }

    /**
     * 允许落后的最大条数，超过即按 {@link #overflowPolicy()} 处理。
     *
     * @return 条数
     */
    public int bufferSize() {
        return bufferSize;
    }

    /**
     * 溢出策略。
     *
     * @return 策略
     */
    public WalConfig.TailerOverflowPolicy overflowPolicy() {
        return overflowPolicy;
    }

    /**
     * 回调。
     *
     * @return handler，非 null
     */
    public TailHandler handler() {
        return handler;
    }

    @Override
    public String toString() {
        return "TailerOptions{partitionId=" + partitionId
                + ", fromLsn=" + fromLsn
                + ", bufferSize=" + bufferSize
                + ", overflowPolicy=" + overflowPolicy
                + ", handler=" + handler.getClass().getSimpleName() + '}';
    }

    /**
     * {@link TailerOptions} 的流式构建器。
     *
     * <p>默认值：{@code partitionId=0}、{@code fromLsn=1}、{@code bufferSize=65536}、
     * {@code overflowPolicy=WalConfig.TailerOverflowPolicy#DISCONNECT}。
     */
    public static final class Builder {

        private int partitionId;
        private long fromLsn = 1L;
        private int bufferSize = 65_536;
        private WalConfig.TailerOverflowPolicy overflowPolicy =
                WalConfig.TailerOverflowPolicy.DISCONNECT;
        private TailHandler handler;

        private Builder() {
        }

        /**
         * 设置订阅的分区。
         *
         * @param pid 分区 ID，非负
         * @return this
         */
        public Builder partitionId(int pid) {
            Preconditions.checkArgument(pid >= 0, "partitionId 不得为负: %s", pid);
            this.partitionId = pid;
            return this;
        }

        /**
         * 从指定 LSN 开始订阅（含）。
         *
         * @param lsn LSN，必须 {@code >= 0}；{@code 0} 等价于 1（从最早开始）
         * @return this
         */
        public Builder fromLsn(long lsn) {
            Preconditions.checkArgument(lsn >= 0L, "fromLsn 不得为负: %s", lsn);
            this.fromLsn = Math.max(1L, lsn);
            return this;
        }

        /**
         * 设置允许落后的最大条数。
         *
         * @param records 条数，取值 {@code [8, 10^9]}
         * @return this
         */
        public Builder bufferSize(int records) {
            Preconditions.checkInRange(records, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE, "bufferSize");
            this.bufferSize = records;
            return this;
        }

        /**
         * 设置溢出策略。
         *
         * @param policy 策略，非 null
         * @return this
         */
        public Builder overflowPolicy(WalConfig.TailerOverflowPolicy policy) {
            this.overflowPolicy = Preconditions.checkNotNull(policy, "overflowPolicy");
            return this;
        }

        /**
         * 设置回调（必填）。
         *
         * @param handler 回调，非 null
         * @return this
         */
        public Builder handler(TailHandler handler) {
            this.handler = Preconditions.checkNotNull(handler, "handler");
            return this;
        }

        /**
         * 构建选项。
         *
         * @return 不可变选项
         * @throws IllegalArgumentException 未设置 handler
         */
        public TailerOptions build() {
            if (handler == null) {
                throw new IllegalArgumentException(
                        "TailerOptions 必须设置 handler：没有回调的订阅只会空转浪费一个线程");
            }
            return new TailerOptions(partitionId, fromLsn, bufferSize, overflowPolicy, handler);
        }
    }
}
