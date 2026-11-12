package com.walcore.api;

import com.walcore.internal.util.Preconditions;

/**
 * 游标开办选项（{@link Wal#cursor(int, CursorOptions)}）。
 *
 * <p>本类<b>不可变</b>且线程安全，可作为常量反复使用。
 *
 * <pre>{@code
 * CursorOptions opts = CursorOptions.builder()
 *         .fromLsn(1_000_000L)
 *         .resumeFromCheckpoint(false)
 *         .verifyOnRead(true)
 *         .build();
 * }</pre>
 */
public final class CursorOptions {

    /** 默认选项：从分区第一条记录开始、不校验 CRC、不用 checkpoint。 */
    public static final CursorOptions DEFAULTS = builder().build();

    private final long fromLsn;
    private final boolean resumeFromCheckpoint;
    private final boolean verifyOnRead;
    private final long checkpointIntervalMillis;
    private final boolean useMappedWindows;

    private CursorOptions(Builder b) {
        this.fromLsn = b.fromLsn;
        this.resumeFromCheckpoint = b.resumeFromCheckpoint;
        this.verifyOnRead = b.verifyOnRead;
        this.checkpointIntervalMillis = b.checkpointIntervalMillis;
        this.useMappedWindows = b.useMappedWindows;
        Preconditions.checkArgument(fromLsn >= 0L, "fromLsn 不得为负: %s", fromLsn);
        Preconditions.checkPositive(checkpointIntervalMillis, "checkpointIntervalMillis");
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
     * 起始 LSN（包含）。默认 {@code 1}，即分区第一条记录。
     *
     * @return 起始 LSN
     */
    public long fromLsn() {
        return fromLsn;
    }

    /**
     * 是否从 {@code pXXXX.cursor} 里的 checkpoint 续读。
     *
     * <p>{@code true} 时 checkpoint 中记录的 LSN <b>覆盖</b> {@link #fromLsn()}。
     *
     * @return 需要续读返回 true
     */
    public boolean resumeFromCheckpoint() {
        return resumeFromCheckpoint;
    }

    /**
     * 是否在读取时逐帧校验 CRC32C。
     *
     * <p>默认 {@code false}：正常关闭后的段已经过 CRC 校验，重复校验纯属浪费 CPU。
     * 排障/审计场景可打开。
     *
     * @return 校验返回 true
     */
    public boolean verifyOnRead() {
        return verifyOnRead;
    }

    /**
     * checkpoint 落盘间隔（毫秒）。
     *
     * @return 毫秒
     */
    public long checkpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    /**
     * 读取非 mmap 段时是否使用 {@code MappedWindowCache} 的只读映射窗口。
     *
     * <p>打开后 FileChannel 后端的顺序读会走 8 × 32MB 的 LRU 映射窗口（零拷贝），
     * 关闭则退化为逐帧 {@code channel.read}。默认 {@code true}。
     *
     * @return 使用窗口返回 true
     */
    public boolean useMappedWindows() {
        return useMappedWindows;
    }

    @Override
    public String toString() {
        return "CursorOptions{fromLsn=" + fromLsn
                + ", resumeFromCheckpoint=" + resumeFromCheckpoint
                + ", verifyOnRead=" + verifyOnRead
                + ", checkpointIntervalMillis=" + checkpointIntervalMillis
                + ", useMappedWindows=" + useMappedWindows + '}';
    }

    /**
     * {@link CursorOptions} 的可变构建器（非线程安全，用完即弃）。
     */
    public static final class Builder {

        private long fromLsn = 1L;
        private boolean resumeFromCheckpoint = false;
        private boolean verifyOnRead = false;
        private long checkpointIntervalMillis = 1000L;
        private boolean useMappedWindows = true;

        private Builder() {
        }

        /**
         * 起始 LSN。
         *
         * @param lsn LSN，不得为负
         * @return this
         */
        public Builder fromLsn(long lsn) {
            this.fromLsn = lsn;
            return this;
        }

        /**
         * 是否从 checkpoint 续读。
         *
         * @param enabled 开关
         * @return this
         */
        public Builder resumeFromCheckpoint(boolean enabled) {
            this.resumeFromCheckpoint = enabled;
            return this;
        }

        /**
         * 是否逐帧校验 CRC。
         *
         * @param enabled 开关
         * @return this
         */
        public Builder verifyOnRead(boolean enabled) {
            this.verifyOnRead = enabled;
            return this;
        }

        /**
         * checkpoint 间隔。
         *
         * @param millis 毫秒
         * @return this
         */
        public Builder checkpointIntervalMillis(long millis) {
            this.checkpointIntervalMillis = millis;
            return this;
        }

        /**
         * 是否使用映射窗口读非 mmap 段。
         *
         * @param enabled 开关
         * @return this
         */
        public Builder useMappedWindows(boolean enabled) {
            this.useMappedWindows = enabled;
            return this;
        }

        /**
         * 构建不可变选项。
         *
         * @return 选项
         */
        public CursorOptions build() {
            return new CursorOptions(this);
        }
    }
}
