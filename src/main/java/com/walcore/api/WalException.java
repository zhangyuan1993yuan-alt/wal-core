package com.walcore.api;

/**
 * WAL 框架所有受检与非受检异常的公共基类。
 *
 * <p>设计要点：
 * <ul>
 *   <li><b>全部为 {@link RuntimeException}</b>：{@code append()} 在热路径上，若为受检异常会在
 *       每个调用点强制 {@code try/catch}，既污染调用方代码也妨碍 JIT 优化。WAL 的失败大多是
 *       "进程内不可恢复"（磁盘满、段损坏、已关闭），让使用方在需要的地方统一捕获即可。</li>
 *   <li><b>每个子类有稳定的 {@link #errorCode()}</b>：错误码是<b>契约</b>，使用方按错误码做
 *       分支（例如 {@code WAL_CLOSED} 触发重建实例、{@code WAL_DISK_FULL} 触发告警），
 *       因此<b>永不得</b>修改已发布子类的错误码字符串。</li>
 *   <li><b>不记录日志</b>：异常只携带上下文，记录由调用边界统一完成，避免同一故障刷多条日志。</li>
 * </ul>
 *
 * <p>子类清单（共 15 个，与 ARCH §17.2 #14 一致）：
 * {@link ClosedException}、{@link OverloadedException}（含磁盘满语义）、{@link IoException}、
 * {@link CorruptedException}、{@link BackpressureException}、{@link PartitionException}、
 * {@link TimeoutException}、{@link PluginLoadException}、{@link SegmentMissingException}、
 * {@link IncompatibleFormatException}、{@link AlreadyOpenException}、
 * {@link OffHeapExhaustedException}、{@link CodecException}、{@link RecoveryFailedException}、
 * {@link TailerOverflowException}。
 */
public abstract class WalException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 描述信息
     */
    protected WalException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 描述信息
     * @param cause 根因
     */
    protected WalException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 稳定的错误码字符串，供使用方做程序化分支判断。
     *
     * @return 形如 {@code WAL_CLOSED} 的大写下划线串
     */
    public abstract String errorCode();

    // ==================================================================
    // 1. 生命周期
    // ==================================================================

    /** WAL 实例已关闭（{@code close()} 期间或之后仍有写入）。错误码 {@code WAL_CLOSED}。 */
    public static final class ClosedException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public ClosedException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_CLOSED";
        }
    }

    /** 双实例打开同一目录（{@code LOCK} 文件已被占用）。错误码 {@code WAL_ALREADY_OPEN}。 */
    public static final class AlreadyOpenException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public AlreadyOpenException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_ALREADY_OPEN";
        }
    }

    // ==================================================================
    // 2. 容量与背压
    // ==================================================================

    /**
     * 系统过载：磁盘满、健康度降级为 DEGRADED/FAILED 后拒绝新写入。
     *
     * <p>错误码：一般过载为 {@code WAL_OVERLOADED}；用 {@link #diskFull(String)} 构造的实例
     * 返回 {@code WAL_DISK_FULL}（ARCH §14 失败模式 #1 要求磁盘满必须能被单独识别）。
     */
    public static final class OverloadedException extends WalException {
        private static final long serialVersionUID = 1L;
        private final String code;

        /**
         * 构造一般过载异常（错误码 {@code WAL_OVERLOADED}）。
         *
         * @param message 描述信息
         */
        public OverloadedException(String message) {
            super(message);
            this.code = "WAL_OVERLOADED";
        }

        /**
         * 构造过载异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public OverloadedException(String message, Throwable cause) {
            super(message, cause);
            this.code = "WAL_OVERLOADED";
        }

        private OverloadedException(String message, Throwable cause, String code) {
            super(message, cause);
            this.code = code;
        }

        /**
         * 构造"磁盘满"语义的过载异常（错误码 {@code WAL_DISK_FULL}）。
         *
         * @param message 描述信息，建议包含分区、段路径与可用空间
         * @return 异常实例
         */
        public static OverloadedException diskFull(String message) {
            return new OverloadedException(message, null, "WAL_DISK_FULL");
        }

        /**
         * 构造"磁盘满"语义的过载异常。
         *
         * @param message 描述信息
         * @param cause 根因
         * @return 异常实例
         */
        public static OverloadedException diskFull(String message, Throwable cause) {
            return new OverloadedException(message, cause, "WAL_DISK_FULL");
        }

        @Override
        public String errorCode() {
            return code;
        }
    }

    /**
     * 背压超时：staging 缓冲满 / 脏字节超阈值 / 下一段未就绪，阻塞到超时仍无法写入。
     *
     * <p>错误码 {@code WAL_BACKPRESSURE}。与 {@link OverloadedException} 的区别：
     * 背压是<b>暂时性</b>的（重试可能成功），过载是<b>状态性</b>的（重试不会成功）。
     */
    public static final class BackpressureException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public BackpressureException(String message) {
            super(message);
        }

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public BackpressureException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_BACKPRESSURE";
        }
    }

    /** 堆外内存预算耗尽。错误码 {@code WAL_OFFHEAP_EXHAUSTED}。 */
    public static final class OffHeapExhaustedException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public OffHeapExhaustedException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_OFFHEAP_EXHAUSTED";
        }
    }

    // ==================================================================
    // 3. IO 与持久化
    // ==================================================================

    /** 底层 IO 失败（{@code force()} / {@code write()} / {@code truncate} 抛 IOException）。错误码 {@code WAL_IO}。 */
    public static final class IoException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public IoException(String message) {
            super(message);
        }

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public IoException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_IO";
        }
    }

    /** 等待持久化超时（{@code flushAndWait} 未在超时内达成，或 fsync watchdog 触发）。错误码 {@code WAL_TIMEOUT}。 */
    public static final class TimeoutException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public TimeoutException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_TIMEOUT";
        }
    }

    /** 段文件缺失（段序列 firstLsn 不连续，或被外部删除）。错误码 {@code WAL_SEGMENT_MISSING}。 */
    public static final class SegmentMissingException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息，建议包含缺失区间 {@code [a,b]}
         */
        public SegmentMissingException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_SEGMENT_MISSING";
        }
    }

    // ==================================================================
    // 4. 数据完整性与格式
    // ==================================================================

    /** 数据损坏（magic / frameLen / CRC 校验失败，且策略要求抛出而非截断）。错误码 {@code WAL_CORRUPTED}。 */
    public static final class CorruptedException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息，建议包含分区/段/偏移/期望与实际 CRC
         */
        public CorruptedException(String message) {
            super(message);
        }

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public CorruptedException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_CORRUPTED";
        }
    }

    /** 磁盘格式版本高于本核心库可理解的版本。错误码 {@code WAL_INCOMPATIBLE_FORMAT}。 */
    public static final class IncompatibleFormatException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息，建议包含磁盘 formatVersion 与核心支持版本
         */
        public IncompatibleFormatException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_INCOMPATIBLE_FORMAT";
        }
    }

    /** 恢复失败（三条判定条件未全部满足）。错误码 {@code WAL_RECOVERY_FAILED}。 */
    public static final class RecoveryFailedException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因，可为 null
         */
        public RecoveryFailedException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_RECOVERY_FAILED";
        }
    }

    // ==================================================================
    // 5. 语义与扩展点
    // ==================================================================

    /** 分区不存在 / 已冻结 / 已清除 / ID 越界。错误码 {@code WAL_PARTITION}。 */
    public static final class PartitionException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public PartitionException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_PARTITION";
        }
    }

    /** 插件发现/加载/仲裁/热切换失败。错误码 {@code WAL_PLUGIN}。 */
    public static final class PluginLoadException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息，{@code FAIL_FAST} 模式下必须包含接口全名、已尝试的
         *                ClassLoader 列表、期望的 provider 文件名、已发现的 id 列表
         */
        public PluginLoadException(String message) {
            super(message);
        }

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public PluginLoadException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_PLUGIN";
        }
    }

    /** 编码器失败（codec 抛异常、输出长度不符、未知 codecId）。错误码 {@code WAL_CODEC}。 */
    public static final class CodecException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息，建议包含 codecId 与已加载 codec 列表
         */
        public CodecException(String message) {
            super(message);
        }

        /**
         * 构造异常。
         *
         * @param message 描述信息
         * @param cause 根因
         */
        public CodecException(String message, Throwable cause) {
            super(message, cause);
        }

        @Override
        public String errorCode() {
            return "WAL_CODEC";
        }
    }

    /** Tailer 订阅者消费落后超过缓冲上限。错误码 {@code WAL_TAILER_OVERFLOW}。 */
    public static final class TailerOverflowException extends WalException {
        private static final long serialVersionUID = 1L;

        /**
         * 构造异常。
         *
         * @param message 描述信息
         */
        public TailerOverflowException(String message) {
            super(message);
        }

        @Override
        public String errorCode() {
            return "WAL_TAILER_OVERFLOW";
        }
    }
}
