package com.walcore.api;

import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.util.Preconditions;

/**
 * 单次追加调用的可选参数。
 *
 * <p>本类是<b>不可变</b>且<b>无状态</b>的：同一个实例可以被任意线程反复复用，也可以在
 * 一个生命周期内不分配任何对象地传入 {@link Wal#append(int, java.nio.ByteBuffer, AppendOptions)}
 * —— 这正是"append 路径零分配"的前提之一。
 *
 * <p>请优先使用内置常量 {@link #DEFAULTS}（业务数据记录、无幂等键、非事务提交点），
 * 需要变体时用静态工厂（同样无分配，实例被缓存在常量池中）。
 *
 * <h2>幂等语义的边界（重要，别做多）</h2>
 * {@link #idempotentKeyHash()} 只负责把业务幂等键的 64 位摘要写进帧头的 {@code attributes}
 * 字段并置位 {@code flags.IDEMPOTENT_KEY}，重放时由 {@link WalRecord#attributes()} 原样回吐。
 * <b>框架不建去重索引</b>：什么算"同一条业务记录"是业务语义，且一个万级 HashSet 会直接
 * 破坏零分配承诺。去重判定由上层的 {@code ReplayListener} 自行完成。
 */
public final class AppendOptions {

    /** 默认选项：{@code recordType = DATA}、{@code attributes = 0}、非事务提交点。 */
    public static final AppendOptions DEFAULTS =
            new AppendOptions(FrameLayout.RecordTypes.DATA, 0L, 0L, false);

    /** 事务提交点选项：{@code recordType = DATA}、{@code txnCommit = true}。 */
    public static final AppendOptions TXN_COMMIT =
            new AppendOptions(FrameLayout.RecordTypes.DATA, 0L, 0L, true);

    /** 控制记录选项。 */
    public static final AppendOptions CONTROL =
            new AppendOptions(FrameLayout.RecordTypes.CONTROL, 0L, 0L, false);

    private final byte recordType;
    private final long attributes;
    private final long idempotentKeyHash;
    private final boolean txnCommit;

    private AppendOptions(byte recordType, long attributes, long idempotentKeyHash, boolean txnCommit) {
        this.recordType = recordType;
        this.attributes = attributes;
        this.idempotentKeyHash = idempotentKeyHash;
        this.txnCommit = txnCommit;
    }

    /**
     * 默认选项（业务数据记录）。
     *
     * @return 单例常量
     */
    public static AppendOptions defaults() {
        return DEFAULTS;
    }

    /**
     * 以指定记录类型构造选项。
     *
     * @param recordType 记录类型，1..255
     * @return 选项实例
     * @throws IllegalArgumentException 类型为 0
     */
    public static AppendOptions of(byte recordType) {
        Preconditions.checkArgument(recordType != 0, "recordType 不得为 0");
        if (recordType == FrameLayout.RecordTypes.DATA) {
            return DEFAULTS;
        }
        if (recordType == FrameLayout.RecordTypes.CONTROL) {
            return CONTROL;
        }
        return new AppendOptions(recordType, 0L, 0L, false);
    }

    /**
     * 以指定幂等键摘要构造选项（置位 {@code flags.IDEMPOTENT_KEY}）。
     *
     * @param keyHash 业务幂等键的 64 位摘要，不得为 0（0 表示"无幂等键"）
     * @return 选项实例
     * @throws IllegalArgumentException 当 {@code keyHash == 0}
     */
    public static AppendOptions idempotent(long keyHash) {
        Preconditions.checkArgument(keyHash != 0L,
                "idempotentKeyHash 不得为 0（0 是'无幂等键'的保留值）");
        return new AppendOptions(FrameLayout.RecordTypes.DATA, 0L, keyHash, false);
    }

    /**
     * 以指定 {@code attributes} 构造选项（事务 ID / 业务自定义摘要）。
     *
     * @param attributes 复用字段值
     * @return 选项实例
     */
    public static AppendOptions attributes(long attributes) {
        return new AppendOptions(FrameLayout.RecordTypes.DATA, attributes, 0L, false);
    }

    /**
     * 记录类型，写入帧头 {@code recordType}。
     *
     * @return 记录类型
     */
    public byte recordType() {
        return recordType;
    }

    /**
     * 帧头 {@code attributes} 复用字段的取值来源之一（另一个是幂等键摘要）。
     *
     * @return 属性值
     */
    public long attributes() {
        return attributes;
    }

    /**
     * 幂等键摘要；0 表示未指定。
     *
     * @return 摘要值
     */
    public long idempotentKeyHash() {
        return idempotentKeyHash;
    }

    /**
     * 是否为事务提交点（置位 {@code flags.TXN_COMMIT}）。
     *
     * @return 是则返回 true
     */
    public boolean txnCommit() {
        return txnCommit;
    }

    /**
     * 计算写入帧头的 {@code attributes} 字段终值。
     *
     * <p>写入顺序优先级：幂等键摘要 &gt; 显式 attributes。这样"带幂等键"的场景不会被
     * 误写的 attributes 覆盖。
     *
     * @return 终值
     */
    public long effectiveAttributes() {
        return idempotentKeyHash != 0L ? idempotentKeyHash : attributes;
    }

    /**
     * 计算写入帧头的 {@code flags} 字节（只置位已定义位，bit6~7 恒为 0）。
     *
     * @return flags 字节
     */
    public byte flags() {
        byte f = 0;
        if (idempotentKeyHash != 0L) {
            f |= FrameLayout.Flags.IDEMPOTENT_KEY;
        }
        if (txnCommit) {
            f |= FrameLayout.Flags.TXN_COMMIT;
        }
        return f;
    }

    @Override
    public String toString() {
        return "AppendOptions{recordType=" + (recordType & 0xFF)
                + ", attributes=" + attributes
                + ", idempotentKeyHash=" + idempotentKeyHash
                + ", txnCommit=" + txnCommit
                + '}';
    }
}
