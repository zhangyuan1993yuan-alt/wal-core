package com.walcore.examples.spi;

import com.walcore.api.WalVersion;
import com.walcore.spi.PluginContext;
import com.walcore.spi.ReplicationSink;
import com.walcore.spi.WalRecordView;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * 示例插件 ③：<b>行打印复制 sink</b>——把每条记录打印成一行，用于本地联调与"复制链路是否通"的观测。
 *
 * <h2>这个例子要教会什么</h2>
 * <ul>
 *   <li>{@link ReplicationSink} 的四个回调语义：分区新增 / 分区移除 / 收到记录 / 截断；
 *       框架保证<b>同一分区的回调按 LSN 递增</b>到达，跨分区不保证。</li>
 *   <li><b>绝不能在回调里做重活</b>：本类只做 {@code println}（并且允许调用方换成
 *       {@code ByteArrayOutputStream} 做测试）。真实复制实现应当"投递到有界队列 + 异步发送"，
 *       队列满时<b>丢最旧</b>而不是阻塞——阻塞会让 WAL 变成消息队列，把写入拖死。</li>
 *   <li>{@link WalRecordView#payload()} 返回的是<b>只读视图</b>，只在本次回调内有效；
 *       需要留用就必须自己拷贝（见下面的 {@code copyPayload}）。</li>
 * </ul>
 *
 * <h2>怎么用</h2>
 * <pre>{@code
 * new WalBuilder().replicationSink(PrintlnReplicationSink.ID)...
 * }</pre>
 */
public final class PrintlnReplicationSink implements ReplicationSink {

    /** 插件 id。 */
    public static final String ID = "example:replication-println";

    private final PrintStream out;
    private final LongAdder records = new LongAdder();
    private final LongAdder bytes = new LongAdder();
    private final LongAdder truncated = new LongAdder();
    private final Map<Integer, Long> lastLsnByPartition = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile PluginContext context;

    /**
     * 无参构造：输出到 {@code System.out}（{@code ServiceLoader} 要求无参构造）。
     */
    public PrintlnReplicationSink() {
        this(System.out);
    }

    /**
     * 指定输出流（测试时传 {@code ByteArrayOutputStream} 包出来的 PrintStream）。
     *
     * @param out 输出流，非 null
     */
    public PrintlnReplicationSink(PrintStream out) {
        this.out = out == null ? System.out : out;
    }

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
        this.context = pluginContext;
    }

    @Override
    public void close() {
        this.context = null;
        out.flush();
    }

    /**
     * 复制模式：本示例只是本地打印，副本等价于"标准输出"，因此声明 {@code ASYNC}。
     *
     * <p>声明 {@code SEMI_SYNC}/{@code SYNC} 会让框架把 {@code flushAndWait} 与
     * {@link #replicatedLsn(int)} 绑定——如果实现实际上并没有等到副本确认，
     * 那就是<b>假同步</b>，故障切换时会丢数据。宁可选低一档，也不要虚报。
     *
     * @return {@code ASYNC}
     */
    @Override
    public Mode mode() {
        return Mode.ASYNC;
    }

    @Override
    public void onPartitionAdded(int partitionId, long firstLsn) {
        out.println("SINK partition-added pid=" + partitionId + " firstLsn=" + firstLsn);
    }

    @Override
    public void onPartitionRemoved(int partitionId, long lastLsn) {
        out.println("SINK partition-removed pid=" + partitionId + " lastLsn=" + lastLsn);
    }

    @Override
    public void onRecord(WalRecordView record) {
        // 只读视图：payload 在回调返回后即失效，这里立刻转成字符串
        String payload = copyPayload(record);
        out.println("SINK record pid=" + record.partitionId()
                + " lsn=" + record.lsn()
                + " len=" + record.payloadLength()
                + " payload=" + payload);
        lastLsnByPartition.put(record.partitionId(), record.lsn());
        records.increment();
        bytes.add(record.payloadLength());
    }

    @Override
    public void onTruncated(int partitionId, long fromLsn) {
        // 框架在恢复期发现尾部损坏、需要回退尾段时调用。
        // 复制实现应当据此通知下游做同样的回退，否则下游会比日志"多出"一段数据。
        truncated.increment();
        out.println("SINK truncated pid=" + partitionId + " fromLsn=" + fromLsn);
    }

    /**
     * 已"复制"到的最大 LSN。本示例是同步打印到本地流，写出去即视为已确认，
     * 因此直接返回最近的 LSN（{@code SEMI_SYNC} 模式下框架会据此放行 {@code flushAndWait}）。
     *
     * <p>真实复制实现这里应返回<b>副本侧确认</b>的位点，而不是本地写出的位点——
     * 返回偏大会让"已复制"变成谎言，主库故障切换时丢数据。
     *
     * @param partitionId 分区 ID
     * @return LSN；未知返回 0
     */
    @Override
    public long replicatedLsn(int partitionId) {
        Long v = lastLsnByPartition.get(partitionId);
        return v == null ? 0L : v;
    }

    /**
     * 把只读 payload 视图拷贝成可打印字符串（非可打印字节替换成 {@code .}）。
     *
     * @param record 记录视图
     * @return 字符串
     */
    private static String copyPayload(WalRecordView record) {
        var view = record.payload();
        if (view == null || view.remaining() <= 0) {
            return "";
        }
        byte[] copy = new byte[view.remaining()];
        view.duplicate().get(copy);
        StringBuilder sb = new StringBuilder(copy.length);
        for (byte b : copy) {
            if (b >= 0x20 && b < 0x7F) {
                sb.append((char) b);
            } else if (b == '\n') {
                sb.append("\\n");
            } else {
                sb.append('.');
            }
        }
        return sb.toString();
    }

    /**
     * 已转发记录数。
     *
     * @return 条数
     */
    public long recordCount() {
        return records.sum();
    }

    /**
     * 已转发载荷字节数。
     *
     * @return 字节数
     */
    public long payloadBytes() {
        return bytes.sum();
    }

    /**
     * 收到的截断通知次数。
     *
     * @return 次数
     */
    public long truncationCount() {
        return truncated.sum();
    }

    /**
     * 演示用的 payload 编码常量（保持与 README 一致）。
     *
     * @return 字符集
     */
    public static java.nio.charset.Charset payloadCharset() {
        return StandardCharsets.UTF_8;
    }

    @Override
    public String toString() {
        return "PrintlnReplicationSink{records=" + recordCount()
                + ", bytes=" + payloadBytes() + '}';
    }
}
