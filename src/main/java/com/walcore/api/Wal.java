package com.walcore.api;

import com.walcore.internal.plugin.PluginManager;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

/**
 * WAL 框架对外的<b>唯一门面接口</b>。
 *
 * <p>生命周期契约：
 * <ol>
 *   <li>{@code WalConfig.builder()...build()} 打开实例，内部完成
 *       {@code LOCK} 独占 → 读 {@code wal.meta} → 扫描最后一个段 → 撕裂写截断 → 索引校验/重建
 *       → 计算 {@code nextLsn} → 启动后台线程；</li>
 *   <li>写入：{@link #append(int, ByteBuffer, AppendOptions)} 纯内存返回 LSN；
 *       需要持久确认时再调用 {@link #flushAndWait(int, long, long)}（撮合引擎在回 ACK 前调用）；</li>
 *   <li>{@link #close()} 两阶段优雅关闭，保证"close 后立即 kill -9 也不丢已确认数据"。</li>
 * </ol>
 *
 * <h2>能力清单（全部已交付）</h2>
 * <ul>
 *   <li><b>写入</b>：{@code append} / {@code appendSync} / {@code appendBatch}（T08）；</li>
 *   <li><b>读取</b>：{@link WalCursor} 顺序读 + 断点续读（T09）、{@link WalTailer} 实时订阅（T13）；</li>
 *   <li><b>持久化</b>：组提交三重攒批 + {@code FsyncPolicy} 三档（T07）、{@code flushAndWait} 持久确认；</li>
 *   <li><b>可靠性</b>：崩溃恢复 / 撕裂写检测 / 索引重建 / 重放回调（T10）、两阶段优雅关闭（T12）；</li>
 *   <li><b>运维</b>：保留策略（T14）、快照与 checkpoint（T15）、动态增删分区（T16）、
 *       指标埋点与外接 {@code MeterSink}（T17）；</li>
 *   <li><b>扩展</b>：五类 SPI（codec / storage / index / retention / replication）+ 插件热切换（T11）。</li>
 * </ul>
 *
 * <p><b>不是本接口能力</b>的两个常见诉求，特意不做：
 * <ul>
 *   <li><b>框架内幂等去重</b>：框架只把幂等键摘要写进帧头并原样回吐
 *       （{@link WalRecord#attributes()} + {@link WalRecord#hasIdempotentKey()}），
 *       "什么算同一条业务记录"由使用方判定——1 万个 key 的 {@code HashSet} 会破坏
 *       append 零分配，且去重是业务语义（TASKS §3 WAL-P1-06 明确了这条边界）。</li>
 *   <li><b>在线全量校验 {@code verify}</b>：等价能力是"停机 + 重开走崩溃恢复路径"，
 *       会完整校验每一帧 CRC 并产出 {@code RecoveryReport}。为了不给出一个语义模糊的
 *       在线接口，本版本不暴露 {@code verify()}。</li>
 * </ul>
 */
public interface Wal extends AutoCloseable {

    // ==================================================================
    // 配置与生命周期
    // ==================================================================

    /**
     * 本次实例的不可变配置。
     *
     * @return 配置
     */
    WalConfig config();

    /**
     * 优雅关闭（默认超时 30s）。
     *
     * <p>顺序：拒新写 → 排空组提交 → 每活跃段 {@code force(true)} → 写
     * {@code wal.meta(cleanShutdown=true)} → 释放 {@code LOCK}。幂等。
     */
    @Override
    void close();

    /**
     * 带超时的优雅关闭。
     *
     * @param timeout 最长等待时长
     * @return 关闭报告；超时返回 {@code clean=false} 但<b>不抛异常</b>
     */
    CloseReport close(Duration timeout);

    /**
     * 实例是否已关闭。
     *
     * @return 已关闭返回 true
     */
    boolean isClosed();

    // ==================================================================
    // 写入
    // ==================================================================

    /**
     * 追加一条记录（纯内存路径，不 fsync）。
     *
     * <p>{@code payload} 的 {@code [position, limit)} 区间会被完整复制进段文件；
     * 方法返回后调用方可以复用该缓冲区。
     *
     * @param partitionId 分区 ID
     * @param payload 业务字节
     * @param options 可选参数，不得为 null（用 {@link AppendOptions#DEFAULTS}）
     * @return 全局唯一、严格递增的 LSN
     */
    long append(int partitionId, ByteBuffer payload, AppendOptions options);

    /**
     * 追加一条记录，使用默认选项。
     *
     * @param partitionId 分区 ID
     * @param payload 业务字节
     * @return LSN
     */
    default long append(int partitionId, ByteBuffer payload) {
        return append(partitionId, payload, AppendOptions.DEFAULTS);
    }

    /**
     * 追加并等待持久化（{@code append + flushAndWait} 的便捷组合）。
     *
     * @param partitionId 分区 ID
     * @param payload 业务字节
     * @param options 可选参数
     * @return LSN（返回时已保证 {@code durableLsn >= lsn}）
     */
    long appendSync(int partitionId, ByteBuffer payload, AppendOptions options);

    /**
     * 批量追加（同一分区、同一批次），返回与入参一一对应的 LSN 数组。
     *
     * <p>数组元素严格连续（{@code result[i+1] == result[i] + 1}），这是"无空洞"不变量的直接体现。
     *
     * @param partitionId 分区 ID
     * @param payloads 业务字节列表
     * @return LSN 数组
     */
    long[] appendBatch(int partitionId, List<ByteBuffer> payloads);

    // ==================================================================
    // 持久化水位
    // ==================================================================

    /**
     * 阻塞直到指定 LSN 已持久化。
     *
     * <p>快路径：{@code durableLsn >= lsn} 时立即返回，<b>零系统调用、零 park</b>。
     *
     * @param partitionId 分区 ID
     * @param lsn 目标 LSN
     * @param timeoutMillis 超时
     * @return 已持久化返回 true；超时返回 false
     */
    boolean flushAndWait(int partitionId, long lsn, long timeoutMillis);

    /**
     * 阻塞直到指定 LSN 已持久化，使用 {@code WalConfig#flushAndWaitTimeoutMillis()}。
     *
     * @param partitionId 分区 ID
     * @param lsn 目标 LSN
     * @return 已持久化返回 true
     */
    default boolean flushAndWait(int partitionId, long lsn) {
        return flushAndWait(partitionId, lsn, config().flushAndWaitTimeoutMillis());
    }

    /**
     * 分区已持久化的最大 LSN。
     *
     * @param partitionId 分区 ID
     * @return LSN，未写入过返回 0
     */
    long durableLsn(int partitionId);

    /**
     * 分区已发布（完整写入段文件但未 guaranteed 持久化）的最大 LSN。
     *
     * @param partitionId 分区 ID
     * @return LSN，未写入过返回 0
     */
    long publishedLsn(int partitionId);

    // ==================================================================
    // 读取
    // ==================================================================

    /**
     * 打开一个顺序读游标。
     *
     * @param partitionId 分区 ID
     * @param options 选项，不得为 null
     * @return 游标（使用方负责 close）
     */
    WalCursor cursor(int partitionId, CursorOptions options);

    /**
     * 打开一个顺序读游标，使用默认选项（从分区第一条记录开始）。
     *
     * @param partitionId 分区 ID
     * @return 游标
     */
    default WalCursor cursor(int partitionId) {
        return cursor(partitionId, CursorOptions.DEFAULTS);
    }

    /**
     * 打开一个实时订阅（先追历史、追平后转实时）。
     *
     * <p>订阅在<b>独立线程</b>上消费，因此回调再慢也不会拖慢 {@code append()}。
     * 交付语义见 {@link WalTailer}：追平切换点无空隙、无重复。
     *
     * @param options 选项（含回调），不得为 null
     * @return 订阅句柄（使用方负责 close）
     */
    WalTailer tailer(TailerOptions options);

    /**
     * 完整走一遍恢复流程并把每条记录交给 {@code listener}（用于"重建业务状态"）。
     *
     * <p><b>注意</b>：这是一个<b>额外的</b>重放，与 {@code open()} 时已经做过的
     * 完整性恢复（扫描/截断/索引）不同；后者不会回调用户代码。本方法把分区数据
     * 按 LSN 全局升序重读一遍，不改动任何文件。
     *
     * @param listener 回调，可为 null（等价于只做一次完整性校验）
     * @return 恢复报告
     */
    RecoveryReport recover(ReplayListener listener);

    // ==================================================================
    // 分区管理
    // ==================================================================

    /**
     * 动态新增分区，自动分配 ID。
     *
     * @param options 选项
     * @return 分配到的分区 ID
     */
    int addPartition(PartitionOptions options);

    /**
     * 动态新增分区，指定 ID。
     *
     * @param partitionId 分区 ID
     * @param options 选项
     */
    void addPartition(int partitionId, PartitionOptions options);

    /**
     * 停用 / 归档 / 清除分区。
     *
     * @param partitionId 分区 ID
     * @param mode 移除模式
     */
    void removePartition(int partitionId, RemoveMode mode);

    /**
     * 手动轮转活跃段（运维/测试用）。
     *
     * @param partitionId 分区 ID
     */
    void rotate(int partitionId);

    /**
     * 分区元信息快照。
     *
     * @param partitionId 分区 ID
     * @return 信息；分区不存在返回 null
     */
    PartitionInfo partitionInfo(int partitionId);

    // ==================================================================
    // 观测与扩展
    // ==================================================================

    /**
     * 快照存储（写入/读取分区状态镜像，用于缩短恢复时的重放量）。
     *
     * @return 快照存储；实例尚未初始化完成时可能为 null
     */
    com.walcore.spi.SnapshotStore snapshotStore();

    /**
     * 运行期统计快照。
     *
     * @return 快照
     */
    WalStats stats();

    /**
     * 插件管理器（发现 / 仲裁 / 热切换 / 诊断）。
     *
     * @return 插件管理器
     */
    PluginManager pluginManager();

    /**
     * 当前生效的编解码器插件 ID。
     *
     * @return 插件 ID
     */
    String codecId();
}
