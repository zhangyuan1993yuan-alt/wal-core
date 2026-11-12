package com.walcore.spi;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * ⑥ 快照契约：把"分区状态"按 LSN 落盘，用于把恢复的重放量从"全部日志"降到"快照之后的日志"。
 *
 * <h2>为什么它不算"第五个扩展点"</h2>
 * ARCH §10.2 决策 #5：快照的<b>格式</b>由上层业务决定（Protobuf / JSON / 自定义二进制），
 * 框架只定义"怎么存、怎么取、怎么校验"这套契约。因此它虽然也是 SPI（可替换实现，
 * 例如放到 S3），但语义上是"框架的存储契约"而不是"业务扩展点"。
 *
 * <h2>与保留策略的联动（关键）</h2>
 * {@link #pinnedLsn(int)} 返回最近快照的 {@code uptoLsn}。{@code RetentionManager}
 * 会把它当作不可删除边界：<b>删掉快照点之前的段没关系</b>（状态已经在快照里），
 * 但<b>删掉快照点之后的段会导致数据永久丢失</b>（既不在快照里，也不在日志里）。
 */
public interface SnapshotStore extends WalPlugin {

    /**
     * 写入一份快照。
     *
     * <p>实现必须保证原子性：中断后不得留下"半截快照"（否则下次 {@link #latest} 会看到它）。
     * 推荐"临时文件 + ATOMIC_MOVE"。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 快照覆盖到的最后一条 LSN（含）
     * @param state 状态内容流（框架保证读完即关闭）
     * @param stateBytes 内容字节数
     * @return 写入的元数据（含 SHA-256）
     */
    SnapshotMeta write(int partitionId, long uptoLsn, InputStream state, long stateBytes);

    /**
     * 最近一次快照。
     *
     * @param partitionId 分区 ID
     * @return 元数据；无快照返回 {@link Optional#empty()}
     */
    Optional<SnapshotMeta> latest(int partitionId);

    /**
     * 列出一个分区的全部快照，按 {@code uptoLsn} 升序。
     *
     * @param partitionId 分区 ID
     * @return 不可变列表
     */
    List<SnapshotMeta> list(int partitionId);

    /**
     * 打开快照内容流（实现<b>必须</b>先校验 SHA-256，不匹配抛异常）。
     *
     * @param meta 元数据
     * @return 内容流（调用方负责关闭）
     */
    InputStream open(SnapshotMeta meta);

    /**
     * 删除 {@code uptoLsn} 之前的快照（保留该 LSN 及更新的）。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 边界
     */
    void deleteBefore(int partitionId, long uptoLsn);

    /**
     * 可安全删除的最小 LSN = 最近快照的 {@code uptoLsn}。
     *
     * @param partitionId 分区 ID
     * @return LSN；无快照返回 0
     */
    long pinnedLsn(int partitionId);
}
