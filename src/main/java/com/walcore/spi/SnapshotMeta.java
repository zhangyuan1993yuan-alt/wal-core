package com.walcore.spi;

import java.nio.file.Path;

/**
 * 一次快照的元数据（不可变）。
 *
 * <p>快照 = "分区状态在某个 LSN 处的完整镜像"。有了它，恢复只需
 * "加载最近快照 + 重放之后的日志"，而不必从第一条日志重建状态。
 *
 * <h2>为什么必须带 {@code checksumHex}</h2>
 * 快照文件是<b>唯一无法靠 CRC 帧自证</b>的数据：它由上层业务序列化产生，格式不受框架约束。
 * 一旦文件被截断/位翻转而框架照常加载，得到的就是"状态看起来正常但内容错了"——
 * 比直接启动失败严重得多。因此写快照时计算 SHA-256，读时必校验，不匹配直接拒绝加载。
 */
public final class SnapshotMeta {

    private final int partitionId;
    private final long uptoLsn;
    private final long createdAtMillis;
    private final long stateBytes;
    private final String checksumHex;
    private final Path path;

    /**
     * 构造元数据。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 快照覆盖到的最后一条 LSN（含）
     * @param createdAtMillis 创建时刻（epoch 毫秒）
     * @param stateBytes 状态字节数
     * @param checksumHex 内容 SHA-256（小写 hex，64 字符）
     * @param path 快照文件路径；非文件实现可为 null
     */
    public SnapshotMeta(int partitionId, long uptoLsn, long createdAtMillis, long stateBytes,
                        String checksumHex, Path path) {
        this.partitionId = partitionId;
        this.uptoLsn = uptoLsn;
        this.createdAtMillis = createdAtMillis;
        this.stateBytes = stateBytes;
        this.checksumHex = checksumHex;
        this.path = path;
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
     * 快照覆盖到的最后一条 LSN（含）。恢复时从 {@code uptoLsn + 1} 继续重放。
     *
     * @return LSN
     */
    public long uptoLsn() {
        return uptoLsn;
    }

    /**
     * 创建时刻。
     *
     * @return epoch 毫秒
     */
    public long createdAtMillis() {
        return createdAtMillis;
    }

    /**
     * 状态字节数。
     *
     * @return 字节数
     */
    public long stateBytes() {
        return stateBytes;
    }

    /**
     * 内容 SHA-256（小写 hex）。
     *
     * @return 摘要
     */
    public String checksumHex() {
        return checksumHex;
    }

    /**
     * 快照文件路径（非文件实现返回 null）。
     *
     * @return 路径
     */
    public Path path() {
        return path;
    }

    @Override
    public String toString() {
        return "SnapshotMeta{p" + partitionId
                + ", uptoLsn=" + uptoLsn
                + ", bytes=" + stateBytes
                + ", sha256=" + (checksumHex == null ? "null" : checksumHex.substring(0, 8) + "…")
                + '}';
    }
}
