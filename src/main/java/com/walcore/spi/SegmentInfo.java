package com.walcore.spi;

import java.nio.file.Path;

/**
 * 一个段的描述信息（保留策略的输入 / 删除回调的参数）。
 *
 * <h2>关于 {@code sizeBytes} 的口径（重要）</h2>
 * 段文件是<b>预分配</b>的（默认 {@code PreallocateMode.SPARSE}），所以
 * {@code physicalLength()} 恒等于段容量（256MB），与"实际写了多少"无关。
 * 本类给出的是<b>逻辑占用</b>：
 * <ul>
 *   <li>已封存（{@code READONLY}/{@code FROZEN}）的段：{@code finalLength}，即最后一帧的末尾偏移；</li>
 *   <li>活跃段：{@code publishedWritePos()}，即当前写入位置。</li>
 * </ul>
 * 这样"按总大小做保留"才是符合直觉的：写了 1KB 的段算 1KB，而不是 256MB。
 * 磁盘实际占用请看 {@code wal.stats().totalLogBytes()} 的口径说明。
 */
public final class SegmentInfo {

    private final int partitionId;
    private final long firstLsn;
    private final long lastLsn;
    private final long sizeBytes;
    private final long createdMillis;
    private final Path path;

    /**
     * 构造段信息。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录 LSN（等于文件名里的名字 LSN）
     * @param lastLsn 段内末条记录 LSN；空段为 0
     * @param sizeBytes 逻辑占用字节数
     * @param createdMillis 创建时刻（epoch 毫秒）
     * @param path 段文件路径
     */
    public SegmentInfo(int partitionId, long firstLsn, long lastLsn, long sizeBytes,
                       long createdMillis, Path path) {
        this.partitionId = partitionId;
        this.firstLsn = firstLsn;
        this.lastLsn = lastLsn;
        this.sizeBytes = sizeBytes;
        this.createdMillis = createdMillis;
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
     * 段内首条记录 LSN。
     *
     * @return LSN
     */
    public long firstLsn() {
        return firstLsn;
    }

    /**
     * 段内末条记录 LSN。
     *
     * @return LSN；空段返回 0
     */
    public long lastLsn() {
        return lastLsn;
    }

    /**
     * 逻辑占用字节数（见类注释的口径说明）。
     *
     * @return 字节数
     */
    public long sizeBytes() {
        return sizeBytes;
    }

    /**
     * 创建时刻。
     *
     * @return epoch 毫秒
     */
    public long createdMillis() {
        return createdMillis;
    }

    /**
     * 段文件路径。
     *
     * @return 路径
     */
    public Path path() {
        return path;
    }

    @Override
    public String toString() {
        return "SegmentInfo{p" + partitionId
                + ", firstLsn=" + firstLsn
                + ", lastLsn=" + lastLsn
                + ", size=" + sizeBytes
                + ", created=" + createdMillis + '}';
    }
}
