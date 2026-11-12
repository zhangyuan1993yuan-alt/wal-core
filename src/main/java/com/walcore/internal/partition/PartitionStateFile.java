package com.walcore.internal.partition;

import com.walcore.api.PartitionInfo;
import com.walcore.internal.segment.SegmentNaming;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 分区状态文件 {@code pXXXX.state}：把"分区已冻结/已归档"这件事落到磁盘上。
 *
 * <h2>为什么必须持久化</h2>
 * {@link com.walcore.api.RemoveMode#FREEZE} 的语义是"品种下线，但审计数据要留着"。
 * 如果状态只存在内存里，进程重启后恢复流程只看到"这个目录里有段文件"，会把它
 * 当成一个正常的 {@code ACTIVE} 分区并重新开放写入——上游以为已经下线的分区
 * 被悄悄写进新数据，这在"按分区做资金清算"的场景里是灾难性的。
 *
 * <h2>为什么是独立小文件而不是塞进 {@code wal.meta}</h2>
 * {@code wal.meta} 是<b>全库单点</b>：每次分区状态变化都要原子重写整个文件，
 * 而分区冻结通常伴随"同一时刻冻结几十个分区"的批量运维动作。独立文件让每次变更
 * 只影响一个几百字节的文件，而且单个文件损坏只丢一个分区的状态，不会让整个库
 * 打不开（{@code wal.meta} 损坏就走向"整目录扫描恢复"的重路径）。
 *
 * <h2>格式</h2>
 * 极简的一行 ASCII：{@code ACTIVE} / {@code FROZEN} / {@code ARCHIVED} / {@code PURGED}。
 * 只写不做校验和：内容只有一个枚举名，长度固定、取值有限，任何半写状态都会
 * 解析失败并回落到"未知状态"（调用方按 {@code ACTIVE} 处理），不会产生错乱状态。
 *
 * <p>写入仍是 {@code .tmp → ATOMIC_MOVE}，保证读者要么看到旧状态、要么看到新状态。
 */
public final class PartitionStateFile {

    private PartitionStateFile() {
    }

    /**
     * 写入分区状态（原子替换）。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @param state 状态
     * @throws IOException 写入失败
     */
    public static void write(Path baseDir, int partitionId, PartitionInfo.State state)
            throws IOException {
        Path target = SegmentNaming.statePath(baseDir, partitionId);
        Path dir = target.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".tmp");
        Files.write(tmp, (state.name() + System.lineSeparator()).getBytes(StandardCharsets.US_ASCII));
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * 读取分区状态。
     *
     * <p>文件不存在、内容为空、内容非法都返回 {@code null}（调用方按"未声明"处理）。
     * 这里刻意不抛异常：状态文件损坏不值得让整个 WAL 打不开，最坏情况退化成
     * "该分区按可写处理"，与没有这个文件时的行为一致。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @return 状态；未知返回 null
     */
    public static PartitionInfo.State read(Path baseDir, int partitionId) {
        Path target = SegmentNaming.statePath(baseDir, partitionId);
        if (!Files.isRegularFile(target)) {
            return null;
        }
        try {
            String text = new String(Files.readAllBytes(target), StandardCharsets.US_ASCII).trim();
            if (text.isEmpty()) {
                return null;
            }
            return PartitionInfo.State.valueOf(text);
        } catch (IOException e) {
            throw new UncheckedIOException("读取分区状态文件失败: " + target, e);
        } catch (IllegalArgumentException e) {
            // 枚举名不认识：可能是更新版本写下的新状态，按"未声明"处理更安全
            return null;
        }
    }

    /**
     * 删除分区状态文件（{@code PURGE} 之后调用；目录通常会一起被删掉）。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     */
    public static void delete(Path baseDir, int partitionId) {
        try {
            Files.deleteIfExists(SegmentNaming.statePath(baseDir, partitionId));
        } catch (IOException ignored) {
            // 删除失败不影响正确性：目录清理会在 SegmentManager 里再做一次
        }
    }
}
