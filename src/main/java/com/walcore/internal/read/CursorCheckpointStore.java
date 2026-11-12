package com.walcore.internal.read;

import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.Crc32cUtil;
import com.walcore.internal.util.MetaFile;
import com.walcore.internal.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 游标位置持久化（ARCH §8.1 的"断点续读"，P0-06 ② 不丢不重）。
 *
 * <p>文件内容形如：
 * <pre>
 * lsn=1048576
 * crc32c=9F3C21AB
 * </pre>
 * 末行是对前面正文（{@code "lsn=1048576\n"}）的 CRC32C。<b>为什么要 CRC</b>：
 * 断电可能留下半截文件，只读出一个 {@code lsn=10485} 这种截断值比没有 checkpoint
 * 危险得多——它会静默地把游标定位到错误位置。CRC 让"半截文件"变成"没有 checkpoint"。
 *
 * <p>写入用"临时文件 + {@code ATOMIC_MOVE}"，因此任何时刻磁盘上的 {@code .cursor}
 * 要么是旧的完整值、要么是新的完整值，不存在中间态。
 */
public final class CursorCheckpointStore {

    /** 正文键名。 */
    public static final String KEY_LSN = "lsn";

    private final Path baseDir;

    /**
     * 构造 checkpoint 存储。
     *
     * @param baseDir 日志根目录
     */
    public CursorCheckpointStore(Path baseDir) {
        this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
    }

    /**
     * 持久化游标位置（原子替换）。
     *
     * @param partitionId 分区 ID
     * @param lsn 下一次要读的 LSN
     * @throws IOException 写入失败
     */
    public void persist(int partitionId, long lsn) throws IOException {
        Preconditions.checkPositive(partitionId + 1L, "partitionId");
        Preconditions.checkArgument(lsn >= 0L, "lsn 不得为负: %s", lsn);
        Path target = SegmentNaming.cursorPath(baseDir, partitionId);
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String body = KEY_LSN + '=' + lsn + '\n';
        int crc = Crc32cUtil.computeRange(body.getBytes(StandardCharsets.UTF_8), 0,
                body.getBytes(StandardCharsets.UTF_8).length);
        String text = body + MetaFile.CRC_KEY + '=' + Integer.toHexString(crc).toUpperCase() + '\n';

        Path tmp = target.resolveSibling(target.getFileName() + SegmentNaming.TMP_EXT);
        Files.writeString(tmp, text, StandardCharsets.UTF_8);
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * 读取游标位置。
     *
     * @param partitionId 分区 ID
     * @return 上次持久化的 LSN；文件不存在、CRC 不符、内容截断时返回 0（视为"没有 checkpoint"）
     */
    public long restore(int partitionId) {
        Path target = SegmentNaming.cursorPath(baseDir, partitionId);
        if (!Files.isRegularFile(target)) {
            return 0L;
        }
        try {
            String text = Files.readString(target, StandardCharsets.UTF_8);
            int crcLineStart = text.lastIndexOf(MetaFile.CRC_PREFIX);
            if (crcLineStart < 0) {
                return 0L;
            }
            String body = text.substring(0, crcLineStart);
            String crcLine = text.substring(crcLineStart).trim();
            int eq = crcLine.indexOf('=');
            if (eq < 0) {
                return 0L;
            }
            int expected;
            try {
                expected = (int) Long.parseLong(crcLine.substring(eq + 1).trim(), 16);
            } catch (NumberFormatException e) {
                return 0L;
            }
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            int actual = Crc32cUtil.computeRange(bodyBytes, 0, bodyBytes.length);
            if (expected != actual) {
                return 0L;
            }
            for (String line : body.split("\n")) {
                int separator = line.indexOf('=');
                if (separator > 0 && KEY_LSN.equals(line.substring(0, separator).trim())) {
                    return Long.parseLong(line.substring(separator + 1).trim());
                }
            }
            return 0L;
        } catch (IOException | NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 删除某个分区的 checkpoint（分区被 PURGE 时调用）。
     *
     * @param partitionId 分区 ID
     * @throws IOException 删除失败
     */
    public void delete(int partitionId) throws IOException {
        Files.deleteIfExists(SegmentNaming.cursorPath(baseDir, partitionId));
    }

    /**
     * 读取全部 checkpoint（诊断用）。
     *
     * @param partitionIds 需要查询的分区 ID
     * @return 分区 ID -&gt; LSN（只含有效项）
     */
    public Map<Integer, Long> restoreAll(int[] partitionIds) {
        Map<Integer, Long> out = new LinkedHashMap<>();
        for (int pid : partitionIds) {
            long lsn = restore(pid);
            if (lsn > 0L) {
                out.put(pid, lsn);
            }
        }
        return out;
    }

    @Override
    public String toString() {
        return "CursorCheckpointStore{" + baseDir + '}';
    }
}
