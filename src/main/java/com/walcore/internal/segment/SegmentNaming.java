package com.walcore.internal.segment;

import com.walcore.internal.util.Preconditions;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;

/**
 * 段文件命名与目录布局规则（唯一真源，对应 ARCH §6.1）。
 *
 * <h2>命名格式</h2>
 * <pre>
 * wal-{partitionId:%04d}-{firstLsn:%020d}.wal
 * wal-{partitionId:%04d}-{firstLsn:%020d}.idx
 * wal-{partitionId:%04d}-{firstLsn:%020d}.wal.tmp   （预建中，完成后 ATOMIC_MOVE 去掉 .tmp）
 * </pre>
 *
 * <h2>为什么要 {@code %020d}</h2>
 * 固定 20 位（{@code long} 最大 19 位数字）让 {@code firstLsn} 的<b>字典序 == 数值序</b>，
 * 无需符号位、永不需要负号。这样：
 * <ul>
 *   <li>{@code Collections.sort} 直接按字符串比较即可，不用解析数字（快且不易错）；</li>
 *   <li>满足 PRD WAL-P0-02 ② "文件名含起始 seq/编号，字典序即时间序"；</li>
 *   <li>{@code ls} 的输出天然就是时间序，运维排查非常直观。</li>
 * </ul>
 *
 * <p>注意一个容易忽略的性质：预建段的文件名用的是"预建时刻的 {@code LSN} 计数器值"，
 * 它 ≤ 该段实际首条记录的 LSN。这不影响正确性——因为 {@code firstLsn} 的字典序仍严格递增，
 * 且 {@code findSegmentFor(lsn)} 只需要"名字里的 LSN ≤ 段内所有真实 LSN"这一性质。
 * 真实首条 LSN 在打开段时由扫描第一帧得出。
 */
public final class SegmentNaming {

    /** 分区目录名。 */
    public static final String PARTITIONS_DIR = "partitions";

    /** 全局元数据文件名。 */
    public static final String META_FILE = "wal.meta";

    /** 独占锁文件名。 */
    public static final String LOCK_FILE = "LOCK";

    /** 外部插件 JAR 目录名。 */
    public static final String PLUGIN_DIR = "plugin";

    /** 快照目录名。 */
    public static final String SNAPSHOTS_DIR = "snapshots";

    /** 段数据文件扩展名。 */
    public static final String WAL_EXT = ".wal";

    /** 索引文件扩展名。 */
    public static final String IDX_EXT = ".idx";

    /** 预建中的临时扩展名。 */
    public static final String TMP_EXT = ".tmp";

    /** 游标文件名后缀（{@code p0000.cursor}）。 */
    public static final String CURSOR_EXT = ".cursor";

    /** 分区状态文件名后缀（{@code p0000.state}）。 */
    public static final String STATE_EXT = ".state";

    /** 设计支持的最大分区数（ID 范围 {@code 0..4095}）。 */
    public static final int MAX_PARTITION_ID = 4095;

    /** 分区 ID 的字符宽度。 */
    private static final int PID_WIDTH = 4;

    /** LSN 的字符宽度（long 最大 19 位，取 20 位保证永不溢出、永无负号）。 */
    private static final int LSN_WIDTH = 20;

    private SegmentNaming() {
    }

    // ------------------------------------------------------------------
    // 目录布局
    // ------------------------------------------------------------------

    /**
     * 分区目录：{@code <baseDir>/partitions/p{pid:%04d}}。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @return 分区目录
     */
    public static Path partitionDir(Path baseDir, int partitionId) {
        Preconditions.checkNotNull(baseDir, "baseDir");
        Preconditions.checkArgument(partitionId >= 0 && partitionId <= MAX_PARTITION_ID,
                "partitionId 越界: %s", partitionId);
        return baseDir.resolve(PARTITIONS_DIR).resolve(partitionName(partitionId));
    }

    /**
     * 分区目录名：{@code p%04d}。
     *
     * @param partitionId 分区 ID
     * @return 目录名
     */
    public static String partitionName(int partitionId) {
        return String.format(Locale.ROOT, "p%0" + PID_WIDTH + "d", partitionId);
    }

    // ------------------------------------------------------------------
    // 文件名
    // ------------------------------------------------------------------

    /**
     * 段文件名：{@code wal-p0003-00000000000000131072.wal}。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 文件名
     */
    public static String walFileName(int partitionId, long firstLsn) {
        return baseName(partitionId, firstLsn) + WAL_EXT;
    }

    /**
     * 索引文件名。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 文件名
     */
    public static String idxFileName(int partitionId, long firstLsn) {
        return baseName(partitionId, firstLsn) + IDX_EXT;
    }

    /**
     * 预建临时文件名：{@code wal-p0003-...wal.tmp}。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 文件名
     */
    public static String tmpFileName(int partitionId, long firstLsn) {
        return baseName(partitionId, firstLsn) + WAL_EXT + TMP_EXT;
    }

    /**
     * 段文件完整路径。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 路径
     */
    public static Path walPath(Path baseDir, int partitionId, long firstLsn) {
        return partitionDir(baseDir, partitionId).resolve(walFileName(partitionId, firstLsn));
    }

    /**
     * 索引文件完整路径。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 路径
     */
    public static Path idxPath(Path baseDir, int partitionId, long firstLsn) {
        return partitionDir(baseDir, partitionId).resolve(idxFileName(partitionId, firstLsn));
    }

    /**
     * 预建临时文件完整路径。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @return 路径
     */
    public static Path tmpPath(Path baseDir, int partitionId, long firstLsn) {
        return partitionDir(baseDir, partitionId).resolve(tmpFileName(partitionId, firstLsn));
    }

    /**
     * 游标文件完整路径。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @return 路径
     */
    public static Path cursorPath(Path baseDir, int partitionId) {
        return partitionDir(baseDir, partitionId).resolve(partitionName(partitionId) + CURSOR_EXT);
    }

    /**
     * 分区状态文件完整路径。
     *
     * @param baseDir 日志根目录
     * @param partitionId 分区 ID
     * @return 路径
     */
    public static Path statePath(Path baseDir, int partitionId) {
        return partitionDir(baseDir, partitionId).resolve(partitionName(partitionId) + STATE_EXT);
    }

    // ------------------------------------------------------------------
    // 解析
    // ------------------------------------------------------------------

    /**
     * 解析结果。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN
     * @param extension 扩展名（{@code ".wal"} / {@code ".idx"} / {@code ".wal.tmp"}）
     */
    public record Parsed(int partitionId, long firstLsn, String extension) {
    }

    /**
     * 解析段文件名。
     *
     * <p>非法形态一律返回 {@link Optional#empty()}（不抛异常，便于"扫描目录时跳过无关文件"）：
     * 负号出现在 LSN 位、位数不是 4/20、分区 ID 越界、LSN 为 0、扩展名不认识。
     *
     * @param path 文件路径（只看文件名）
     * @return 解析结果
     */
    public static Optional<Parsed> parse(Path path) {
        if (path == null || path.getFileName() == null) {
            return Optional.empty();
        }
        return parseName(path.getFileName().toString());
    }

    /**
     * 解析段文件名（字符串形态）。
     *
     * @param fileName 文件名
     * @return 解析结果
     */
    public static Optional<Parsed> parseName(String fileName) {
        if (fileName == null) {
            return Optional.empty();
        }
        String extension;
        String body;
        if (fileName.endsWith(WAL_EXT + TMP_EXT)) {
            extension = WAL_EXT + TMP_EXT;
            body = fileName.substring(0, fileName.length() - extension.length());
        } else if (fileName.endsWith(WAL_EXT)) {
            extension = WAL_EXT;
            body = fileName.substring(0, fileName.length() - extension.length());
        } else if (fileName.endsWith(IDX_EXT)) {
            extension = IDX_EXT;
            body = fileName.substring(0, fileName.length() - extension.length());
        } else {
            return Optional.empty();
        }
        // body 形如 wal-p0003-00000000000000131072
        String prefix = "wal-p";
        if (!body.startsWith(prefix)) {
            return Optional.empty();
        }
        String rest = body.substring(prefix.length());
        int dash = rest.indexOf('-');
        if (dash < 0) {
            return Optional.empty();
        }
        String pidPart = rest.substring(0, dash);
        String lsnPart = rest.substring(dash + 1);
        if (pidPart.length() != PID_WIDTH || lsnPart.length() != LSN_WIDTH) {
            return Optional.empty();
        }
        if (!isAllDigits(pidPart) || !isAllDigits(lsnPart)) {
            return Optional.empty();
        }
        int partitionId;
        long firstLsn;
        try {
            partitionId = Integer.parseInt(pidPart);
            firstLsn = Long.parseLong(lsnPart);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        if (partitionId < 0 || partitionId > MAX_PARTITION_ID) {
            return Optional.empty();
        }
        if (firstLsn <= 0L) {
            return Optional.empty();
        }
        return Optional.of(new Parsed(partitionId, firstLsn, extension));
    }

    /**
     * 从文件名解析 {@code firstLsn}。
     *
     * @param fileName 文件名
     * @return LSN
     */
    public static Optional<Long> parseFirstLsn(String fileName) {
        return parseName(fileName).map(Parsed::firstLsn);
    }

    /**
     * 判断是否为段数据文件（含预建中的 {@code .tmp}）。
     *
     * @param path 路径
     * @return 是则返回 true
     */
    public static boolean isWalFile(Path path) {
        return parse(path).map(p -> WAL_EXT.equals(p.extension()) || (WAL_EXT + TMP_EXT).equals(p.extension()))
                .orElse(false);
    }

    /**
     * 判断是否为索引文件。
     *
     * @param path 路径
     * @return 是则返回 true
     */
    public static boolean isIdxFile(Path path) {
        return parse(path).map(p -> IDX_EXT.equals(p.extension())).orElse(false);
    }

    /**
     * 判断是否为"已完成"的段数据文件（不含 {@code .tmp}）。
     *
     * @param path 路径
     * @return 是则返回 true
     */
    public static boolean isSealedWalFile(Path path) {
        return parse(path).map(p -> WAL_EXT.equals(p.extension())).orElse(false);
    }

    // ------------------------------------------------------------------
    // 内部工具
    // ------------------------------------------------------------------

    /**
     * 生成 {@code wal-p%04d-%020d} 形式的基名。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 首条记录 LSN
     * @return 基名
     */
    private static String baseName(int partitionId, long firstLsn) {
        Preconditions.checkArgument(partitionId >= 0 && partitionId <= MAX_PARTITION_ID,
                "partitionId 越界: %s", partitionId);
        Preconditions.checkArgument(firstLsn > 0L, "firstLsn 必须为正数: %s", firstLsn);
        String pidPart = String.format(Locale.ROOT, "%0" + PID_WIDTH + "d", partitionId);
        String lsnPart = String.format(Locale.ROOT, "%0" + LSN_WIDTH + "d", firstLsn);
        return "wal-p" + pidPart + "-" + lsnPart;
    }

    /**
     * 是否全为 ASCII 数字。
     *
     * @param s 字符串
     * @return 全为数字返回 true
     */
    private static boolean isAllDigits(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return !s.isEmpty();
    }
}
