package com.walcore.segment;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>T05 空段泄漏的回归测试（D3 根因修复）</b>。
 *
 * <h2>被钉住的不变量</h2>
 * <ol>
 *   <li><b>目录里的 {@code .wal} 文件数 == 逻辑段数</b>（预建储备被停掉之后）。
 *       这条不变量一旦破了，磁盘会被空段慢慢吃掉，而且恢复流程每次启动都要多扫一遍。</li>
 *   <li>不允许残留 {@code .wal.tmp}（原子改名的半成品）。</li>
 *   <li>高频轮转下每条写入的记录依然可读，LSN 严格递增（泄漏修复不能以正确性为代价）。</li>
 * </ol>
 *
 * <h2>为什么用"连续 1000 次轮转"来触发</h2>
 * 泄漏的触发条件是"预建线程还在建段时写入线程又要轮转"，也就是<b>轮转速度快于建段速度</b>。
 * 段大小取 4096B（远小于生产 256MB），建段本身只有几十微秒，连续轮转就能稳定命中竞态。
 * 修复前的实测表现是：每次轮转泄漏一个空 {@code .wal}，1000 次轮转后目录里约 2000 个文件，
 * 而逻辑段数只有 1001。
 */
@DisplayName("段轮转不泄漏空段（T05 回归）")
class SegmentRotationLeakTest {

    /** 轮转次数：足够让"预建未完成又要轮转"的竞态反复发生。 */
    private static final int ROTATIONS = 1000;

    @Test
    @DisplayName("连续 1000 次轮转后：.wal 文件数 == 逻辑段数，且无 .tmp 残留")
    void rotationDoesNotLeakEmptySegments(@TempDir Path dir) throws IOException {
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            int pid = 0;
            for (int i = 0; i < ROTATIONS; i++) {
                wal.rotate(pid);
                if ((i & 0x7F) == 0) {
                    // 轮转过程中也不允许出现"文件数远超逻辑段数"（预建储备最多 1 个）
                    assertNoLeakDuringRotation(dir, wal.segmentManager().segmentCount(pid));
                }
            }

            // 停掉预建线程：它手里的"已备好但未被采用"的那一份会被自己删掉，
            // 因此此后"文件数 == 逻辑段数"必须严格成立。
            wal.segmentManager().preparer().stop();

            int logical = wal.segmentManager().segmentCount(pid);
            assertEquals(ROTATIONS + 1, logical,
                    "首段 1 个 + 轮转 " + ROTATIONS + " 次 = " + (ROTATIONS + 1) + " 个逻辑段");
            assertEquals(logical, countWalFiles(dir, pid),
                    "目录里的 .wal 文件数必须等于逻辑段数（多出来的就是泄漏的空段）");
            assertEquals(logical, countIdxFiles(dir, pid),
                    ".idx 文件数也必须与段数一致（泄漏的段连带泄漏索引文件）");
            assertEquals(0L, countTmpFiles(dir, pid), "不得残留 .wal.tmp");
        }
    }

    @Test
    @DisplayName("带写入的高频轮转：记录不丢、LSN 严格递增、文件数与段数一致")
    void rotationUnderWritesKeepsDataAndFileCount(@TempDir Path dir) throws IOException {
        int pid = 0;
        int perSegment = 20;
        long lastLsn = 0L;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < perSegment; j++) {
                    long lsn = wal.append(pid, TestWal.payload(16), AppendOptions.DEFAULTS);
                    assertEquals(lastLsn + 1L, lsn, "LSN 必须严格递增且无空洞");
                    lastLsn = lsn;
                }
                wal.rotate(pid);
            }
            assertTrue(wal.flushAndWait(pid, lastLsn, 10_000L), "必须可刷盘");

            wal.segmentManager().preparer().stop();
            int logical = wal.segmentManager().segmentCount(pid);
            assertEquals(logical, countWalFiles(dir, pid),
                    "带写入时同样不允许泄漏空段");
            assertEquals(200 * perSegment, TestWal.readAllLsns(wal, pid).size(),
                    "轮转期间写入的记录一条都不能少");
        }
    }

    @Test
    @DisplayName("数据目录里不会出现名字重复的段（名字 LSN 严格递增）")
    void segmentNamesAreStrictlyIncreasing(@TempDir Path dir) throws IOException {
        int pid = 0;
        try (DefaultWal wal = TestWal.open(dir, 4096L, b -> {
        })) {
            for (int i = 0; i < 300; i++) {
                wal.append(pid, TestWal.payload(16), AppendOptions.DEFAULTS);
                wal.rotate(pid);
            }
            wal.segmentManager().preparer().stop();

            List<String> names = walFiles(dir, pid);
            assertEquals(wal.segmentManager().segmentCount(pid), names.size(),
                    "文件数必须等于段数");
            long prev = Long.MIN_VALUE;
            for (String name : names) {
                long parsed = parseLsn(name);
                assertTrue(parsed > prev, "段名 LSN 必须严格递增: " + names);
                prev = parsed;
            }
            assertFalse(names.isEmpty(), "必须至少有一个段");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 轮转进行中的宽松断言：预建储备最多只允许存在 1 个，因此文件数不得超过
     * "逻辑段数 + 1"。修复前这里会先崩，因为每个分区会同时看到数十个孤儿文件。
     *
     * @param dir 根目录
     * @param logical 逻辑段数
     * @throws IOException 列目录失败
     */
    private static void assertNoLeakDuringRotation(Path dir, int logical) throws IOException {
        int files = countWalFiles(dir, 0);
        assertTrue(files <= logical + 1,
                "轮转进行中出现泄漏：.wal 文件数 = " + files + "，逻辑段数 = " + logical);
    }

    /**
     * 统计分区目录里的 {@code .wal} 文件数。
     *
     * @param dir 根目录
     * @param pid 分区 ID
     * @return 数量
     * @throws IOException 列目录失败
     */
    private static int countWalFiles(Path dir, int pid) throws IOException {
        return walFiles(dir, pid).size();
    }

    /**
     * 列出分区目录里的 {@code .wal} 文件名（按名字排序，段名定长所以字典序即数值序）。
     *
     * @param dir 根目录
     * @param pid 分区 ID
     * @return 文件名列表
     * @throws IOException 列目录失败
     */
    private static List<String> walFiles(Path dir, int pid) throws IOException {
        Path partitionDir = SegmentNaming.partitionDir(dir, pid);
        if (!Files.isDirectory(partitionDir)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(partitionDir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(SegmentNaming.WAL_EXT))
                    .sorted()
                    .toList();
        }
    }

    /**
     * 统计 {@code .idx} 文件数。
     *
     * @param dir 根目录
     * @param pid 分区 ID
     * @return 数量
     * @throws IOException 列目录失败
     */
    private static int countIdxFiles(Path dir, int pid) throws IOException {
        Path partitionDir = SegmentNaming.partitionDir(dir, pid);
        if (!Files.isDirectory(partitionDir)) {
            return 0;
        }
        try (Stream<Path> s = Files.list(partitionDir)) {
            return (int) s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(SegmentNaming.IDX_EXT))
                    .count();
        }
    }

    /**
     * 统计 {@code .tmp} 文件数。
     *
     * @param dir 根目录
     * @param pid 分区 ID
     * @return 数量
     * @throws IOException 列目录失败
     */
    private static long countTmpFiles(Path dir, int pid) throws IOException {
        Path partitionDir = SegmentNaming.partitionDir(dir, pid);
        if (!Files.isDirectory(partitionDir)) {
            return 0L;
        }
        try (Stream<Path> s = Files.list(partitionDir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(SegmentNaming.TMP_EXT))
                    .count();
        }
    }

    /**
     * 从 {@code wal-p0000-00000000000000000001.wal} 解析出名字 LSN。
     *
     * @param fileName 文件名
     * @return 名字 LSN
     */
    private static long parseLsn(String fileName) {
        int firstDash = fileName.indexOf('-');
        int secondDash = fileName.indexOf('-', firstDash + 1);
        int dot = fileName.lastIndexOf('.');
        if (firstDash < 0 || secondDash < 0 || dot <= secondDash) {
            throw new WalException.PartitionException("无法解析段文件名: " + fileName);
        }
        return Long.parseLong(fileName.substring(secondDash + 1, dot));
    }
}
