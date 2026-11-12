package com.walcore.segment;

import com.walcore.internal.segment.SegmentNaming;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T05 验收之一：<b>段文件命名规则</b>（ARCH §6.1）。
 *
 * <p>核心命题只有一条但极其重要：
 * <b>{@code firstLsn} 的字典序必须等于数值序</b>。满足它，才能"按字符串排序即时间序"，
 * 运维 {@code ls}、恢复期扫描目录、{@code Collections.sort} 都无需解析数字。
 * 一旦不满足（例如漏了前导零），恢复期会把段顺序搞错，进而逐帧重组出<b>乱序的 LSN 流</b>，
 * 而这种错误在功能测试里几乎不可能被发现。
 *
 * <p>验证手段：随机取 100 个 {@code firstLsn}（含极小、极大、跨越位数边界的值），
 * 转成文件名后<b>按字符串排序</b>，结果必须与按数值排序完全一致。
 */
class SegmentNamingTest {

    private static final Path BASE = Paths.get("build", "wal-test");

    @Test
    @DisplayName("字典序 == 数值序（100 个随机 firstLsn，含位数边界）")
    void lexicographicOrderEqualsNumericOrder() {
        Random rnd = new Random(20251107L);
        List<Long> lsns = new ArrayList<>();
        // 边界值：1、2、9、10、99、100、999、1000、...、Long.MAX_VALUE
        lsns.add(1L);
        lsns.add(2L);
        lsns.add(9L);
        lsns.add(10L);
        lsns.add(99L);
        lsns.add(100L);
        lsns.add(999L);
        lsns.add(1_000L);
        lsns.add(9_999L);
        lsns.add(10_000L);
        lsns.add(99_999_999L);
        lsns.add(100_000_000L);
        lsns.add(1_000_000_000_000_000_000L);
        lsns.add(9_000_000_000_000_000_000L);
        lsns.add(Long.MAX_VALUE);
        while (lsns.size() < 100) {
            long v = Math.floorMod(rnd.nextLong(), Long.MAX_VALUE);
            if (v > 0L) {
                lsns.add(v);
            }
        }

        List<Long> byNumber = new ArrayList<>(lsns);
        byNumber.sort(Comparator.naturalOrder());

        List<String> names = new ArrayList<>(lsns.size());
        for (long lsn : lsns) {
            names.add(SegmentNaming.walFileName(7, lsn));
        }
        List<String> byString = new ArrayList<>(names);
        byString.sort(Comparator.naturalOrder());

        for (int i = 0; i < byNumber.size(); i++) {
            String expected = SegmentNaming.walFileName(7, byNumber.get(i));
            assertEquals(expected, byString.get(i),
                    "第 " + i + " 位字符串排序结果与数值排序不一致（前缀零位数不足？）");
        }
    }

    @Test
    @DisplayName("同分区 1000 个连续段名互不相同且严格递增")
    void consecutiveNamesAreUniqueAndIncreasing() {
        Set<String> seen = new HashSet<>();
        String prev = null;
        for (int i = 1; i <= 1000; i++) {
            String name = SegmentNaming.walFileName(42, i);
            assertTrue(seen.add(name), "文件名重复: " + name);
            if (prev != null) {
                assertTrue(prev.compareTo(name) < 0,
                        "字典序必须严格递增: " + prev + " vs " + name);
            }
            prev = name;
        }
        assertEquals(1000, seen.size());
    }

    @Test
    @DisplayName("文件名格式固定为 wal-p%04d-%020d.wal")
    void fileNameFormat() {
        assertEquals("wal-p0000-00000000000000000001.wal", SegmentNaming.walFileName(0, 1L));
        assertEquals("wal-p0003-00000000000000131072.wal", SegmentNaming.walFileName(3, 131_072L));
        assertEquals("wal-p4095-09223372036854775807.wal",
                SegmentNaming.walFileName(4095, Long.MAX_VALUE));
        assertEquals("wal-p0000-00000000000000000001.idx", SegmentNaming.idxFileName(0, 1L));
        assertEquals("wal-p0000-00000000000000000001.wal.tmp",
                SegmentNaming.tmpFileName(0, 1L));
        assertEquals("p0007", SegmentNaming.partitionName(7));
    }

    @Test
    @DisplayName("parse 能还原 pid/firstLsn/扩展名（round-trip）")
    void parseRoundTrip() {
        long[] lsns = {1L, 2L, 131_072L, 268_435_457L, Long.MAX_VALUE};
        int[] pids = {0, 1, 3, 4095};
        for (int pid : pids) {
            for (long lsn : lsns) {
                assertParsed(SegmentNaming.walFileName(pid, lsn), pid, lsn, SegmentNaming.WAL_EXT);
                assertParsed(SegmentNaming.idxFileName(pid, lsn), pid, lsn, SegmentNaming.IDX_EXT);
                assertParsed(SegmentNaming.tmpFileName(pid, lsn), pid, lsn,
                        SegmentNaming.WAL_EXT + SegmentNaming.TMP_EXT);
                // 完整路径同样可解析（只取文件名部分）
                assertEquals(Optional.of(lsn),
                        SegmentNaming.parseFirstLsn(
                                SegmentNaming.walPath(BASE, pid, lsn).getFileName().toString()));
            }
        }
    }

    @Test
    @DisplayName("非法文件名一律返回 Optional.empty()，不抛异常")
    void parseRejectsInvalidNames() {
        String[] invalid = {
                null,
                "",
                "wal.meta",
                "LOCK",
                "no-extension",
                "wal-p003-000000000000000000001.wal",       // pid 只有 3 位
                "wal-p00003-00000000000000000001.wal",      // pid 有 5 位
                "wal-p0000-0000000000000000001.wal",        // lsn 只有 19 位
                "wal-p0000-000000000000000000001.wal",      // lsn 有 21 位
                "wal-p-001-00000000000000000001.wal",       // pid 含负号
                "wal-p0000--0000000000000000001.wal",       // lsn 含负号
                "wal-p0000-00000000000000000000.wal",       // lsn == 0
                "wal-p4096-00000000000000000001.wal",       // pid 越界
                "wal-p0000-00000000000000000001.log",       // 扩展名不认识
                "xyz-p0000-00000000000000000001.wal",       // 前缀不对
                "wal-p0000-0000000000000000001a.wal",       // lsn 含字母
                "wal-p000x-00000000000000000001.wal",       // pid 含字母
                "wal-p000000000000000000001.wal",           // 缺分隔符
        };
        for (String name : invalid) {
            assertEquals(Optional.empty(), SegmentNaming.parseName(name),
                    "非法名必须返回 empty: " + name);
        }
        assertEquals(Optional.empty(), SegmentNaming.parse(null));
        assertEquals(Optional.empty(), SegmentNaming.parseFirstLsn("wal.meta"));
    }

    @Test
    @DisplayName("isWalFile / isIdxFile / isSealedWalFile 区分三种扩展名")
    void extensionPredicates() {
        Path wal = SegmentNaming.walPath(BASE, 1, 5L);
        Path idx = SegmentNaming.idxPath(BASE, 1, 5L);
        Path tmp = SegmentNaming.tmpPath(BASE, 1, 5L);
        Path meta = BASE.resolve("wal.meta");

        assertTrue(SegmentNaming.isWalFile(wal));
        assertTrue(SegmentNaming.isWalFile(tmp), "预建中的 .wal.tmp 也属于数据文件");
        assertFalse(SegmentNaming.isIdxFile(wal));
        assertTrue(SegmentNaming.isIdxFile(idx));
        assertFalse(SegmentNaming.isWalFile(idx));
        assertTrue(SegmentNaming.isSealedWalFile(wal));
        assertFalse(SegmentNaming.isSealedWalFile(tmp), ".tmp 尚未封存完成");
        assertFalse(SegmentNaming.isWalFile(meta));
        assertFalse(SegmentNaming.isIdxFile(meta));
    }

    @Test
    @DisplayName("目录布局：partitions/p%04d 与游标/状态文件名")
    void directoryLayout() {
        assertEquals(BASE.resolve("partitions").resolve("p0000"),
                SegmentNaming.partitionDir(BASE, 0));
        assertEquals(BASE.resolve("partitions").resolve("p4095"),
                SegmentNaming.partitionDir(BASE, 4095));
        assertEquals(BASE.resolve("partitions").resolve("p0012")
                        .resolve("wal-p0012-00000000000000000042.wal"),
                SegmentNaming.walPath(BASE, 12, 42L));
        assertEquals(BASE.resolve("partitions").resolve("p0012").resolve("p0012.cursor"),
                SegmentNaming.cursorPath(BASE, 12));
        assertEquals(BASE.resolve("partitions").resolve("p0012").resolve("p0012.state"),
                SegmentNaming.statePath(BASE, 12));
        assertEquals("partitions", SegmentNaming.PARTITIONS_DIR);
        assertEquals("wal.meta", SegmentNaming.META_FILE);
        assertEquals("LOCK", SegmentNaming.LOCK_FILE);
        assertEquals(4095, SegmentNaming.MAX_PARTITION_ID);
    }

    @Test
    @DisplayName("非法入参在生成阶段就拒绝（pid 越界 / firstLsn <= 0）")
    void rejectsIllegalArguments() {
        assertThrows(IllegalArgumentException.class, () -> SegmentNaming.walFileName(-1, 1L));
        assertThrows(IllegalArgumentException.class, () -> SegmentNaming.walFileName(4096, 1L));
        assertThrows(IllegalArgumentException.class, () -> SegmentNaming.walFileName(0, 0L));
        assertThrows(IllegalArgumentException.class, () -> SegmentNaming.walFileName(0, -5L));
        assertThrows(IllegalArgumentException.class, () -> SegmentNaming.partitionDir(BASE, -1));
        assertThrows(NullPointerException.class, () -> SegmentNaming.partitionDir(null, 0));
    }

    private static void assertParsed(String fileName, int pid, long lsn, String extension) {
        Optional<SegmentNaming.Parsed> p = SegmentNaming.parseName(fileName);
        assertTrue(p.isPresent(), "应能解析: " + fileName);
        assertEquals(pid, p.get().partitionId(), "pid 不一致: " + fileName);
        assertEquals(lsn, p.get().firstLsn(), "firstLsn 不一致: " + fileName);
        assertEquals(extension, p.get().extension(), "扩展名不一致: " + fileName);
    }
}
