package com.walcore.util;

import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.MetaFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T05 验收之三：{@code wal.meta} 的<b>CRC 校验 + 原子替换 + 半截检测</b>。
 *
 * <p>为什么这三条都要单独测：{@code wal.meta} 是启动时最先读、也最容易被崩溃撕裂的文件。
 * 它一旦被误读，框架会拿到错误的 {@code nextLsn}（导致 LSN 重复，直接毁掉 P0-05）
 * 或错误的 {@code cleanShutdown}（导致跳过恢复校验）。
 * 因此"读到半截必须能被识别出来并降级为'扫描目录重建'"，是恢复流程安全的前提。
 */
class MetaFileTest {

    @Test
    @DisplayName("write → read round-trip，CRC 有效")
    void writeAndReadRoundTrip(@TempDir Path dir) throws IOException {
        Map<String, String> values = MetaFile.newEntries(1, 7L, 131_073L, false, "0,1,2,3");
        values.put(MetaFile.KEY_PARTITION_COUNT, "4");
        values.put(MetaFile.KEY_NEXT_SEGMENT_ID, "131072");
        MetaFile.write(dir, values);

        MetaFile.Meta meta = MetaFile.read(dir);
        assertTrue(meta.present());
        assertTrue(meta.crcValid(), "刚写完的 meta 必须 CRC 有效");
        assertTrue(meta.trusted());
        assertEquals(1L, meta.getLong(MetaFile.KEY_FORMAT_VERSION, -1L));
        assertEquals(7L, meta.getLong(MetaFile.KEY_EPOCH, -1L));
        assertEquals(131_073L, meta.getLong(MetaFile.KEY_NEXT_LSN, -1L));
        assertEquals(4L, meta.getLong(MetaFile.KEY_PARTITION_COUNT, -1L));
        assertFalse(meta.getBoolean(MetaFile.KEY_CLEAN_SHUTDOWN, true));
        assertEquals("0,1,2,3", meta.get(MetaFile.KEY_PARTITION_IDS));
        assertEquals(131_072L, meta.getLong(MetaFile.KEY_NEXT_SEGMENT_ID, -1L));
        assertNull(meta.get("不存在的键"));
        assertEquals(-42L, meta.getLong("不存在的键", -42L), "缺省值必须生效");
        assertTrue(meta.getBoolean("不存在的键", true));
    }

    @Test
    @DisplayName("文件末尾带 crc32c= 行，且 crc 覆盖除最后一行外的全部字节")
    void fileFormatIsTextWithCrcLine(@TempDir Path dir) throws IOException {
        MetaFile.write(dir, MetaFile.newEntries(1, 1L, 1L, true, "0"));
        Path file = dir.resolve(SegmentNaming.META_FILE);
        String text = Files.readString(file, StandardCharsets.UTF_8);
        String[] lines = text.split("\n");
        assertTrue(lines[0].startsWith(MetaFile.KEY_FORMAT_VERSION + "="), "首行应为 formatVersion");
        assertTrue(lines[lines.length - 1].startsWith(MetaFile.CRC_PREFIX),
                "最后一行必须是 crc32c= 行，实际: " + lines[lines.length - 1]);
        assertTrue(text.endsWith("\n"), "文件应以换行结束（避免最后一行被截断时歧义）");
        // 人类可直接 cat 排查是手写文本格式的主要理由之一
        assertTrue(text.contains("cleanShutdown=true"));
    }

    @Test
    @DisplayName("原子替换：写完不残留 .tmp，且覆盖旧文件后仍可读")
    void atomicReplaceLeavesNoTmp(@TempDir Path dir) throws IOException {
        MetaFile.write(dir, MetaFile.newEntries(1, 1L, 100L, false, "0,1"));
        MetaFile.write(dir, MetaFile.newEntries(1, 2L, 200L, true, "0,1"));
        MetaFile.write(dir, MetaFile.newEntries(1, 3L, 300L, true, "0,1,2"));

        try (Stream<Path> s = Files.list(dir)) {
            assertEquals(1L, s.count(), "目录里只应有 wal.meta，不得残留 wal.meta.tmp");
        }
        MetaFile.Meta meta = MetaFile.read(dir);
        assertEquals(3L, meta.getLong(MetaFile.KEY_EPOCH, -1L), "应读到最新一次写入");
        assertEquals(300L, meta.getLong(MetaFile.KEY_NEXT_LSN, -1L));
        assertTrue(meta.crcValid());
    }

    @Test
    @DisplayName("半截文件（写入中途崩溃）能被识别：present=true 但 crcValid=false")
    void tornFileIsDetected(@TempDir Path dir) throws IOException {
        MetaFile.write(dir, MetaFile.newEntries(1, 1L, 500L, false, "0,1,2"));
        Path file = dir.resolve(SegmentNaming.META_FILE);
        String full = Files.readString(file, StandardCharsets.UTF_8);

        // 模拟"只写进去一半"：截掉后 40% 的字节
        int cut = full.length() * 6 / 10;
        Files.writeString(file, full.substring(0, cut), StandardCharsets.UTF_8);

        MetaFile.Meta meta = MetaFile.read(dir);
        assertTrue(meta.present(), "文件仍在（只是内容残缺）");
        assertFalse(meta.crcValid(), "残缺文件必须 CRC 失败（否则会误信错误的 nextLsn）");
        assertFalse(meta.trusted(), "不可信任 → 调用方应扫描 partitions 目录重建");
    }

    @Test
    @DisplayName("内容被篡改（改一个数字）也能被 CRC 抓到")
    void tamperedContentIsDetected(@TempDir Path dir) throws IOException {
        MetaFile.write(dir, MetaFile.newEntries(1, 1L, 500L, false, "0"));
        Path file = dir.resolve(SegmentNaming.META_FILE);
        String text = Files.readString(file, StandardCharsets.UTF_8);
        // 把 nextLsn=500 改成 nextLsn=900：位数不变，长度不变，只有 CRC 能发现
        String tampered = text.replace("nextLsn=500", "nextLsn=900");
        assertTrue(tampered.contains("nextLsn=900"), "篡改应生效");
        Files.writeString(file, tampered, StandardCharsets.UTF_8);

        MetaFile.Meta meta = MetaFile.read(dir);
        assertFalse(meta.crcValid(), "篡改 nextLsn 后 CRC 必须失配（这是 P0-05 的最后一道防线）");
    }

    @Test
    @DisplayName("完全没有 crc 行的文件视为不可信")
    void missingCrcLineIsDistrusted(@TempDir Path dir) throws IOException {
        Path file = dir.resolve(SegmentNaming.META_FILE);
        Files.writeString(file, "formatVersion=1\nnextLsn=42\n", StandardCharsets.UTF_8);
        MetaFile.Meta meta = MetaFile.read(dir);
        assertTrue(meta.present());
        assertFalse(meta.crcValid(), "没有 crc 行时不能信任内容");
    }

    @Test
    @DisplayName("文件不存在时返回 present=false（不抛异常）")
    void absentFileIsNotAnError(@TempDir Path dir) throws IOException {
        MetaFile.Meta meta = MetaFile.read(dir);
        assertFalse(meta.present());
        assertFalse(meta.crcValid());
        assertFalse(meta.trusted());
        assertTrue(meta.values().isEmpty());
        assertEquals(0L, meta.getLong(MetaFile.KEY_NEXT_LSN, 0L));
    }

    @Test
    @DisplayName("调用方误传 crc32c 键时被忽略（避免双重 crc 行）")
    void callerSuppliedCrcKeyIsIgnored(@TempDir Path dir) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        values.put(MetaFile.KEY_NEXT_LSN, "77");
        values.put(MetaFile.CRC_PREFIX, "12345");
        MetaFile.write(dir, values);
        String text = Files.readString(dir.resolve(SegmentNaming.META_FILE),
                StandardCharsets.UTF_8);
        assertEquals(1, countOccurrences(text, MetaFile.CRC_PREFIX),
                "文件中必须恰好一行 crc32c=，实际内容:\n" + text);
        MetaFile.Meta meta = MetaFile.read(dir);
        assertTrue(meta.crcValid());
        assertEquals(77L, meta.getLong(MetaFile.KEY_NEXT_LSN, -1L));
    }

    @Test
    @DisplayName("values() 是不可变视图，防止调用方篡改解析结果")
    void valuesAreImmutable(@TempDir Path dir) throws IOException {
        MetaFile.write(dir, MetaFile.newEntries(1, 1L, 1L, true, "0"));
        MetaFile.Meta meta = MetaFile.read(dir);
        try {
            meta.values().put("hack", "1");
            throw new AssertionError("values() 必须不可变");
        } catch (UnsupportedOperationException expected) {
            // 期望行为
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
