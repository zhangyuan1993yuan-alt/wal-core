package com.walcore.io;

import com.walcore.internal.io.MappedWindowCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MappedWindowCache} 的验收测试（TASKS-wal.md T03 验收点 ⑤）。
 *
 * <p>核心断言：<b>同时打开的文件句柄数永不超过 {@code windowCount}</b>。
 * 这是 ARCH §14 失败模式 #3（EMFILE）的防线——若 4096 个分区各自整段 mmap 只读段，
 * 进程会瞬间耗尽文件句柄。窗口化 + LRU 把这个上界压成常数。
 */
class MappedWindowCacheTest {

    /**
     * 造一个内容可识别的文件。
     *
     * @param dir 目录
     * @param name 文件名
     * @param size 字节数
     * @param seed 内容种子
     * @return 路径
     * @throws IOException IO 失败
     */
    private static Path makeFile(Path dir, String name, int size, int seed) throws IOException {
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) ((seed * 31 + i * 7) & 0xFF);
        }
        Path p = dir.resolve(name);
        Files.write(p, content);
        return p;
    }

    @Test
    @DisplayName("⑤ 打开的文件句柄数永不超 windowCount，LRU 逐出生效")
    void handleCountNeverExceedsWindowCount(@TempDir Path tmpDir) throws IOException {
        int windowCount = 8;
        long windowBytes = 4096L;
        List<Path> files = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            files.add(makeFile(tmpDir, "seg-" + i + ".bin", 8192, i));
        }
        try (MappedWindowCache cache = new MappedWindowCache(windowCount, windowBytes)) {
            for (Path p : files) {
                ByteBuffer view = cache.acquire(p, 0L, 1024);
                assertTrue(view.remaining() >= 1024);
                assertTrue(cache.openFileCount() <= windowCount,
                        "打开的文件数 " + cache.openFileCount() + " 超过上限 " + windowCount);
            }
            assertEquals(windowCount, cache.openFileCount(), "满载后应恰好维持 windowCount 个窗口");
            assertTrue(cache.evictedCount() >= 4L,
                    "应已逐出至少 4 个窗口，实际 " + cache.evictedCount());
        }
    }

    @Test
    @DisplayName("⑤ 返回的切片内容正确（含窗口内偏移）")
    void sliceContentCorrect(@TempDir Path tmpDir) throws IOException {
        long windowBytes = 4096L;
        int size = 8192;
        Path p = makeFile(tmpDir, "content.bin", size, 3);
        byte[] content = Files.readAllBytes(p);

        try (MappedWindowCache cache = new MappedWindowCache(2, windowBytes)) {
            long offset = windowBytes + 100L;
            ByteBuffer view = cache.acquire(p, offset, 50);
            byte[] actual = new byte[50];
            view.get(actual);
            byte[] expected = new byte[50];
            System.arraycopy(content, (int) offset, expected, 0, 50);
            assertArrayEquals(expected, actual);
            assertEquals(1, cache.openFileCount());
        }
    }

    @Test
    @DisplayName("⑤ 同一窗口重复 acquire 不新增句柄；evict(path) 释放该文件全部窗口")
    void reuseAndEvict(@TempDir Path tmpDir) throws IOException {
        Path a = makeFile(tmpDir, "a.bin", 8192, 1);
        Path b = makeFile(tmpDir, "b.bin", 8192, 2);
        try (MappedWindowCache cache = new MappedWindowCache(4, 4096L)) {
            cache.acquire(a, 0L, 100);
            cache.acquire(a, 100L, 100);
            cache.acquire(a, 4000L, 50);
            assertEquals(1, cache.openFileCount(), "同一文件同一窗口应复用");

            cache.acquire(a, 5000L, 100);
            assertEquals(2, cache.openFileCount(), "跨到第二个窗口后应有 2 个窗口");

            cache.acquire(b, 0L, 100);
            assertEquals(3, cache.openFileCount());

            cache.evict(a);
            assertEquals(1, cache.openFileCount(), "evict(a) 后只应剩 b 的窗口");
            cache.evict(b);
            assertEquals(0, cache.openFileCount());
        }
    }

    @Test
    @DisplayName("⑤ 单次请求长度超过窗口大小时拒绝（提示调大 mappedWindowBytes）")
    void requestLargerThanWindowRejected(@TempDir Path tmpDir) throws IOException {
        Path p = makeFile(tmpDir, "big.bin", 16384, 5);
        try (MappedWindowCache cache = new MappedWindowCache(2, 1024L)) {
            assertThrows(IllegalArgumentException.class, () -> cache.acquire(p, 0L, 2048));
            // 边界值：恰好等于窗口大小是允许的
            assertEquals(1024, cache.acquire(p, 0L, 1024).remaining());
        }
    }

    @Test
    @DisplayName("⑤ close() 后 acquire 抛异常，且句柄被全部释放")
    void closedCacheRejectsAcquire(@TempDir Path tmpDir) throws IOException {
        Path p = makeFile(tmpDir, "c.bin", 4096, 7);
        MappedWindowCache cache = new MappedWindowCache(2, 4096L);
        cache.acquire(p, 0L, 64);
        assertEquals(1, cache.openFileCount());
        cache.close();
        assertEquals(0, cache.openFileCount());
        assertThrows(IOException.class, () -> cache.acquire(p, 0L, 64));
        cache.close(); // 幂等
    }

    @Test
    @DisplayName("mappedBytes 汇总当前窗口占用，且随逐出下降")
    void mappedBytesAccounting(@TempDir Path tmpDir) throws IOException {
        Path p = makeFile(tmpDir, "d.bin", 8192, 9);
        try (MappedWindowCache cache = new MappedWindowCache(4, 4096L)) {
            assertEquals(0L, cache.mappedBytes());
            cache.acquire(p, 0L, 64);
            assertEquals(4096L, cache.mappedBytes());
            cache.acquire(p, 4096L, 64);
            assertTrue(cache.mappedBytes() <= 8192L);
            cache.evict(p);
            assertEquals(0L, cache.mappedBytes());
        }
    }
}
