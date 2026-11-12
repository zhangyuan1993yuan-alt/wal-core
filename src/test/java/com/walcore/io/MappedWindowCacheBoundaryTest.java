package com.walcore.io;

import com.walcore.internal.io.MappedWindowCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F3 回归：跨窗口边界的一次性读取不得泄漏 mmap 映射。</b>
 *
 * <h2>被回归的缺陷</h2>
 * 旧 {@code mapExact} 按精确范围 mmap 后关闭通道、返回映射切片：
 * 映射本身无人释放（{@code MappedByteBuffer} 没有 unmap API），切片被丢弃后
 * 要等 GC 触发 Cleaner 才回收。游标/tailer 每次跨窗口边界读都<b>泄漏一次映射</b>，
 * 长时间逐条推进时泄漏速率 ≈ 读取速率，虚拟地址空间与内核页表终被耗尽。
 *
 * <h2>断言口径</h2>
 * 修复后的实现改为堆拷贝：跨边界返回的缓冲 {@code isDirect() == false}
 * （旧实现返回 mmap 切片，恒为 direct）；窗口内命中仍是零拷贝 mmap 切片
 * （{@code isDirect() == true}），零拷贝主路径不受影响；窗口计数不因跨边界读取增长
 * （一次性路径不进 LRU，句柄上界防线仍然成立）。
 */
class MappedWindowCacheBoundaryTest {

    @Test
    @DisplayName("F3：跨窗口边界返回堆拷贝（非 direct），内容精确，窗口数不增长")
    void boundaryReadIsHeapCopyWithExactContent(@TempDir Path dir) throws IOException {
        int windowBytes = 4096;
        int size = windowBytes * 3;
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) ((i * 31 + 7) & 0xFF);
        }
        Path file = dir.resolve("boundary.bin");
        Files.write(file, content);

        try (MappedWindowCache cache = new MappedWindowCache(4, windowBytes)) {
            // ---- 窗口内命中：仍是 mmap 零拷贝切片 ----
            ByteBuffer inWindow = cache.acquire(file, 100L, 512);
            assertTrue(inWindow.isDirect(), "窗口内命中必须仍是 mmap 零拷贝切片");
            byte[] expected = new byte[512];
            System.arraycopy(content, 100, expected, 0, 512);
            assertArrayEquals(expected, toArray(inWindow, 512), "窗口内内容必须精确");

            // ---- 跨窗口边界：堆拷贝 + 内容精确 ----
            // 请求 [windowBytes-64, windowBytes-64+512)：横跨第 0/1 两个窗口的边界
            int offset = windowBytes - 64;
            ByteBuffer boundary = cache.acquire(file, (long) offset, 512);
            assertFalse(boundary.isDirect(),
                    "跨窗口边界的一次性读取必须是堆拷贝（F3：mmap 泄漏修复），"
                            + "旧实现这里返回的是无人释放的映射切片");
            byte[] expectedBoundary = new byte[512];
            System.arraycopy(content, offset, expectedBoundary, 0, 512);
            assertArrayEquals(expectedBoundary, toArray(boundary, 512),
                    "跨边界内容必须逐字节精确（堆拷贝不得丢字节/错位）");

            // ---- 边界读不进 LRU：窗口计数不增长 ----
            int before = cache.openFileCount();
            for (int i = 0; i < 100; i++) {
                // 反复跨边界读：若仍走 mmap（旧实现），这里是 100 次映射泄漏
                ByteBuffer again = cache.acquire(file, (long) offset, 512);
                assertFalse(again.isDirect(), "第 " + i + " 次跨边界读取仍必须是堆拷贝");
            }
            assertEquals(before, cache.openFileCount(),
                    "跨边界读取不得增加缓存窗口数（一次性路径不进 LRU）");

            // ---- 边界切片与窗口切片互不干扰 ----
            ByteBuffer after = cache.acquire(file, (long) (windowBytes * 2 + 16), 256);
            byte[] expectedAfter = new byte[256];
            System.arraycopy(content, windowBytes * 2 + 16, expectedAfter, 0, 256);
            assertArrayEquals(expectedAfter, toArray(after, 256), "后续窗口内读取仍精确");
        }
    }

    /**
     * 把缓冲内容读成字节数组。
     *
     * @param buffer 只读缓冲
     * @param len 期望长度
     * @return 字节
     */
    private static byte[] toArray(ByteBuffer buffer, int len) {
        assertEquals(len, buffer.remaining(), "缓冲可读长度必须等于请求长度");
        byte[] out = new byte[len];
        buffer.get(0, out);
        return out;
    }
}
