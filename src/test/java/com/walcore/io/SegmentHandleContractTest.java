package com.walcore.io;

import com.walcore.api.AppendOptions;
import com.walcore.internal.append.FrameEncoder;
import com.walcore.internal.io.FileChannelStorageBackend;
import com.walcore.internal.io.HeapStorageBackend;
import com.walcore.internal.io.MappedStorageBackend;
import com.walcore.internal.spiimpl.RawRecordCodec;
import com.walcore.internal.io.StorageBackendFactory;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 三后端<b>共用</b>的 {@link SegmentHandle} 契约测试。
 *
 * <p>这是"Windows 上正确性有保证"的唯一手段（ARCH §16.1）：同一套断言在
 * mmap / FileChannel / 堆内三个后端上各跑一遍。任何一个后端行为偏离，这里立刻定位。
 *
 * <p>对应 TASKS-wal.md T03 的验收点 ① ② ⑥ ⑦。
 */
class SegmentHandleContractTest {

    /** 测试用段容量：64KB（远小于默认 256MB，让 mmap 在 Windows 上也能秒级完成）。 */
    private static final long SEGMENT_SIZE = 64 * 1024;

    private static final int PARTITION_ID = 3;

    private static final long FIRST_LSN = 1_310_720L;

    private static final RawRecordCodec CODEC = new RawRecordCodec();

    private static final FrameEncoder ENCODER = new FrameEncoder(CODEC);

    /**
     * 三个后端 id 的数据源。
     *
     * @return id 流
     */
    static Stream<String> backendIds() {
        return Stream.of(
                StorageBackendFactory.ID_MAPPED,
                StorageBackendFactory.ID_FILE_CHANNEL,
                StorageBackendFactory.ID_HEAP);
    }

    /**
     * 按 id 构造并初始化一个后端。
     *
     * @param backendId 插件 id
     * @param baseDir 根目录
     * @return 后端
     * @throws IOException 初始化失败
     */
    private static StorageBackend backend(String backendId, Path baseDir) throws IOException {
        StorageConfig config = new StorageConfig(baseDir, SEGMENT_SIZE, false,
                1024 * 1024, 4, true);
        return new StorageBackendFactory(() -> true).create(backendId, config);
    }

    /**
     * 构造一帧。
     *
     * @param payloadSize payload 字节数
     * @param lsn LSN
     * @return 帧缓冲区（position=0，limit=capacity=frameLen）
     */
    private static ByteBuffer frame(int payloadSize, long lsn) {
        byte[] body = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            body[i] = (byte) ((i + lsn) & 0xFF);
        }
        int cap = ENCODER.requiredFrameLen(payloadSize);
        ByteBuffer work = ByteBuffer.allocateDirect(cap);
        int frameLen = ENCODER.encodeFrame(work, 0, cap, lsn, PARTITION_ID,
                ByteBuffer.wrap(body), AppendOptions.DEFAULTS);
        assertTrue(frameLen > 0);
        ByteBuffer exact = ByteBuffer.allocateDirect(frameLen);
        ByteBuffer v = work.duplicate();
        v.limit(frameLen);
        v.position(0);
        exact.put(v);
        exact.clear();
        return exact;
    }

    // ------------------------------------------------------------------
    // ① create / append / appendPosition
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: create 后基本属性正确")
    @MethodSource("backendIds")
    @DisplayName("① 三后端 create 后 capacity / appendPosition / isMapped 语义一致")
    void createBasics(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            assertEquals(FIRST_LSN, h.firstLsn());
            assertEquals(SEGMENT_SIZE, h.capacityBytes());
            assertEquals(0L, h.appendPosition());
            assertNotNull(h.path());
            assertEquals(StorageBackendFactory.ID_MAPPED.equals(backendId), h.isMapped(),
                    "只有 mmap 后端的 isMapped() 应为 true");
        }
    }

    // ------------------------------------------------------------------
    // ② 连续 append 后 appendPosition == Σ frameLen，read 逐字节等于写入内容
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: append 与 read 逐字节一致")
    @MethodSource("backendIds")
    @DisplayName("② 连续 append 后 appendPosition == Σ frameLen，read 逐字节等于写入内容")
    void appendAndReadBack(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        List<ByteBuffer> frames = new ArrayList<>();
        frames.add(frame(128, FIRST_LSN));
        frames.add(frame(256, FIRST_LSN + 1));
        frames.add(frame(8, FIRST_LSN + 2));
        frames.add(frame(1000, FIRST_LSN + 3));

        long expectedPos = 0L;
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            for (ByteBuffer f : frames) {
                h.append(f.duplicate());
                expectedPos += f.capacity();
                assertEquals(expectedPos, h.appendPosition(),
                        "appendPosition 必须等于 Σ frameLen");
            }
            assertEquals(expectedPos, h.appendPosition());
            assertTrue(h.physicalLength() >= h.appendPosition(),
                    "physicalLength 不得小于 appendPosition");

            long offset = 0L;
            for (ByteBuffer f : frames) {
                byte[] expected = new byte[f.capacity()];
                f.duplicate().get(expected);

                ByteBuffer dst = ByteBuffer.allocate(f.capacity());
                int n = h.read(offset, dst, f.capacity());
                assertEquals(f.capacity(), n, "读取字节数不符 @ offset=" + offset);
                byte[] actual = new byte[n];
                dst.position(0);
                dst.get(actual);
                assertArrayEquals(expected, actual, "读回的字节必须与写入完全一致 @ offset=" + offset);
                offset += f.capacity();
            }
        }
    }

    // ------------------------------------------------------------------
    // force / physicalLength
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: force 与 physicalLength")
    @MethodSource("backendIds")
    @DisplayName("① force(false)/force(true) 均不抛异常，physicalLength 随写入单调不减")
    void forceAndPhysicalLength(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            long before = h.physicalLength();
            h.append(frame(256, FIRST_LSN));
            h.force(false);
            h.force(true);
            long after = h.physicalLength();
            assertTrue(after >= before);
            assertTrue(after >= h.appendPosition());
        }
    }

    // ------------------------------------------------------------------
    // truncate
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: truncate 后 appendPosition 与可读内容一致")
    @MethodSource("backendIds")
    @DisplayName("① truncate 后 appendPosition == newLength，且 [0,newLength) 仍可正确读出")
    void truncateResetsAppendPosition(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        ByteBuffer f1 = frame(128, FIRST_LSN);
        ByteBuffer f2 = frame(256, FIRST_LSN + 1);
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            h.append(f1.duplicate());
            h.append(f2.duplicate());
            long cut = f1.capacity();
            h.truncate(cut);
            assertEquals(cut, h.appendPosition(), "truncate 后 appendPosition 必须等于 newLength");
            assertTrue(h.physicalLength() >= cut);

            ByteBuffer dst = ByteBuffer.allocate((int) cut);
            int n = h.read(0L, dst, (int) cut);
            assertEquals((int) cut, n);
            byte[] expected = new byte[(int) cut];
            f1.duplicate().get(expected);
            byte[] actual = new byte[n];
            dst.position(0);
            dst.get(actual);
            assertArrayEquals(expected, actual);

            // 截断后可以继续追加（恢复流程依赖这一点）
            h.append(f2.duplicate());
            assertEquals(cut + f2.capacity(), h.appendPosition());
        }
    }

    // ------------------------------------------------------------------
    // ⑥ close 幂等 / 关闭后文件可删除（Windows 上 mmap 的 key 验证）
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: close 幂等且关闭后可删除段文件")
    @MethodSource("backendIds")
    @DisplayName("⑥ close() 幂等；关闭后段文件可被删除（mmap 后端依赖 unmap 生效）")
    void closeIsIdempotentAndFileDeletable(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE);
        Path path = h.path();
        h.append(frame(256, FIRST_LSN));
        h.force(false);
        h.close();
        h.close(); // 幂等：不得抛异常

        if (StorageBackendFactory.ID_HEAP.equals(backendId)) {
            assertFalse(Files.exists(path), "堆内后端不应真的创建文件");
        } else {
            assertTrue(Files.exists(path), "关闭后文件仍应存在");
            // 关键断言：如果 mmap 未 unmap，Windows 上这里会抛 AccessDeniedException
            Files.delete(path);
            assertFalse(Files.exists(path), "关闭后段文件必须可删除（unmap 已生效）");
        }
        be.close();
    }

    // ------------------------------------------------------------------
    // open 既有段
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: open 只读句柄能读回既有内容")
    @MethodSource("backendIds")
    @DisplayName("① open(existing, writable=false) 能读回此前写入的字节")
    void reopenReadOnly(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        ByteBuffer f = frame(512, FIRST_LSN);
        Path path;
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            h.append(f.duplicate());
            h.force(true);
            path = h.path();
        }
        try (SegmentHandle ro = be.open(path, FIRST_LSN, false)) {
            assertTrue(ro.appendPosition() > 0L,
                    "只读句柄的 appendPosition 应反映文件实际长度");
            ByteBuffer dst = ByteBuffer.allocate(f.capacity());
            int n = ro.read(0L, dst, f.capacity());
            assertEquals(f.capacity(), n);
            byte[] expected = new byte[f.capacity()];
            f.duplicate().get(expected);
            byte[] actual = new byte[n];
            dst.position(0);
            dst.get(actual);
            assertArrayEquals(expected, actual);
            assertThrows(IOException.class, () -> ro.append(f.duplicate()),
                    "只读句柄不得允许 append");
        }
    }

    // ------------------------------------------------------------------
    // 越界写入
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "后端 {0}: 超出容量写入被拒绝")
    @MethodSource("backendIds")
    @DisplayName("写入超过段容量时抛 IOException，且 appendPosition 不前进")
    void appendBeyondCapacityFails(String backendId, @TempDir Path tmpDir) throws IOException {
        StorageBackend be = backend(backendId, tmpDir);
        try (SegmentHandle h = be.create(PARTITION_ID, FIRST_LSN, SEGMENT_SIZE)) {
            ByteBuffer big = frame((int) (SEGMENT_SIZE - 100), FIRST_LSN);
            h.append(big.duplicate());
            long pos = h.appendPosition();
            ByteBuffer extra = frame(256, FIRST_LSN + 1);
            assertThrows(IOException.class, () -> h.append(extra.duplicate()));
            assertEquals(pos, h.appendPosition(), "失败的 append 不得推进 appendPosition");
        }
    }

    // ------------------------------------------------------------------
    // 后端 id / 能力声明
    // ------------------------------------------------------------------

    @Test
    @DisplayName("三后端的 id / apiVersion / supportsZeroCopyAppend 声明正确")
    void backendMetadata() throws IOException {
        Path dir = Files.createTempDirectory("wal-meta");
        try {
            assertEquals("builtin:storage-mapped", new MappedStorageBackend().id());
            assertEquals("builtin:storage-fc", new FileChannelStorageBackend().id());
            assertEquals("builtin:storage-heap", new HeapStorageBackend().id());
            assertTrue(new MappedStorageBackend().supportsZeroCopyAppend());
            assertFalse(new FileChannelStorageBackend().supportsZeroCopyAppend());
            assertFalse(new HeapStorageBackend().supportsZeroCopyAppend());
            assertEquals(com.walcore.api.WalVersion.API_VERSION, new MappedStorageBackend().apiVersion());
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    @DisplayName("StorageBackendFactory 按平台探测选择默认后端（两个分支都覆盖）")
    void factoryPlatformBranch() {
        StorageBackendFactory linux = new StorageBackendFactory(() -> true);
        assertEquals(StorageBackendFactory.ID_MAPPED, linux.defaultBackendId());

        StorageBackendFactory other = new StorageBackendFactory(() -> false);
        assertEquals(StorageBackendFactory.ID_FILE_CHANNEL, other.defaultBackendId());

        // 真实平台：Windows 开发机必须选 FileChannel
        StorageBackendFactory real = new StorageBackendFactory();
        if (!com.walcore.internal.util.Platform.isLinuxX86_64()) {
            assertEquals(StorageBackendFactory.ID_FILE_CHANNEL, real.defaultBackendId());
        }
        assertThrows(IllegalArgumentException.class, () -> real.instantiate("builtin:storage-nope"));
    }

    /**
     * 递归删除临时目录（避免 JDK 的 @TempDir 清理时因 mmap 残留失败）。
     *
     * @param dir 目录
     * @throws IOException IO 失败
     */
    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            List<Path> paths = walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList();
            for (Path p : paths) {
                Files.deleteIfExists(p);
            }
        }
    }
}
