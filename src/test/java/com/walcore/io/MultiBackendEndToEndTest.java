package com.walcore.io;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.io.StorageBackendFactory;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>三后端端到端</b>契约测试：{@code DefaultWal} 层跑通 mmap / FileChannel / Heap。
 *
 * <h2>与 {@code SegmentHandleContractTest} 的分工</h2>
 * 那个测试验证的是<b>句柄</b>（create/open/append/read/force/truncate）的行为一致性；
 * 本类验证的是<b>整条链路</b>：写入管线 → 组提交 → 游标读 → 重开恢复。
 * 差别很重要：句柄契约全绿，上层仍可能因为"某后端不返回正确的物理长度"或
 * "某后端 {@code force} 语义不同"而在恢复时丢数据，这类问题只有端到端能抓到。
 *
 * <p><b>Windows 上的意义</b>：开发机默认走 FileChannel 后端，mmap 后端在 Windows 上
 * 只保证"能跑通"（性能不达标）。本类强制三个后端各跑一遍，保证不存在
 * "仅 Linux 可编译/可运行"的代码分支——这是 TASKS §0 铁律 6 的强制要求。
 */
@DisplayName("三后端端到端（mmap / FileChannel / Heap）")
class MultiBackendEndToEndTest {

    private static final int RECORDS = 40;
    private static final long SEGMENT = 4096L;

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            StorageBackendFactory.ID_MAPPED,
            StorageBackendFactory.ID_FILE_CHANNEL,
            StorageBackendFactory.ID_HEAP})
    @DisplayName("写入 → 刷盘 → 读回 全链路一致")
    void writeFlushReadIsConsistent(String backendId, @TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.storageBackend(backendId))) {
            assertEquals(backendId, wal.config().storageBackendId(),
                    "配置必须记录实际所选后端，否则运维无法确认跑的是哪一个");

            List<byte[]> originals = appendAndFlush(wal, backendId);

            List<WalRecord> read = readAll(wal, 0);
            assertEquals(RECORDS, read.size(), backendId + " 后端读回条数不符");
            for (int i = 0; i < RECORDS; i++) {
                assertEquals(i + 1L, read.get(i).lsn(), backendId + " 后端 LSN 顺序不符");
                assertArrayEquals(originals.get(i), read.get(i).payloadBytes(),
                        backendId + " 后端第 " + i + " 条 payload 不一致");
            }
            assertTrue(wal.stats().totalLogBytes() > 0L,
                    backendId + " 后端日志字节数必须非零（口径见 WalStats#totalLogBytes）");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            StorageBackendFactory.ID_MAPPED,
            StorageBackendFactory.ID_FILE_CHANNEL})
    @DisplayName("落盘后端：重开恢复 + LSN 接续")
    void reopenRecoversAndContinuesLsn(String backendId, @TempDir Path dir) {
        List<byte[]> originals;
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.storageBackend(backendId))) {
            originals = appendAndFlush(wal, backendId);
        }
        try (DefaultWal reopened = TestWal.open(dir, SEGMENT, b -> b.storageBackend(backendId))) {
            List<WalRecord> recovered = readAll(reopened, 0);
            assertEquals(RECORDS, recovered.size(),
                    backendId + " 后端重开后条数必须一致（恢复路径依赖后端返回正确的物理长度）");
            for (int i = 0; i < RECORDS; i++) {
                assertArrayEquals(originals.get(i), recovered.get(i).payloadBytes(),
                        backendId + " 后端重开后第 " + i + " 条 payload 不一致");
            }
            long next = reopened.append(0, ByteBuffer.wrap("after-reopen"
                    .getBytes(StandardCharsets.UTF_8)), AppendOptions.DEFAULTS);
            assertEquals(RECORDS + 1L, next,
                    backendId + " 后端重开后 LSN 必须接续（既不回退也不跳号）");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {StorageBackendFactory.ID_HEAP})
    @DisplayName("内存后端：不产生任何 .wal 文件（名副其实的纯内存）")
    void heapBackendLeavesNoFiles(String backendId, @TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT, b -> b.storageBackend(backendId))) {
            appendAndFlush(wal, backendId);
            assertEquals(RECORDS, readAll(wal, 0).size(), "内存后端读回条数不符");
        }
        Path partitions = dir.resolve("partitions");
        if (!Files.isDirectory(partitions)) {
            return;
        }
        try (Stream<Path> files = Files.walk(partitions)) {
            long walFiles = files.filter(p -> p.toString().endsWith(".wal")).count();
            assertEquals(0L, walFiles,
                    "Heap 后端不应在磁盘留下 .wal 文件，实际 " + walFiles + " 个");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 写 {@link #RECORDS} 条并刷盘。
     *
     * @param wal 实例
     * @param backendId 后端 id（用于构造可辨识的 payload）
     * @return 原始 payload 字节列表
     */
    private static List<byte[]> appendAndFlush(DefaultWal wal, String backendId) {
        List<byte[]> originals = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            byte[] bytes = ("be-" + backendId + "-" + i).getBytes(StandardCharsets.UTF_8);
            originals.add(bytes);
            long lsn = wal.append(0, ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
            assertEquals(i + 1L, lsn, "LSN 必须从 1 起严格递增");
        }
        assertTrue(wal.flushAndWait(0, RECORDS, 20_000L),
                backendId + " 后端必须能在超时内确认持久化");
        return originals;
    }

    /**
     * 用游标读回某分区全部记录副本。
     *
     * @param wal 实例
     * @param pid 分区
     * @return 记录副本
     */
    private static List<WalRecord> readAll(DefaultWal wal, int pid) {
        List<WalRecord> out = new ArrayList<>();
        try (WalCursor cursor = wal.cursor(pid,
                CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.copy());
            }
        }
        return out;
    }
}
