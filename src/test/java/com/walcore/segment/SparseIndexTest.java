package com.walcore.segment;

import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.segment.SparseIndex;
import com.walcore.internal.spiimpl.DefaultIndexStore;
import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexHit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T06 验收：<b>稀疏索引的二分正确性、延迟、持久化与降级</b>。
 *
 * <h2>本测试要证明的四件事</h2>
 * <ol>
 *   <li><b>空间核算</b>：1 亿条记录按 {@code indexIntervalRecords=1000} 只需 10 万项
 *       = 1.6MB/分区，远低于 P0-07 ② 的 {@code 32B × 记录数/1000} 上限。
 *       这里用 1M 条 → 1000 项 → 16KB 验证比例关系。</li>
 *   <li><b>定位延迟</b>：{@code lookup} 必须是 O(log n) 的纯内存二分，p99 ≤ 10µs
 *       （实测通常在 100ns 量级）。若退化成线性扫，1000 项也能测出差异。</li>
 *   <li><b>持久化</b>：{@code .idx} 有 32B 头（magic/version/firstLsn/itemCount/crc），
 *       重启后能重新加载；头部 CRC 错必须能识别并给出 {@code needsRebuild} 信号。</li>
 *   <li><b>降级</b>：内存预算耗尽后 {@code heapDisabled} 置位、{@code memoryBytes()} 归零，
 *       查找改为文件二分——绝不 OOM（{@code maxIndexHeapBytes} 是硬上限）。</li>
 * </ol>
 */
class SparseIndexTest {

    private static final int INTERVAL = 1000;

    // ==================================================================
    // 1. 空间核算与基本二分
    // ==================================================================

    @Test
    @DisplayName("1M 条记录 / 每 1000 条一项 = 1000 项 = 16KB 堆内")
    void memoryFootprintMatchesBudget() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 256L * 1024 * 1024);
        int records = 1_000_000;
        for (int i = 0; i < records; i += INTERVAL) {
            idx.append(1L + i, (long) i * 320L);
        }
        assertEquals(records / INTERVAL, idx.size(), "索引项数必须只有记录数的 0.1%");
        // 1000 项 × 2 个 long = 16KB（数组按 1.5 倍扩容，允许少量余量）
        long bytes = idx.memoryBytes();
        assertTrue(bytes >= 1000L * 16L && bytes <= 1600L * 16L,
                "1000 项的堆内占用应约 16KB，实际 " + bytes + " 字节");
        assertEquals(1L, idx.lastIndexedLsn() - (records - INTERVAL),
                "lastIndexedLsn 必须是最后一项的 LSN");
    }

    @Test
    @DisplayName("lookup 与暴力 floor 查找在 10 万个随机 LSN 上完全一致")
    void lookupMatchesBruteForce() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(2, 1L, 0L);
        List<Long> lsns = new ArrayList<>();
        long lsn = 5L;
        Random rnd = new Random(42L);
        for (int i = 0; i < 2000; i++) {
            lsns.add(lsn);
            idx.append(lsn, lsn * 10L);
            lsn += 1 + rnd.nextInt(900);
        }
        Random q = new Random(7L);
        for (int i = 0; i < 100_000; i++) {
            long target = Math.floorMod(q.nextLong(), lsn + 1000L);
            IndexHit hit = idx.lookup(target);
            long expectedLsn = -1L;
            for (long candidate : lsns) {
                if (candidate <= target) {
                    expectedLsn = candidate;
                } else {
                    break;
                }
            }
            if (expectedLsn < 0L) {
                assertEquals(IndexHit.NONE, hit, "target=" + target + " 应无命中");
                assertFalse(hit.isPresent());
            } else {
                assertTrue(hit.isPresent(), "target=" + target + " 应命中 " + expectedLsn);
                assertEquals(expectedLsn, hit.lsn(),
                        "target=" + target + " 的 floor 不一致");
                assertEquals(expectedLsn * 10L, hit.fileOffset());
            }
        }
    }

    @Test
    @DisplayName("lookup p99 ≤ 10µs（100 万次查询）")
    @Tag("perf")
    void lookupLatency() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        for (int i = 0; i < 100_000; i++) {
            idx.append(1L + (long) i * INTERVAL, (long) i * 320L);
        }
        long[] nanos = new long[1_000_000];
        Random rnd = new Random(11L);
        for (int i = 0; i < nanos.length; i++) {
            long target = 1L + Math.floorMod(rnd.nextLong(), 100_000L * INTERVAL);
            long t0 = System.nanoTime();
            IndexHit hit = idx.lookup(target);
            nanos[i] = System.nanoTime() - t0;
            if (!hit.isPresent()) {
                throw new AssertionError("target=" + target + " 不该无命中");
            }
        }
        Arrays.sort(nanos);
        long p50 = nanos[nanos.length / 2];
        long p99 = nanos[(int) (nanos.length * 0.99)];
        assertTrue(p99 <= 10_000L,
                "lookup p99 = " + (p99 / 1000.0) + "µs 超过 10µs 上限"
                        + "（p50=" + (p50 / 1000.0) + "µs，10 万项二分应远小于此）");
    }

    @Test
    @DisplayName("低于首个索引项的 LSN 返回 IndexHit.NONE，由上层做段首顺序扫")
    void lookupBelowFirstEntryReturnsNone() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 100L, 0L);
        idx.append(100L, 0L);
        idx.append(200L, 320L);
        assertEquals(IndexHit.NONE, idx.lookup(99L));
        assertEquals(IndexHit.NONE, idx.lookup(0L));
        assertEquals(100L, idx.lookup(100L).lsn());
        assertEquals(100L, idx.lookup(199L).lsn(), "命中 floor 项");
        assertEquals(200L, idx.lookup(200L).lsn());
        assertEquals(200L, idx.lookup(Long.MAX_VALUE).lsn(), "超出末项时落到末项");
    }

    @Test
    @DisplayName("中间缺项（数据损坏/丢失）时仍返回前一个索引项，由顺序扫兜底")
    void missingMiddleEntryStillResolvesToPrevious() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        for (int i = 0; i < 10; i++) {
            idx.append(1L + (long) i * 100L, (long) i * 320L);
        }
        // 模拟"第 5 项丢了"：直接截断到只剩 lsns <= 301 的 4 项（1/101/201/301）
        idx.truncateAfter(301L);
        assertEquals(4, idx.size());
        IndexHit hit = idx.lookup(650L);
        assertTrue(hit.isPresent(), "缺项时仍应返回 floor 项，绝不能返回 NONE");
        assertEquals(301L, hit.lsn(), "返回的应是剩余项中的最大者");
    }

    @Test
    @DisplayName("重复或回退的 LSN 被忽略，不污染二分")
    void nonIncreasingLsnIsIgnored() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        idx.append(10L, 0L);
        idx.append(10L, 320L);   // 重复
        idx.append(5L, 640L);    // 回退
        idx.append(20L, 960L);
        assertEquals(2, idx.size(), "只应接受 10 与 20");
        assertEquals(10L, idx.lookup(15L).lsn());
        assertEquals(0L, idx.lookup(15L).fileOffset(), "重复项不能覆盖首项偏移");
    }

    // ==================================================================
    // 2. 持久化
    // ==================================================================

    @Test
    @DisplayName(".idx 落盘 → 重新打开后能加载并正确查找")
    void persistsAndReloads(@TempDir Path dir) throws IOException {
        Path idxPath = SegmentNaming.idxPath(dir, 1, 1000L);
        SparseIndex idx = new SparseIndex(idxPath, 16L << 20);
        idx.openSegment(1, 1000L, 8L << 20);
        for (int i = 0; i < 500; i++) {
            idx.append(1000L + (long) i * 10L, (long) i * 320L);
        }
        idx.force();
        assertEquals(500, idx.size());
        assertEquals(500L, idx.fileItemCount());
        idx.close();

        assertTrue(Files.exists(idxPath), "索引文件必须落盘");
        assertEquals(SparseIndex.HEADER_LEN + 500L * SparseIndex.ENTRY_LEN,
                Files.size(idxPath), "文件长度 = 32B 头 + 500 × 16B");
        assertEquals(32L, SparseIndex.HEADER_LEN);

        SparseIndex reloaded = new SparseIndex(idxPath, 16L << 20);
        reloaded.openSegment(1, 1000L, 8L << 20);
        assertFalse(reloaded.needsRebuild(), "头部合法的索引不该要求重建");
        assertEquals(500, reloaded.size(), "重新打开后必须把条目加载回堆内");
        assertEquals(1000L, reloaded.segmentFirstLsn());
        IndexHit hit = reloaded.lookup(1000L + 250L * 10L);
        assertTrue(hit.isPresent());
        assertEquals(1000L + 250L * 10L, hit.lsn());
        assertEquals(250L * 320L, hit.fileOffset(), "重载后偏移必须与写入时一致");
        reloaded.close();
    }

    @Test
    @DisplayName(".idx 头部 magic 被破坏 → needsRebuild=true（不抛异常）")
    void corruptedMagicTriggersRebuildSignal(@TempDir Path dir) throws IOException {
        Path idxPath = SegmentNaming.idxPath(dir, 1, 1L);
        SparseIndex idx = new SparseIndex(idxPath, 16L << 20);
        idx.openSegment(1, 1L, 8L << 20);
        for (int i = 0; i < 10; i++) {
            idx.append(1L + i, i * 320L);
        }
        idx.force();
        idx.close();

        corrupt(idxPath, 0, 0xDEADBEEF); // 破坏 magic

        SparseIndex bad = new SparseIndex(idxPath, 16L << 20);
        bad.openSegment(1, 1L, 8L << 20);
        assertTrue(bad.needsRebuild(),
                "magic 错必须给出重建信号（否则会用错误的偏移去读数据，读到垃圾帧）");
        assertEquals(SparseIndex.ID, bad.id());
        bad.close();
    }

    @Test
    @DisplayName(".idx 头部 CRC 被破坏 → needsRebuild=true")
    void corruptedHeaderCrcTriggersRebuildSignal(@TempDir Path dir) throws IOException {
        Path idxPath = SegmentNaming.idxPath(dir, 1, 1L);
        SparseIndex idx = new SparseIndex(idxPath, 16L << 20);
        idx.openSegment(1, 1L, 8L << 20);
        for (int i = 0; i < 10; i++) {
            idx.append(1L + i, i * 320L);
        }
        idx.force();
        idx.close();

        corrupt(idxPath, 8, 12_345_678); // itemCount/crc 前的字段，但 CRC 是对前 24B 算的

        SparseIndex bad = new SparseIndex(idxPath, 16L << 20);
        bad.openSegment(1, 1L, 8L << 20);
        assertTrue(bad.needsRebuild(), "头部 CRC 错必须给出重建信号");
        bad.close();
    }

    @Test
    @DisplayName(".idx 文件长度与 itemCount 不一致 → needsRebuild=true（半截文件）")
    void truncatedIdxFileTriggersRebuildSignal(@TempDir Path dir) throws IOException {
        Path idxPath = SegmentNaming.idxPath(dir, 1, 1L);
        SparseIndex idx = new SparseIndex(idxPath, 16L << 20);
        idx.openSegment(1, 1L, 8L << 20);
        for (int i = 0; i < 20; i++) {
            idx.append(1L + i, i * 320L);
        }
        idx.force();
        idx.close();

        // 直接把文件截短 3 项，头部仍声明 20 项 → 长度校验必须发现
        try (FileChannel ch = FileChannel.open(idxPath, StandardOpenOption.WRITE)) {
            ch.truncate(SparseIndex.HEADER_LEN + 17L * SparseIndex.ENTRY_LEN);
        }

        SparseIndex bad = new SparseIndex(idxPath, 16L << 20);
        bad.openSegment(1, 1L, 8L << 20);
        assertTrue(bad.needsRebuild(),
                "文件比 itemCount 声明的短，必须判定为撕裂并要求重建");
        bad.close();
    }

    // ==================================================================
    // 3. 截断与重建
    // ==================================================================

    @Test
    @DisplayName("truncateAfter：丢弃 > watermark 的项，无越界，lastIndexedLsn 正确")
    void truncateAfterKeepsPrefixAndDoesNotOverflow() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        for (int i = 0; i < 100; i++) {
            idx.append(1L + i * 10L, i * 320L);
        }
        idx.truncateAfter(501L);
        assertEquals(51, idx.size(), "1,11,...,501 共 51 项");
        assertEquals(501L, idx.lastIndexedLsn());
        assertEquals(501L, idx.lookup(505L).lsn());

        // 截断到空
        idx.truncateAfter(0L);
        assertEquals(0, idx.size());
        assertEquals(0L, idx.lastIndexedLsn());
        assertEquals(IndexHit.NONE, idx.lookup(1L));

        // 空索引上截断是幂等的，不抛异常
        idx.truncateAfter(0L);
        idx.truncateAfter(Long.MAX_VALUE);
        assertEquals(0, idx.size());
    }

    @Test
    @DisplayName("truncateBeyondOffset：按文件偏移截断（与 .wal 截断联动）")
    void truncateBeyondOffset() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        for (int i = 0; i < 10; i++) {
            idx.append(1L + i, i * 320L);
        }
        idx.truncateBeyondOffset(320L * 4L);
        assertEquals(5, idx.size(), "偏移 0,320,...,1280 共 5 项");
        assertEquals(5L, idx.lastIndexedLsn());

        idx.truncateBeyondOffset(0L);
        assertEquals(1, idx.size(), "只剩偏移 0 的项");
        idx.truncateBeyondOffset(-1L);
        assertEquals(0, idx.size());
    }

    @Test
    @DisplayName("rebuild：从迭代器全量重建，覆盖旧内容")
    void rebuildFromIterator() {
        SparseIndex idx = new SparseIndex();
        idx.openSegment(0, 1L, 0L);
        for (int i = 0; i < 50; i++) {
            idx.append(1L + i, i * 320L);
        }
        assertEquals(50, idx.size());

        List<IndexEntry> fresh = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            fresh.add(new IndexEntry(1000L + i * 5L, i * 64L));
        }
        idx.rebuild(fresh.iterator());
        assertEquals(7, idx.size(), "重建后条目数必须等于源长度");
        assertEquals(1000L, idx.lookup(1000L).lsn());
        assertEquals(1030L, idx.lookup(9999L).lsn());
        assertEquals(6L * 64L, idx.lookup(9999L).fileOffset());

        // 空源也应可用
        idx.rebuild(new ArrayList<IndexEntry>().iterator());
        assertEquals(0, idx.size());
        assertEquals(IndexHit.NONE, idx.lookup(1L));
    }

    // ==================================================================
    // 4. 堆预算降级
    // ==================================================================

    @Test
    @DisplayName("堆预算耗尽后 heapDisabled=true、memoryBytes()=0，查找改走文件且仍正确")
    void heapBudgetExhaustionDegradesToFileLookup(@TempDir Path dir) throws IOException {
        // 只给 1KB 堆预算 → 最多 64 项（16B/项）
        Path idxPath = SegmentNaming.idxPath(dir, 1, 1L);
        SparseIndex idx = new SparseIndex(idxPath, 1024L);
        idx.openSegment(1, 1L, 8L << 20);
        for (int i = 0; i < 200; i++) {
            idx.append(1L + (long) i * 10L, (long) i * 320L);
        }
        assertTrue(idx.heapDisabled(), "超过 maxHeapBytes 后必须关闭堆内数组");
        assertEquals(0L, idx.memoryBytes(), "降级后堆内占用必须归零（预算硬上限）");
        assertEquals(200, idx.size(), "降级只影响内存，索引项本身必须仍全部保留");
        assertEquals(200L, idx.fileItemCount(), "文件里必须有全部 200 项");

        IndexHit hit = idx.lookup(1L + 150L * 10L);
        assertTrue(hit.isPresent(), "降级后查找必须仍能命中");
        assertEquals(1L + 150L * 10L, hit.lsn());
        assertEquals(150L * 320L, hit.fileOffset());
        assertEquals(1L + 199L * 10L, idx.lastIndexedLsn());
        idx.close();

        // 重新打开时预算仍然很小 → 立即降级，但能读回全部项
        SparseIndex again = new SparseIndex(idxPath, 1024L);
        again.openSegment(1, 1L, 8L << 20);
        assertEquals(200L, again.fileItemCount());
        assertTrue(again.lookup(1L + 199L * 10L).isPresent());
        again.close();
    }

    // ==================================================================
    // 5. DefaultIndexStore（多段路由）
    // ==================================================================

    @Test
    @DisplayName("DefaultIndexStore：按段路由、内存求和、关闭单段")
    void defaultIndexStoreRoutesPerSegment(@TempDir Path dir) {
        DefaultIndexStore store = new DefaultIndexStore(dir, 16L << 20);
        assertEquals(SparseIndex.ID, store.id());
        assertEquals(0L, store.memoryBytes());

        // 第一段：索引 2 条
        store.openSegment(0, 1L, 8L << 20);
        store.append(1L, 0L);
        store.append(2L, 320L);
        assertEquals(2L, store.lastIndexedLsn());
        assertEquals(1L, store.currentSegmentFirstLsn());

        // 轮转到第二段：append 落在第二段，第一段保持不变
        store.openSegment(0, 1000L, 8L << 20);
        store.append(1000L, 0L);
        assertEquals(1000L, store.lastIndexedLsn(), "append 必须路由到当前活跃段");
        assertEquals(1000L, store.currentSegmentFirstLsn());
        assertEquals(2, store.segmentCount());

        SparseIndex first = store.indexFor(1L);
        SparseIndex second = store.indexFor(1000L);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(2, first.size(), "第一段应保留自己的 2 项");
        assertEquals(1, second.size(), "第二段应只有 1 项");

        // 切回第一段：从 .idx 重新加载后继续追加
        store.openSegment(0, 1L, 8L << 20);
        assertEquals(2, store.indexFor(1L).size(), "重新打开应加载回 2 项");
        store.append(3L, 640L);
        assertEquals(3L, store.lastIndexedLsn());
        assertEquals(3, store.indexFor(1L).size());

        assertTrue(store.memoryBytes() > 0L, "内存占用必须把多段求和");
        assertTrue(Files.exists(SegmentNaming.idxPath(dir, 0, 1L)));
        assertTrue(Files.exists(SegmentNaming.idxPath(dir, 0, 1000L)));

        store.closeSegment(1L);
        assertEquals(1, store.segmentCount());
        store.close();
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    private static void corrupt(Path path, int offset, int value) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(value);
            buf.flip();
            int written = 0;
            while (buf.hasRemaining()) {
                written += ch.write(buf, offset + written);
            }
        }
    }

    /** 保留 Iterator 导入的显式使用，避免"未使用导入"的误读。 */
    @SuppressWarnings("unused")
    private static Iterator<IndexEntry> emptyIterator() {
        return new ArrayList<IndexEntry>().iterator();
    }
}
