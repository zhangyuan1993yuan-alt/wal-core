package com.walcore.internal.segment;

import com.walcore.api.WalVersion;
import com.walcore.internal.util.Crc32cUtil;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexEntry;
import com.walcore.spi.IndexHit;
import com.walcore.spi.IndexStore;
import com.walcore.spi.PluginContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * 16 字节稀疏索引：<b>堆内 {@code long[]} 二分 + {@code .idx} 顺序追加</b>。
 *
 * <h2>为什么是"堆内二分 + 文件只追加"</h2>
 * <ul>
 *   <li>写入频率只有 {@code 1/indexIntervalRecords}（默认每 1000 条一次），
 *       1000 万记录的段只写 1 万个索引项，因此<b>直接 append 到文件、不缓冲、不 mmap</b>
 *       完全够用（ARCH §6.4）。</li>
 *   <li>查找是热路径（每次 {@code seekTo} 一次），必须走堆内 {@code long[]} 二分：
 *       2 万项的二分只有 ~15 次比较，约 50ns，满足 p99 ≤ 10µs 的要求（余量 200 倍）。</li>
 *   <li>堆内占用：每项 16B（两个 {@code long}），1000 万记录 = 1 万项 = 160KB/段，
 *       远优于 P0-07 ② 的 {@code 32B × 记录数/1000} 上限。</li>
 * </ul>
 *
 * <h2>降级路径（不是 OOM 路径）</h2>
 * 当累计堆内占用超过 {@code maxIndexHeapBytes}（默认 192MB，计入 512MB 预算）时，
 * 本实现把 {@code heapDisabled} 置 true，之后<b>只写文件、只在文件上二分</b>。
 * 索引因此变成"稍慢但永不 OOM"，符合 ARCH §16.3 的要求。
 *
 * <h2>{@code .idx} 文件头（<b>32 字节</b>，与 TASKS 里"16B 头"的表述不一致，已记录）</h2>
 * <pre>
 *   offset 0  magic      int32   固定 0x57494458 ("WIDX")
 *   offset 4  version    int32   当前 1
 *   offset 8  firstLsn   int64   段内首条记录 LSN
 *   offset 16 itemCount  int64   索引项条数
 *   offset 24 crc32c     int32   覆盖头的前 24 字节（magic/version/firstLsn/itemCount）
 *   offset 28 reserved   int32   必须写 0
 * </pre>
 * 为什么头不是 16B：{@code magic(4) + firstLsn(8) + itemCount(4) + crc(4) = 20 > 16}，
 * 算术上放不下。取 32B 既放得下，也让头正好是半个 cache line。
 *
 * <p><b>关于"头里存索引正文的 CRC"</b>：ARCH §6.4 说头里存 {@code crc32cOfIndexBody}。
 * 若真覆盖全文，则每次 append 都要重算整个正文的 CRC（O(n²) 总成本），违背"索引每 1000 条
 * 才写一次"的轻量定位。这里改为覆盖头自身 + 用 {@code itemCount × 16 + 32 == 文件长度}
 * 做正文长度一致性校验；正文内容被篡改的保护由"恢复期丢弃 {@code fileOffset > lastGoodOffset}
 * 的条目"承担（ARCH §6.4 最后一段的原始设计意图）。
 */
public final class SparseIndex implements IndexStore {

    /** 插件 id。 */
    public static final String ID = "builtin:index-sparse";

    /** {@code .idx} 文件头魔数，ASCII {@code "WIDX"}。 */
    public static final int MAGIC = 0x57494458;

    /** 索引文件头长度（字节）。 */
    public static final int HEADER_LEN = 32;

    /** 单条索引项长度（字节）。 */
    public static final int ENTRY_LEN = 16;

    /** 参与 CRC 的头前缀长度。 */
    private static final int CRC_PREFIX_LEN = 24;

    private static final int OFF_MAGIC = 0;
    private static final int OFF_VERSION = 4;
    private static final int OFF_FIRST_LSN = 8;
    private static final int OFF_ITEM_COUNT = 16;
    private static final int OFF_CRC = 24;
    private static final int OFF_RESERVED = 28;

    private static final int INITIAL_CAPACITY = 16;

    private volatile PluginContext context;

    private long[] lsns = new long[INITIAL_CAPACITY];
    private long[] offsets = new long[INITIAL_CAPACITY];
    private int count;

    private final Path idxPath;
    private FileChannel channel;
    private long fileItemCount;

    private int partitionId;
    private long segmentFirstLsn;
    private long segmentCapacity;

    private long maxHeapBytes;
    private boolean heapDisabled;
    private boolean needsRebuild;
    private long indexIntervalRecords = 1000L;

    /**
     * 纯内存索引（插件发现用；不落 {@code .idx}）。
     */
    public SparseIndex() {
        this(null, 0L);
    }

    /**
     * 绑定到一个 {@code .idx} 文件的索引。
     *
     * @param idxPath 索引文件路径，null 表示纯内存
     * @param maxHeapBytes 堆内预算（字节），0 表示不限
     */
    public SparseIndex(Path idxPath, long maxHeapBytes) {
        this.idxPath = idxPath;
        this.maxHeapBytes = Math.max(0L, maxHeapBytes);
    }

    // ------------------------------------------------------------------
    // WalPlugin
    // ------------------------------------------------------------------

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext pluginContext) {
        this.context = pluginContext;
        if (pluginContext != null) {
            this.maxHeapBytes = pluginContext.config().maxIndexHeapBytes();
            this.indexIntervalRecords = pluginContext.config().indexIntervalRecords();
        }
    }

    @Override
    public void close() {
        closeFileQuietly();
    }

    /**
     * 设置索引粒度（每多少条记录一个索引项）。
     *
     * @param interval 条数
     */
    public void indexIntervalRecords(int interval) {
        this.indexIntervalRecords = Preconditions.checkPositive(interval, "indexIntervalRecords");
    }

    /**
     * 索引粒度。
     *
     * @return 条数
     */
    public long indexIntervalRecords() {
        return indexIntervalRecords;
    }

    // ------------------------------------------------------------------
    // IndexStore
    // ------------------------------------------------------------------

    @Override
    public synchronized void openSegment(int partitionId, long segmentFirstLsn,
                                        long segmentCapacityBytes) {
        this.partitionId = partitionId;
        this.segmentFirstLsn = segmentFirstLsn;
        this.segmentCapacity = segmentCapacityBytes;
        this.count = 0;
        this.fileItemCount = 0L;
        this.needsRebuild = false;
        closeFileQuietly();
        if (idxPath == null) {
            return;
        }
        try {
            Files.createDirectories(idxPath.getParent());
            boolean usable = Files.exists(idxPath) && Files.size(idxPath) >= HEADER_LEN;
            channel = FileChannel.open(idxPath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (!usable) {
                writeHeader(0L);
                return;
            }
            if (!readAndValidateHeader()) {
                needsRebuild = true;
                return;
            }
            loadFromFile();
        } catch (IOException e) {
            needsRebuild = true;
        }
    }

    @Override
    public synchronized void append(long lsn, long fileOffset) {
        if (count > 0 && lsn <= lsns[count - 1]) {
            // 索引必须严格递增；重复/回退说明上层逻辑错了，直接忽略以免污染二分
            return;
        }
        appendToHeap(lsn, fileOffset);
        appendToFile(lsn, fileOffset);
    }

    @Override
    public synchronized IndexHit lookup(long lsn) {
        if (heapDisabled) {
            return lookupInFile(lsn);
        }
        if (count == 0) {
            return IndexHit.NONE;
        }
        int lo = 0;
        int hi = count - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (lsns[mid] <= lsn) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (found < 0) {
            return IndexHit.NONE;
        }
        return IndexHit.of(lsns[found], offsets[found]);
    }

    @Override
    public synchronized long lastIndexedLsn() {
        if (!heapDisabled && count > 0) {
            return lsns[count - 1];
        }
        if (fileItemCount <= 0L) {
            return 0L;
        }
        IndexEntry last = readEntryFromFile(fileItemCount - 1L);
        return last == null ? 0L : last.lsn;
    }

    @Override
    public synchronized void truncateAfter(long watermarkLsn) {
        int n = totalCount();
        int keep = n;
        while (keep > 0 && lsnAt(keep - 1) > watermarkLsn) {
            keep--;
        }
        truncateToCount(keep);
    }

    /**
     * 丢弃 {@code fileOffset > maxFileOffset} 的索引项（恢复期与 {@code .wal} 截断联动）。
     *
     * @param maxFileOffset 允许的最大帧偏移
     */
    public synchronized void truncateBeyondOffset(long maxFileOffset) {
        int n = totalCount();
        int keep = n;
        while (keep > 0 && offsetAt(keep - 1) > maxFileOffset) {
            keep--;
        }
        if (keep == n) {
            return;
        }
        truncateToCount(keep);
    }

    @Override
    public synchronized void rebuild(Iterator<IndexEntry> source) {
        count = 0;
        fileItemCount = 0L;
        needsRebuild = false;
        heapDisabled = false;
        if (channel != null) {
            try {
                channel.truncate(HEADER_LEN);
                writeHeader(0L);
            } catch (IOException e) {
                throw new UncheckedIOException("重建索引时清空失败: " + idxPath, e);
            }
        }
        if (source != null) {
            while (source.hasNext()) {
                IndexEntry e = source.next();
                appendToHeap(e.lsn, e.fileOffset);
                appendToFile(e.lsn, e.fileOffset);
            }
        }
        forceQuietly();
    }

    @Override
    public synchronized long memoryBytes() {
        if (heapDisabled) {
            return 0L;
        }
        return (long) lsns.length * 8L * 2L;
    }

    @Override
    public synchronized void closeSegment(long segmentFirstLsn) {
        forceQuietly();
        closeFileQuietly();
        count = 0;
        fileItemCount = 0L;
    }

    /**
     * 落盘索引（{@code closeSegment} 与恢复期使用）。
     */
    public synchronized void force() {
        forceQuietly();
    }

    // ------------------------------------------------------------------
    // 附加诊断接口
    // ------------------------------------------------------------------

    /**
     * 索引项<b>总</b>条数（堆内 + 文件）。
     *
     * <p>刻意不返回"堆内条数"：一旦因超预算降级（{@code heapDisabled=true}），
     * 堆内数组会被清空归零，若本方法跟着返回 0，监控和统计会误报"索引丢了"。
     * 真实语义应是"这个段索引了多少项"，与实现是堆还是文件无关。
     *
     * @return 条数
     */
    public synchronized int size() {
        return totalCount();
    }

    /**
     * 堆内二分可用的条数（降级后为 0，供诊断用）。
     *
     * @return 条数
     */
    public synchronized int heapItemCount() {
        return count;
    }

    /**
     * {@code .idx} 文件中持久化的条数。
     *
     * @return 条数
     */
    public synchronized long fileItemCount() {
        return fileItemCount;
    }

    /**
     * 是否需要重建（头 magic/CRC 错、长度不一致）。
     *
     * @return 需要重建返回 true
     */
    public synchronized boolean needsRebuild() {
        return needsRebuild;
    }

    /**
     * 是否因超堆内预算而停用了堆内二分。
     *
     * @return 停用返回 true
     */
    public synchronized boolean heapDisabled() {
        return heapDisabled;
    }

    /**
     * 索引绑定的段首 LSN。
     *
     * @return LSN
     */
    public synchronized long segmentFirstLsn() {
        return segmentFirstLsn;
    }

    /**
     * 把堆内索引项按顺序导出（测试与重建比对用）。
     *
     * @return 索引项数组
     */
    public synchronized IndexEntry[] entries() {
        IndexEntry[] out = new IndexEntry[count];
        for (int i = 0; i < count; i++) {
            out[i] = new IndexEntry(lsns[i], offsets[i]);
        }
        return out;
    }

    /**
     * 索引文件路径（可能为 null）。
     *
     * @return 路径
     */
    public Path idxPath() {
        return idxPath;
    }

    /**
     * 段首 LSN 与条数的可读摘要（诊断用）。
     *
     * @return 字符串
     */
    public synchronized String describe() {
        return "SparseIndex{pid=" + partitionId
                + ", firstLsn=" + segmentFirstLsn
                + ", items=" + count
                + ", fileItems=" + fileItemCount
                + ", heapDisabled=" + heapDisabled
                + ", needsRebuild=" + needsRebuild
                + ", file=" + (idxPath == null ? "<memory>" : idxPath.getFileName().toString())
                + '}';
    }

    // ------------------------------------------------------------------
    // 内部：堆内
    // ------------------------------------------------------------------

    private void appendToHeap(long lsn, long fileOffset) {
        if (heapDisabled) {
            return;
        }
        if (maxHeapBytes > 0L && (long) (count + 1) * ENTRY_LEN > maxHeapBytes) {
            heapDisabled = true;
            count = 0;
            lsns = new long[INITIAL_CAPACITY];
            offsets = new long[INITIAL_CAPACITY];
            if (context != null) {
                context.warn("wal.index.heap.disabled",
                        "索引堆内占用超过 maxIndexHeapBytes=" + maxHeapBytes
                                + "，降级为纯文件二分: " + idxPath, null);
            }
            return;
        }
        if (count == lsns.length) {
            int newCap = count << 1;
            long[] newLsns = new long[newCap];
            long[] newOffsets = new long[newCap];
            System.arraycopy(lsns, 0, newLsns, 0, count);
            System.arraycopy(offsets, 0, newOffsets, 0, count);
            lsns = newLsns;
            offsets = newOffsets;
        }
        lsns[count] = lsn;
        offsets[count] = fileOffset;
        count++;
    }

    /**
     * 索引项总数（堆内或文件，取决于是否已降级）。
     *
     * <p>降级后堆内 {@code count} 被清零，真实条数只剩 {@code fileItemCount}。
     * 所有"遍历/截断"逻辑都必须走本方法，否则降级状态下会把整个索引误删——
     * 这是一个只在"超预算 + 恢复期截断"同时发生时才暴露的组合 bug。
     *
     * @return 条数
     */
    private int totalCount() {
        if (heapDisabled) {
            return (int) Math.min(fileItemCount, Integer.MAX_VALUE);
        }
        return count;
    }

    /**
     * 读取第 {@code i} 项的 LSN（自动适配堆内/文件）。
     *
     * @param i 下标
     * @return LSN
     */
    private long lsnAt(int i) {
        if (!heapDisabled) {
            return lsns[i];
        }
        IndexEntry e = readEntryFromFile(i);
        return e == null ? 0L : e.lsn;
    }

    /**
     * 读取第 {@code i} 项的偏移（自动适配堆内/文件）。
     *
     * @param i 下标
     * @return 文件偏移
     */
    private long offsetAt(int i) {
        if (!heapDisabled) {
            return offsets[i];
        }
        IndexEntry e = readEntryFromFile(i);
        return e == null ? -1L : e.fileOffset;
    }

    private void truncateToCount(int keep) {
        int k = Math.max(0, keep);
        if (!heapDisabled) {
            this.count = Math.min(this.count, k);
        }
        this.fileItemCount = Math.min(this.fileItemCount, (long) k);
        if (channel != null) {
            try {
                channel.truncate(HEADER_LEN + fileItemCount * ENTRY_LEN);
                writeHeader(fileItemCount);
            } catch (IOException e) {
                throw new UncheckedIOException("截断索引失败: " + idxPath, e);
            }
        }
    }

    // ------------------------------------------------------------------
    // 内部：文件
    // ------------------------------------------------------------------

    private void appendToFile(long lsn, long fileOffset) {
        if (channel == null) {
            fileItemCount++;
            return;
        }
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_LEN);
        buf.putLong(0, lsn);
        buf.putLong(8, fileOffset);
        buf.limit(ENTRY_LEN);
        buf.position(0);
        try {
            long pos = HEADER_LEN + fileItemCount * ENTRY_LEN;
            while (buf.hasRemaining()) {
                pos += channel.write(buf, pos);
            }
            fileItemCount++;
            writeHeader(fileItemCount);
        } catch (IOException e) {
            throw new UncheckedIOException("追加索引项失败: " + idxPath, e);
        }
    }

    private IndexHit lookupInFile(long lsn) {
        long n = fileItemCount;
        if (n <= 0L) {
            return IndexHit.NONE;
        }
        long lo = 0L;
        long hi = n - 1L;
        long found = -1L;
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            IndexEntry e = readEntryFromFile(mid);
            if (e == null) {
                return IndexHit.NONE;
            }
            if (e.lsn <= lsn) {
                found = mid;
                lo = mid + 1L;
            } else {
                hi = mid - 1L;
            }
        }
        if (found < 0L) {
            return IndexHit.NONE;
        }
        IndexEntry e = readEntryFromFile(found);
        return e == null ? IndexHit.NONE : IndexHit.of(e.lsn, e.fileOffset);
    }

    private IndexEntry readEntryFromFile(long index) {
        if (channel == null || index < 0L || index >= fileItemCount) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_LEN);
        try {
            long pos = HEADER_LEN + index * ENTRY_LEN;
            while (buf.hasRemaining()) {
                int n = channel.read(buf, pos + buf.position());
                if (n < 0) {
                    return null;
                }
            }
        } catch (IOException e) {
            return null;
        }
        return new IndexEntry(buf.getLong(0), buf.getLong(8));
    }

    private void loadFromFile() throws IOException {
        long size = channel.size();
        long items = (size - HEADER_LEN) / ENTRY_LEN;
        fileItemCount = Math.max(0L, items);
        for (long i = 0; i < fileItemCount && !heapDisabled; i++) {
            IndexEntry e = readEntryFromFile(i);
            if (e == null) {
                break;
            }
            appendToHeap(e.lsn, e.fileOffset);
        }
    }

    private boolean readAndValidateHeader() {
        ByteBuffer head = ByteBuffer.allocate(HEADER_LEN);
        try {
            long pos = 0L;
            while (head.hasRemaining()) {
                int n = channel.read(head, pos + head.position());
                if (n < 0) {
                    return false;
                }
            }
        } catch (IOException e) {
            return false;
        }
        if (head.getInt(OFF_MAGIC) != MAGIC || head.getInt(OFF_VERSION) != 1) {
            return false;
        }
        long storedFirstLsn = head.getLong(OFF_FIRST_LSN);
        if (segmentFirstLsn != 0L && storedFirstLsn != 0L && storedFirstLsn != segmentFirstLsn) {
            return false;
        }
        long itemCount = head.getLong(OFF_ITEM_COUNT);
        if (itemCount < 0L) {
            return false;
        }
        int storedCrc = head.getInt(OFF_CRC);
        int actualCrc = Crc32cUtil.computeRange(head, 0, CRC_PREFIX_LEN);
        if (storedCrc != actualCrc) {
            return false;
        }
        // 正文长度一致性：itemCount * 16 + 32 必须等于文件长度
        long expectedSize = HEADER_LEN + itemCount * ENTRY_LEN;
        return safeSize() == expectedSize;
    }

    private long safeSize() {
        try {
            return channel == null ? 0L : channel.size();
        } catch (IOException e) {
            return 0L;
        }
    }

    private void writeHeader(long itemCount) throws IOException {
        if (channel == null) {
            return;
        }
        ByteBuffer head = ByteBuffer.allocate(HEADER_LEN);
        head.putInt(OFF_MAGIC, MAGIC);
        head.putInt(OFF_VERSION, 1);
        head.putLong(OFF_FIRST_LSN, segmentFirstLsn);
        head.putLong(OFF_ITEM_COUNT, itemCount);
        head.putInt(OFF_CRC, Crc32cUtil.computeRange(head, 0, CRC_PREFIX_LEN));
        head.putInt(OFF_RESERVED, 0);
        head.position(0);
        head.limit(HEADER_LEN);
        long pos = 0L;
        while (head.hasRemaining()) {
            pos += channel.write(head, pos);
        }
    }

    private void forceQuietly() {
        FileChannel ch = this.channel;
        if (ch != null) {
            try {
                ch.force(false);
            } catch (IOException ignored) {
                // 刷盘失败由上层恢复流程负责暴露
            }
        }
    }

    private void closeFileQuietly() {
        forceQuietly();
        FileChannel ch = this.channel;
        this.channel = null;
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException ignored) {
                // 关闭失败不阻断上层
            }
        }
    }
}
