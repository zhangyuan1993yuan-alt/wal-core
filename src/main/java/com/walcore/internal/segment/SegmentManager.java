package com.walcore.internal.segment;

import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.append.LsnAllocator;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexStore;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 段生命周期管理中心：<b>创建 / 轮转 / 查找 / 冻结 / 删除</b>（ARCH §6）。
 *
 * <h2>为什么"查找"必须是 O(log n) 且无锁</h2>
 * 每次 {@code seekTo(lsn)}（游标、tailer、恢复校验）都要定位段。若线性扫段列表，
 * 1 万个段时就是 1 万次比较，直接违反 PRD P0-02 的"<10µs 定位"。
 * 这里为每个分区维护一份 {@code volatile long[] firstLsns}（与 {@code Segment[]} 平行），
 * 二分时只碰纯 long 数组，缓存友好；数组整体 COW 替换，读者只做一次 volatile 读。
 *
 * <h2>为什么把 firstLsns 单独存一份而不读 {@code Segment.firstLsn()}</h2>
 * 遍历 {@code Segment[]} 并调用 getter 会产生 <i>指针追逐</i>（每个元素跳到另一个对象），
 * 1000 个段时 cache miss 概率接近 100%，实测慢 5~10 倍。平行 long 数组是紧凑的，
 * 1000 个 long = 8KB，几乎全在 L2 里。这是"用一点冗余换 10 倍查找速度"的典型取舍。
 *
 * <h2>轮转的临界区边界</h2>
 * 轮转必须在<b>分区写锁内</b>完成（{@code ACTIVE -> READONLY} 与 {@code active} 替换
 * 必须原子，否则两个写线程可能各自轮转一次、生成两个空段）。
 * 因此对外暴露两个方法：
 * <ul>
 *   <li>{@link #rotate(int)}：自己加锁，供运维 {@code wal.rotate(pid)} 与外部调用；</li>
 *   <li>{@link #rotateLocked(int)}：调用方已持锁，供 {@code AppendPipeline} 热路径使用。</li>
 * </ul>
 *
 * <h2>降级</h2>
 * 预建段不可用时内联创建，并 {@code wal.rotate.fallback} 加 1。这是全链路唯一
 * 允许的超 1ms 尖刺，必须可见（TASKS 铁律 7）。
 */
public final class SegmentManager implements AutoCloseable {

    /** 空段数组常量，避免空分配。 */
    private static final Segment[] EMPTY_SEGMENTS = new Segment[0];

    /** 空 firstLsn 数组常量。 */
    private static final long[] EMPTY_LSNS = new long[0];

    private final StorageBackend backend;
    private final PartitionRegistry registry;
    private final Path baseDir;
    private final long segmentSizeBytes;
    private final long segmentMaxRecords;
    private final long segmentMaxAgeMillis;
    private final boolean timeBasedRotation;
    private final LsnAllocator lsnAllocator;
    private final IndexStore indexStore;
    private final MetricsRegistry metrics;
    private final boolean forceParentDirOnCreate;

    private final NextSegmentPreparer preparer;
    private final Map<Integer, PartitionSegments> partitions = new ConcurrentHashMap<>();

    /** 每个分区的"名字 LSN"预定计数器，保证并发创建段时名字不重复。 */
    private final Map<Integer, AtomicLong> nameReservations = new ConcurrentHashMap<>();

    private final AtomicLong fallbackCount = new AtomicLong();
    private final AtomicLong rotationCount = new AtomicLong();
    private final AtomicLong createdCount = new AtomicLong();

    private volatile boolean closed;

    /**
     * 构造段管理器（不启动预建线程）。
     *
     * @param backend 存储后端
     * @param registry 分区注册表
     * @param baseDir 日志根目录
     * @param config 全局配置（读取段大小/记录数/时长/预建开关）
     * @param lsnAllocator LSN 分配器（预建段命名用 {@code peek()}）
     * @param indexStore 索引存储（可为 null，此时段不挂索引，纯顺序扫）
     * @param metrics 指标（可为 null）
     */
    public SegmentManager(StorageBackend backend, PartitionRegistry registry, Path baseDir,
                          WalConfig config, LsnAllocator lsnAllocator, IndexStore indexStore,
                          MetricsRegistry metrics) {
        this.backend = Preconditions.checkNotNull(backend, "backend");
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.baseDir = Preconditions.checkNotNull(baseDir, "baseDir");
        WalConfig cfg = Preconditions.checkNotNull(config, "config");
        this.lsnAllocator = Preconditions.checkNotNull(lsnAllocator, "lsnAllocator");
        this.indexStore = indexStore;
        this.metrics = metrics;
        this.segmentSizeBytes = cfg.segmentSizeBytes();
        this.segmentMaxRecords = cfg.segmentMaxRecords();
        this.segmentMaxAgeMillis = cfg.segmentMaxAgeMillis();
        this.timeBasedRotation = cfg.timeBasedRotation();
        this.forceParentDirOnCreate = cfg.forceParentDirOnCreate();
        this.preparer = new NextSegmentPreparer(backend, segmentSizeBytes, this::reserveNameLsn,
                metrics, "wal");
    }

    /**
     * 启动后台预建线程。
     */
    public void start() {
        preparer.start();
    }

    // ------------------------------------------------------------------
    // 段创建 / 打开
    // ------------------------------------------------------------------

    /**
     * 为分区创建第一个段（{@code addPartition} 与全新 {@code open} 时调用）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN（通常取 {@code lsnAllocator.peek()}）
     * @return 新段
     * @throws IOException 创建失败
     */
    public Segment createFirstSegment(int partitionId, long firstLsn) throws IOException {
        Preconditions.checkArgument(partitionId >= 0, "partitionId 不得为负: %s", partitionId);
        Preconditions.checkArgument(firstLsn > 0L, "firstLsn 必须为正: %s", firstLsn);
        preparer.resume(partitionId);
        PartitionSegments ps = partitionHolder(partitionId);
        synchronized (ps.mutex) {
            if (ps.active != null) {
                throw new WalException.PartitionException(
                        "分区 " + partitionId + " 已有活跃段，不能重复创建首段");
            }
            Segment seg = createSegment(partitionId, firstLsn);
            observeName(partitionId, firstLsn);
            attachToHolder(ps, seg);
            warmUp(partitionId);
            return seg;
        }
    }

    /**
     * 为分区再开一个新段（<b>不要求</b>已有活跃段）。
     *
     * <h2>为什么不能直接用 {@link #createFirstSegment}</h2>
     * {@code createFirstSegment} 的用途是"分区第一次建段"，它拿到的是
     * {@code lsnAllocator.peek()}——一个<b>真实 LSN</b>。但崩溃恢复后旧段全部被封存、
     * 活跃指针被清空（见 {@link #clearActive}），此时"再开一段"必须用
     * {@link #reserveNameLsn} 发号：{@code peek()} 很可能等于**已存在的旧段名**，
     * 于是 {@code .tmp → .wal} 的原子改名会覆盖一个仍被映射的旧文件，
     * Windows 上直接 {@code AccessDeniedException}（这就是"崩一次能起来、崩两次起不来"）。
     *
     * @param partitionId 分区 ID
     * @return 新的活跃段
     * @throws IOException 创建失败
     */
    public Segment createNextSegment(int partitionId) throws IOException {
        PartitionSegments ps = partitionHolder(partitionId);
        synchronized (ps.mutex) {
            preparer.resume(partitionId);
            Segment seg = createSegment(partitionId, reserveNameLsn(partitionId));
            attachToHolder(ps, seg);
            warmUp(partitionId);
            return seg;
        }
    }

    /**
     * 打开既有段（恢复期使用）。
     *
     * <p><b>为什么必须显式传 {@code logicalLength}</b>：段文件是 SPARSE 预分配的，
     * 物理长度恒为 {@code segmentSizeBytes}（8KB/256MB），与"已写入多少"完全无关。
     * 只读句柄的 {@code appendPosition()} 只能报告物理长度，若拿它当封存长度，
     * {@code finalLength} 会变成整个段容量，后续逐帧扫描将扫到一大片零字节。
     * 真实逻辑长度只能由 {@code RecoveryManager} 扫描最后一帧之后得到，
     * 所以这里要求调用方传入；传 {@code -1} 表示"信任句柄自报"（仅适用于
     * 非预分配后端，如堆后端或 {@code PreallocateMode.NONE}）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 从文件名解析的首条记录 LSN
     * @param writable 是否作为活跃可写段打开（仅最后一个段可能为 true）
     * @param logicalLength 已写入的逻辑长度；{@code <0} 表示取 {@code appendPosition()}
     * @return 段
     * @throws IOException 打开失败
     */
    public Segment reopenSegment(int partitionId, long firstLsn, boolean writable,
                                 long logicalLength) throws IOException {
        PartitionSegments ps = partitionHolder(partitionId);
        synchronized (ps.mutex) {
            Path path = SegmentNaming.walPath(baseDir, partitionId, firstLsn);
            SegmentHandle handle = backend.open(path, firstLsn, writable);
            if (writable && logicalLength >= 0L) {
                // 可写句柄的 appendPosition 初值恒为 0（它不知道文件里已有多少字节），
                // 必须把扫描出的逻辑长度显式推回句柄，否则 read() 的上界是 0，
                // 恢复后"水位正确、数据读不出来"。见 SegmentHandle#restoreAppendPosition。
                handle.restoreAppendPosition(logicalLength);
            }
            Segment seg = new Segment(partitionId, firstLsn, handle);
            attachIndex(seg);
            if (!writable) {
                long len = logicalLength >= 0L ? logicalLength : handle.appendPosition();
                seg.sealToReadOnly(len);
            }
            observeName(partitionId, firstLsn);
            attachToHolder(ps, seg);
            return seg;
        }
    }

    /**
     * 打开既有段（逻辑长度取句柄自报，仅适用于非预分配后端）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 首条记录 LSN
     * @param writable 是否可写
     * @return 段
     * @throws IOException 打开失败
     */
    public Segment reopenSegment(int partitionId, long firstLsn, boolean writable)
            throws IOException {
        return reopenSegment(partitionId, firstLsn, writable, -1L);
    }

    /**
     * 创建段（不挂到 holder，供首次创建与内联降级共用）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 名字 LSN
     * @return 新段
     * @throws IOException 创建失败
     */
    private Segment createSegment(int partitionId, long firstLsn) throws IOException {
        SegmentHandle handle = backend.create(partitionId, firstLsn, segmentSizeBytes);
        Segment seg = new Segment(partitionId, firstLsn, handle);
        attachIndex(seg);
        createdCount.incrementAndGet();
        return seg;
    }

    /**
     * 给段挂上稀疏索引（{@code indexStore == null} 或索引实现抛异常时安全降级为"无索引"）。
     *
     * <p>索引是可选的加速层：挂不上只影响 seek 性能，不影响正确性，因此这里
     * <b>吞掉</b>异常并记降级，而不是让 {@code open} 失败。
     *
     * <p><b>已知限制</b>：只有内置的 {@link com.walcore.internal.spiimpl.DefaultIndexStore}
     * 能提供"段内自动写索引项"这条快路径（因为它能给出具体的 {@code SparseIndex} 实例）。
     * 第三方 {@code IndexStore} 插件只会收到 {@code openSegment} 调用，不会自动累积条目——
     * 它的 {@code lookup} 会一直返回 {@code NONE}，读取退化为"从段首顺序扫"。
     * 这是<b>正确但慢</b>的降级，已在 T06/T07 验收记录中登记；要支持插件快路径需要给
     * {@code IndexStore} 加一个"按段 append"的 SPI 方法（属于 T11 的插件化议题）。
     *
     * @param seg 段
     */
    private void attachIndex(Segment seg) {
        if (indexStore == null) {
            return;
        }
        try {
            // 外部索引插件始终收到逐条 onAppended（ARCH 契约修订，T11）
            seg.attachIndexStore(indexStore);
            indexStore.openSegment(seg.partitionId(), seg.firstLsn(), seg.capacityBytes());
            if (indexStore instanceof com.walcore.internal.spiimpl.DefaultIndexStore) {
                SparseIndex idx = ((com.walcore.internal.spiimpl.DefaultIndexStore) indexStore)
                        .indexFor(seg.firstLsn());
                if (idx != null) {
                    seg.attachIndex(idx);
                    // 索引粒度是 long（与 WalConfig 同口径），Segment 侧是 int；
                    // 超过 Integer.MAX_VALUE 的粒度等价于"几乎不建索引"，钳位即可
                    long interval = idx.indexIntervalRecords();
                    int clamped = (int) Math.min(Math.max(interval, 1L), Integer.MAX_VALUE);
                    seg.indexIntervalRecords(clamped);
                }
            }
        } catch (RuntimeException e) {
            // 索引降级：只损失 seek 性能
            if (metrics != null) {
                metrics.increment(MetricsRegistry.CORRUPTION_COUNT);
            }
        }
    }

    /**
     * 取得（或创建）分区的段容器。
     *
     * @param partitionId 分区 ID
     * @return 容器
     */
    private PartitionSegments partitionHolder(int partitionId) {
        return partitions.computeIfAbsent(partitionId, pid -> new PartitionSegments(pid));
    }

    /**
     * 把段追加进 holder 的段序列（同时刷新 {@code firstLsns}、{@code active}、{@code size}）。
     *
     * <h2>并发模型（T14 竞态的根因修复）</h2>
     * 段序列有<b>两个</b>写者来源：写入线程的轮转（持分区写锁，<b>不</b>持 {@code mutex}）
     * 与保留线程的删除（持 {@code mutex}，<b>不</b>要求分区写锁）。早期实现里
     * {@code backing}/{@code lsnBacking}/{@code size} 三个字段被这两条路径无锁并发改写，
     * 后果是<b>平行数组长度错配</b>（实测 {@code ArrayIndexOutOfBoundsException:
     * arraycopy: last source index 7 out of bounds for long[6]}）以及
     * {@code size} 与数组内容错配（读者拿到"段数组来自第 1 代、名字数组来自第 2 代"），
     * 于是一次二分就能把游标定位到<b>错误的段</b>——表现为静默丢区间。
     *
     * <p>现在做了两件事：
     * <ol>
     *   <li><b>所有结构变更都进 {@code ps.mutex}</b>（本方法也进；它是可重入的，
     *       因此 {@code createFirstSegment} 等已持锁的调用方无需改动）；</li>
     *   <li><b>读者只读一个 volatile 快照</b>（{@link PartitionSegments.Snapshot}），
     *       段数组 / 名字数组 / 长度永远来自同一次读取，不存在"半新半旧"的配对。</li>
     * </ol>
     *
     * <p>扩容仍是「1.5 倍增长 + 只写新槽位 + 发布新快照（共享数组）」，
     * 轮转的拷贝分摊依然是 O(1)；只有删除（低频后台）会真正重建数组。
     *
     * @param ps 容器
     * @param seg 段
     */
    private void attachToHolder(PartitionSegments ps, Segment seg) {
        synchronized (ps.mutex) {
            PartitionSegments.Snapshot cur = ps.snapshot;
            int n = cur.size;
            Segment[] segs = cur.segs;
            long[] lsns = cur.lsns;
            if (n > 0) {
                long last = segs[n - 1].firstLsn();
                if (seg.firstLsn() <= last) {
                    throw new WalException.PartitionException(
                            "段名 LSN 必须严格递增: 上一段=" + last + ", 新段=" + seg.firstLsn()
                                    + "（否则 findSegmentFor 会漏段）");
                }
            }
            if (n == segs.length) {
                int newCap = Math.max(4, segs.length + (segs.length >> 1) + 1);
                Segment[] grown = new Segment[newCap];
                long[] grownLsns = new long[newCap];
                System.arraycopy(segs, 0, grown, 0, n);
                System.arraycopy(lsns, 0, grownLsns, 0, n);
                segs = grown;
                lsns = grownLsns;
            }
            segs[n] = seg;
            lsns[n] = seg.firstLsn();
            // 发布点只有这一个：读者读到 size=n+1 时，元素写入必然已可见（volatile 写在前）
            ps.snapshot = new PartitionSegments.Snapshot(segs, lsns, n + 1);
            ps.active = seg;
            ps.version++;
            // 必须同时刷新 PartitionImpl 的活跃段引用：组提交线程是从
            // PartitionImpl#activeSegment() 拿句柄去 force 的。若这里不同步，
            // 轮转后组提交会一直 fsync **旧的**段——新段的数据永远不落盘，
            // 而 flushAndWait 却按水位返回成功（假确认）。这是数据安全问题，不是性能问题。
            PartitionImpl partition = registry.get(ps.pid);
            if (partition != null) {
                partition.setActiveSegment(seg);
            }
        }
    }

    // ------------------------------------------------------------------
    // 查找
    // ------------------------------------------------------------------

    /**
     * 当前活跃（可写）段。
     *
     * @param partitionId 分区 ID
     * @return 段；分区不存在或无段返回 null
     */
    public Segment activeSegment(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        return ps == null ? null : ps.active;
    }

    /**
     * 清空"活跃段"指针（恢复收尾专用）。
     *
     * <p>恢复期会把<b>包括最后段在内</b>的所有既有段都封存为只读——原因是 SPARSE 预分配下
     * "逻辑长度 ≠ 物理长度"，直接在旧段上续写会让写位置落在预分配区的末尾，
     * 中间留下大段零字节（mmap 后端甚至会写在文件 EOF 之外）。清空活跃指针后，
     * 上层立刻新开一段，写入从新段偏移 0 干净开始。
     *
     * <p><b>并发约束（R1）</b>：直接写 {@code ps.active} 与 {@code setActiveSegment(null)}
     * 都不持分区写锁——这在"恢复期单线程、写路径尚未启动"的前提下是安全的
     * （{@code DefaultWal.open} 在本方法返回之前绝不会构造 AppendPipeline）。
     * 运行期若需清活跃指针，必须走 {@code freezePartition} 的持锁路径，<b>不得</b>
     * 调用本方法。
     *
     * @param partitionId 分区 ID
     */
    public void clearActive(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        if (ps != null) {
            ps.active = null;
        }
        PartitionImpl partition = registry.get(partitionId);
        if (partition != null) {
            partition.setActiveSegment(null);
        }
    }

    /**
     * 定位包含 {@code lsn} 的段：在名字 LSN 数组上做 floor 二分。
     *
     * <p>语义：返回<b>名字 LSN ≤ lsn 的最后一个段</b>。因为名字 LSN 恒 ≤ 该段内所有真实
     * LSN（见 {@link NextSegmentPreparer} 的说明），所以真实记录必落在该段内或其后的段内；
     * 由 {@code IndexStore.lookup} 再在段内精确定位。
     *
     * @param partitionId 分区 ID
     * @param lsn 目标 LSN
     * @return 段；{@code lsn} 早于第一个段或分区无段返回 null
     */
    public Segment findSegmentFor(int partitionId, long lsn) {
        PartitionSegments ps = partitions.get(partitionId);
        if (ps == null) {
            return null;
        }
        // 一次 volatile 读拿到"段数组 + 名字数组 + 长度"三者一致的快照。
        // 分别读三个字段（旧实现）会让二分用第 2 代的名字数组、却用第 1 代的段数组取结果，
        // 从而返回一个 firstLsn > lsn 的段——游标因此跳过一整段区间。
        PartitionSegments.Snapshot snap = ps.snapshot;
        int n = snap.size;
        if (n == 0) {
            return null;
        }
        long[] lsns = snap.lsns;
        Segment[] segs = snap.segs;
        int lo = 0;
        int hi = n - 1;
        int best = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (lsns[mid] <= lsn) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return best < 0 ? null : segs[best];
    }

    /**
     * 分区的全部段（升序快照）。冷路径，允许分配。
     *
     * @param partitionId 分区 ID
     * @return 不可变列表；分区不存在返回空列表
     */
    public List<Segment> segments(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        if (ps == null) {
            return Collections.emptyList();
        }
        PartitionSegments.Snapshot snap = ps.snapshot;
        int n = snap.size;
        Segment[] segs = snap.segs;
        List<Segment> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(segs[i]);
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * 分区的段数。
     *
     * @param partitionId 分区 ID
     * @return 数量
     */
    public int segmentCount(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        return ps == null ? 0 : ps.snapshot.size;
    }

    /**
     * 段列表版本号（每次轮转 +1），读者据此感知新增段（ARCH §11 "rolloverVersion"）。
     *
     * @param partitionId 分区 ID
     * @return 版本号；分区不存在返回 -1
     */
    public long rolloverVersion(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        return ps == null ? -1L : ps.version;
    }

    // ------------------------------------------------------------------
    // 轮转
    // ------------------------------------------------------------------

    /**
     * 判断是否需要轮转（写路径每帧调用一次，必须是纯读、无锁、常量时间）。
     *
     * <p>四个触发条件（ARCH §6.2）：空间不足 / 记录数达上限 / 存活时长（仅开启时）/ 手动。
     * "帧不跨段"通过 {@code hasRoom(frameLen)} 实现：{@code remaining < frameLen} 即轮转，
     * 绝不拆分帧——换来读取与恢复逻辑的大幅简化，代价是每段最多浪费 16MB。
     *
     * @param partitionId 分区 ID
     * @param frameLen 即将写入的帧长
     * @return 需要轮转返回 true
     */
    public boolean needsRotation(int partitionId, int frameLen) {
        Segment seg = activeSegment(partitionId);
        if (seg == null) {
            return true;
        }
        if (!seg.hasRoom(frameLen)) {
            return true;
        }
        if (segmentMaxRecords > 0 && seg.recordCount() >= segmentMaxRecords) {
            return true;
        }
        if (timeBasedRotation && segmentMaxAgeMillis > 0
                && System.currentTimeMillis() - seg.createdMillis() >= segmentMaxAgeMillis) {
            return true;
        }
        return false;
    }

    /**
     * 轮转（自行加分区写锁）。供 {@code wal.rotate(pid)} 与外部运维调用。
     *
     * @param partitionId 分区 ID
     * @return 新的活跃段
     * @throws IOException IO 失败
     */
    public Segment rotate(int partitionId) throws IOException {
        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            throw new WalException.PartitionException("分区不存在: " + partitionId);
        }
        boolean alreadyHeld = partition.isHeldByCurrentThread();
        if (!alreadyHeld) {
            partition.lock();
        }
        try {
            return rotateLocked(partitionId);
        } finally {
            if (!alreadyHeld) {
                partition.unlock();
            }
        }
    }

    /**
     * 轮转（<b>调用方必须已持有分区写锁</b>）。
     *
     * <p>步骤（顺序不可调整）：
     * <ol>
     *   <li>{@code sealToReadOnly}：先 {@code force(true)} 再冻结长度——保证"数据先落盘"；</li>
     *   <li>取走预建段，校验名字严格递增；不通过则丢弃并走内联降级；</li>
     *   <li>交换引用（挂进段序列 + 更新 active）；</li>
     *   <li>{@code warmUp} 唤醒背景线程补建，使下一次轮转依然是纯指针交换。</li>
     * </ol>
     *
     * @param partitionId 分区 ID
     * @return 新的活跃段
     * @throws IOException IO 失败
     */
    public Segment rotateLocked(int partitionId) throws IOException {
        PartitionSegments ps = partitionHolder(partitionId);
        Segment old = ps.active;
        if (old == null) {
            throw new WalException.PartitionException(
                    "分区 " + partitionId + " 尚无活跃段，无法轮转");
        }
        // 1) 封存旧段：force(true) → 冻结 finalLength → READONLY
        old.sealToReadOnly(old.publishedWritePos());

        // 2) 尝试取预建段（命中时这里是纯指针操作，< 1µs）
        //
        //    这里必须用"取—校验—不通过就收尸—再取"的循环，而不是一次性 takeReady + discardReady。
        //    原因（T05 空段泄漏的根因）：takeReady 已经把段从 ready 里移走，
        //    此时 discardReady 只会返回 null 且**什么都不做**——段句柄不关、文件不删。
        //    一旦竞态发生（内联降级抢先用 reserveNameLsn 拿到较小的名字），
        //    后续每次轮转都会命中这条分支，变成"每次轮转泄漏一个空 .wal"。
        Segment next = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            Segment candidate = preparer.takeReady(partitionId);
            if (candidate == null) {
                break;
            }
            if (candidate.firstLsn() > old.firstLsn()) {
                next = candidate;
                break;
            }
            // 名字不满足"严格大于上一段"：段序会被破坏，不能使用。
            // 必须连文件一起删掉（discardSegment 会 close + 删除 .wal/.idx），
            // 否则空段留在目录里，恢复流程要多扫一遍，磁盘也会持续增长。
            preparer.discardSegment(candidate);
        }

        // 3) 降级：内联创建（唯一允许的超 1ms 尖刺，必须计数）
        if (next == null) {
            fallbackCount.incrementAndGet();
            if (metrics != null) {
                metrics.increment(MetricsRegistry.ROTATE_FALLBACK);
            }
            next = createSegment(partitionId, reserveNameLsn(partitionId));
        } else if (indexStore != null) {
            // 预建段尚未开索引（预建时还不知道会不会被采用），这里补齐
            attachIndex(next);
        }

        // 4) 交换引用并唤醒补建
        attachToHolder(ps, next);
        rotationCount.incrementAndGet();
        warmUp(partitionId);
        return next;
    }

    /**
     * 请求后台预建下一段（幂等）。
     *
     * @param partitionId 分区 ID
     */
    public void warmUp(int partitionId) {
        if (closed) {
            return;
        }
        preparer.warmUp(partitionId);
    }

    /**
     * 阻塞等待预建完成（仅测试与"打开后首轮预热"使用；生产写路径绝不可调用）。
     *
     * @param partitionId 分区 ID
     * @param timeoutMillis 超时
     * @return 等到储备返回 true
     * @throws InterruptedException 被中断
     */
    public boolean awaitPrepared(int partitionId, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (preparer.hasReady(partitionId)) {
                return true;
            }
            Thread.sleep(1L);
        }
        return preparer.hasReady(partitionId);
    }

    /**
     * 为一个新段<b>预定</b>名字 LSN（每次调用返回严格递增的新值）。
     *
     * <h2>为什么不是"算一下就返回"</h2>
     * 预建线程与轮转降级路径<b>可能同时</b>需要创建一个段。若两者都按
     * {@code max(peek, 上一段+1)} 计算，就会得到<b>同一个</b>名字，于是两者争抢同一个
     * {@code .wal.tmp} 文件——Windows 上表现为 {@code AccessDeniedException}
     * （实测：预建线程正持有 tmp 句柄时，降级路径 {@code FileChannel.open(tmp)} 直接失败）。
     * 因此名字必须<b>预定</b>：每次调用 CAS 出一个新值，无人重复。
     *
     * <h2>为什么取 {@code max(peek, lastReserved+1)}</h2>
     * <ul>
     *   <li>{@code peek()}：保证名字 <b>≤</b> 该段未来首条真实记录的 LSN
     *       （因为 LSN 只会往大走），这是 {@link #findSegmentFor} 二分正确的前提；</li>
     *   <li>{@code +1}：保证同一分区内名字<b>严格递增</b>，不会生成重名文件。</li>
     * </ul>
     *
     * <h2>由此带来的一个必须容忍的性质</h2>
     * 对<b>空分区</b>反复轮转时 {@code peek()} 不前进，名字会被 {@code +1} 推高，
     * 可能出现"名字 LSN &gt; 段内真实首条 LSN"的空段。这不影响正确性：
     * 空段里没有任何帧，读者在 {@code findSegmentFor} 落到空段后会顺序前进到下一段。
     * 代价是"空段多跳一次"，换来的是"绝不会因重名而创建失败"。
     *
     * @param partitionId 分区 ID
     * @return 已预定的名字 LSN
     */
    public long reserveNameLsn(int partitionId) {
        AtomicLong reservation = nameReservations.computeIfAbsent(
                partitionId, pid -> new AtomicLong(0L));
        while (true) {
            long last = reservation.get();
            long candidate = Math.max(lsnAllocator.peek(), last + 1L);
            if (reservation.compareAndSet(last, candidate)) {
                return candidate;
            }
        }
    }

    /**
     * 把分区的名字预定值抬到至少 {@code firstLsn}（恢复/显式指定名字时调用）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 已使用的名字 LSN
     */
    private void observeName(int partitionId, long firstLsn) {
        nameReservations.computeIfAbsent(partitionId, pid -> new AtomicLong(0L))
                .accumulateAndGet(firstLsn, Math::max);
    }

    // ------------------------------------------------------------------
    // 冻结 / 删除
    // ------------------------------------------------------------------

    /**
     * 删除分区内<b>指定的一个段</b>（保留策略专用，T14）。
     *
     * <h2>为什么"删一个段"比"删整个分区"难</h2>
     * 段序列是"平行数组 + 二分查找"的结构，删中间一个元素必须重建数组并发布新长度，
     * 否则二分区间里会留下空洞、{@code findSegmentFor} 会返回一个已关闭句柄的段。
     *
     * <h2>三段式流程（顺序不可调整）</h2>
     * <ol>
     *   <li><b>锁内</b>：定位 → 校验 LSN 围栏（{@code fenceLsn}）→ 拒绝活跃段 →
     *       {@link Segment#beginDelete()} 原子地确认"无读者 + 状态可删"并把状态推进到
     *       {@code DELETE_PENDING} → <b>立刻从段序列摘除</b>。
     *       摘除放在锁内，是为了让"新读者再也定位不到它"，同时把锁的持有时间压到
     *       几十纳秒——unmap 与删文件（Windows 上可能几十毫秒）必须放到锁外，
     *       否则一次删除就会把写入线程的轮转阻塞住，变成 p99 尖刺；</li>
     *   <li><b>锁外</b>：{@code freeze()}（unmap）→ 关索引 → 删 {@code .wal} / {@code .idx}。
     *       Windows 上"映射中的文件不可删"，所以 unmap 必须在删之前；</li>
     *   <li>失败时：段已经从序列里摘除，文件残留由下次启动的清理逻辑处理（绝不把
     *       一个"句柄已关"的段留在序列里）。</li>
     * </ol>
     *
     * <h2>竞态窗口已关闭（T14 回归缺陷的根因）</h2>
     * 早期实现是"先看 {@code readerCount == 0}，再 unmap+删文件"，中间没有任何互斥：
     * 一个刚通过 {@code findSegmentFor} 拿到引用、还没来得及登记读者的线程，
     * 可能在文件被删后读到关闭的句柄。现在这个窗口由 {@link Segment#beginDelete()} /
     * {@link Segment#tryRetainReader()} 的<b>段级监视器</b>闭合：只可能"读者先"或"删除先"，
     * 不存在"两边都以为自己成功了"。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段的名字 LSN（段的唯一标识）
     * @return 实际删除返回 true；被保护/不存在返回 false
     */
    public boolean deleteSegment(int partitionId, long firstLsn) {
        return deleteSegment(partitionId, firstLsn, 0L);
    }

    /**
     * 删除指定的一个段，并在<b>同一临界区内</b>复核"不可删除边界"。
     *
     * <h2>为什么围栏必须传进来复核，而不是只由调用方在循环外判断一次</h2>
     * 保留策略的删除循环是"决策一次、逐个执行"，而一次 unmap + 删文件在 Windows 上
     * 可达数毫秒。实测过的失败形态（T14 回归的第三种签名）：一轮删除在
     * {@code t0} 采样到"没有任何读者"（{@code minPinnedLsn == 0}），随后花 10ms
     * 逐个删文件；就在这段时间里一个游标被创建并登记了围栏 {@code 1}，
     * 于是它<b>刚开始读就被删掉了尚未读到的段</b>，续读直接跳号。
     *
     * <p>把围栏作为参数传进来、在锁内、紧邻 {@code beginDelete()} 之前复核，
     * 把"看起来可删"与"真的摘除"之间的一致性窗口压到<b>几十纳秒</b>；
     * 该段是否正被读则由 {@code beginDelete()} 的读者计数互斥再兜一层。
     * 这两道检查必须都在锁内，否则又只是把窗口挪了个位置。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段的名字 LSN
     * @param fenceLsn 不可删除边界（{@code <= 0} 表示无约束）
     * @return 实际删除返回 true；被保护/不存在/校验未通过返回 false
     */
    public boolean deleteSegment(int partitionId, long firstLsn, long fenceLsn) {
        PartitionSegments ps = partitions.get(partitionId);
        if (ps == null) {
            return false;
        }
        final Segment target;
        synchronized (ps.mutex) {
            PartitionSegments.Snapshot snap = ps.snapshot;
            int n = snap.size;
            if (n <= 1) {
                // 至少保留一个段：全删光会让"日志目录"变成空目录，下次开办要重建，
                // 正在写的记录也无处落地
                return false;
            }
            int idx = -1;
            Segment[] segs = snap.segs;
            for (int i = 0; i < n; i++) {
                if (segs[i].firstLsn() == firstLsn) {
                    idx = i;
                    break;
                }
            }
            if (idx < 0) {
                return false;
            }
            target = segs[idx];
            if (fenceLsn > 0L && target.lastLsn() >= fenceLsn) {
                // 围栏在"决策"之后又被抬高过（有新的读者/快照进来了）
                return false;
            }
            if (target == ps.active || target.isActive()) {
                return false;
            }
            PartitionImpl owner = registry.get(partitionId);
            if (owner != null && owner.activeSegment() == target) {
                // 双保险：写入路径与 PartitionImpl 的活跃段引用必须一致，
                // 万一出现短暂不一致也绝不删"组提交正在 force 的那个段"
                return false;
            }
            // 关键一步：与读者登记互斥地"封闭"这个段（仍有读者则本轮放弃）
            if (!target.beginDelete()) {
                return false;
            }
            removeAtLocked(ps, idx);
        }

        // ---- 锁外：慢操作（unmap / 删文件），不得阻塞轮转 ----
        try {
            target.freeze();
            if (indexStore != null) {
                try {
                    indexStore.closeSegment(firstLsn);
                } catch (RuntimeException ignored) {
                    // 索引关闭失败不阻塞数据删除
                }
            }
            backend.delete(target.handle().path());
            deleteIdxQuietly(target);
        } catch (IOException e) {
            throw new WalException.IoException(
                    "删除段文件失败: " + target.handle().path(), e);
        }
        return true;
    }


    /**
     * 从段序列中摘除一个元素（<b>调用方必须已持有 {@code ps.mutex}</b>）。
     *
     * <h2>为什么重建数组而不是原地左移</h2>
     * 原地左移会让"仍持有旧快照的读者"看到一半移位、一半没移的数组（元素级可见，
     * 整体不可见），二分可能落到错的位置。整体重建 + 一次性发布新快照，
     * 读者要么看到旧序列（含该段），要么看到新序列，没有中间态。
     *
     * <p>新数组容量取 {@code max(旧容量, 4)}（<b>不缩容</b>）：删除是低频后台操作，
     * 保留容量可以让下一次轮转仍是"只写新槽位"，避免删一次就要再扩容一次。
     *
     * @param ps 容器
     * @param idx 要摘除的下标
     */
    private static void removeAtLocked(PartitionSegments ps, int idx) {
        PartitionSegments.Snapshot cur = ps.snapshot;
        int n = cur.size;
        int cap = Math.max(cur.segs.length, 4);
        Segment[] newSegs = new Segment[cap];
        long[] newLsns = new long[cap];
        int w = 0;
        for (int i = 0; i < n; i++) {
            if (i == idx) {
                continue;
            }
            newSegs[w] = cur.segs[i];
            newLsns[w] = cur.lsns[i];
            w++;
        }
        ps.snapshot = new PartitionSegments.Snapshot(newSegs, newLsns, w);
        ps.version++;
    }

    /**
     * 冻结分区：停止预建、封存活跃段、释放所有映射（保留文件）。
     *
     * <p>先封存（要写盘）后 {@code freeze()}（会 unmap），顺序不能反。
     *
     * @param partitionId 分区 ID
     */
    public void freeze(int partitionId) {
        PartitionSegments ps = partitions.get(partitionId);
        if (ps == null) {
            return;
        }
        synchronized (ps.mutex) {
            preparer.cancel(partitionId);
            Segment active = ps.active;
            if (active != null && active.isActive()) {
                try {
                    active.sealToReadOnly(active.publishedWritePos());
                } catch (IOException e) {
                    throw new WalException.IoException(
                            "冻结分区 " + partitionId + " 时封存活跃段失败", e);
                }
            }
            PartitionSegments.Snapshot snap = ps.snapshot;
            int n = snap.size;
            for (int i = 0; i < n; i++) {
                Segment s = snap.segs[i];
                if (s.state() != Segment.SegmentState.FROZEN) {
                    s.freeze();
                }
            }
        }
    }

    /**
     * 删除分区的全部段文件与索引文件（{@code RemoveMode.PURGE}）。
     *
     * <p>顺序：冻结（unmap）→ 关闭索引 → 删除文件。Windows 上"映射中的文件不可删除"，
     * 所以必须先 {@code freeze()}，这是 T03 契约测试专门证明过的点。
     *
     * @param partitionId 分区 ID
     */
    public void deletePartitionData(int partitionId) {
        // 1) 先取消预建并丢弃储备：否则预建线程会在我们删完之后把文件建回来
        preparer.cancel(partitionId);
        nameReservations.remove(partitionId);
        PartitionSegments ps = partitions.remove(partitionId);
        if (ps == null) {
            return;
        }
        synchronized (ps.mutex) {
            PartitionSegments.Snapshot snap = ps.snapshot;
            int n = snap.size;
            for (int i = 0; i < n; i++) {
                Segment s = snap.segs[i];
                if (s.state() != Segment.SegmentState.FROZEN) {
                    s.freeze();
                }
                if (indexStore != null) {
                    try {
                        indexStore.closeSegment(s.firstLsn());
                    } catch (RuntimeException ignored) {
                        // 索引关闭失败不阻塞数据删除
                    }
                }
                try {
                    backend.delete(s.handle().path());
                } catch (IOException e) {
                    throw new WalException.IoException(
                            "删除段文件失败: " + s.handle().path(), e);
                }
                deleteIdxQuietly(s);
            }
        }
        Path dir = SegmentNaming.partitionDir(baseDir, partitionId);
        cleanupDirQuietly(dir);
    }

    /**
     * 删除分区目录里可能残留的 {@code .tmp}/{@code .idx}，再删空目录。
     *
     * <p>为什么需要这一步：预建线程可能刚刚创建了 {@code .wal.tmp} 就收到取消信号，
     * 或者 {@code .idx} 是由索引层独立打开的。只要目录非空，{@code delete} 就会失败，
     * 分区删除就变成"表面上成功、磁盘上留垃圾"——这类残留会在下次启动时被
     * 恢复流程当成真实段扫描，必须彻底清掉。
     *
     * @param dir 分区目录
     */
    private void cleanupDirQuietly(Path dir) {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Windows 上句柄未及时释放，交由下次启动清理
                }
            }
        } catch (IOException ignored) {
            // 列目录失败：交给下面的 deleteIfExists 兜底
        }
        try {
            Files.deleteIfExists(dir);
        } catch (IOException ignored) {
            // 目录非空/被占用都无所谓，下次启动的清理逻辑会处理
        }
    }

    /**
     * 删除段对应的 {@code .idx} 文件（失败不影响主流程）。
     *
     * @param s 段
     */
    private void deleteIdxQuietly(Segment s) {
        Path idx = SegmentNaming.idxPath(baseDir, s.partitionId(), s.firstLsn());
        try {
            Files.deleteIfExists(idx);
        } catch (IOException ignored) {
            // Windows 上句柄未及时释放会失败，交由下次启动清理
        }
    }

    /**
     * 从管理器中移除分区（不删文件，仅释放内存引用）。
     *
     * @param partitionId 分区 ID
     */
    public void detachPartition(int partitionId) {
        partitions.remove(partitionId);
    }

    // ------------------------------------------------------------------
    // 统计 / 关闭
    // ------------------------------------------------------------------

    /**
     * 轮转降级次数（{@code wal.rotate.fallback}）。
     *
     * @return 次数
     */
    public long fallbackCount() {
        return fallbackCount.get();
    }

    /**
     * 成功轮转次数。
     *
     * @return 次数
     */
    public long rotationCount() {
        return rotationCount.get();
    }

    /**
     * 创建的段总数。
     *
     * @return 数量
     */
    public long createdCount() {
        return createdCount.get();
    }

    /**
     * 预建器（供 {@code WalStats} 上报与测试断言）。
     *
     * @return 预建器
     */
    public NextSegmentPreparer preparer() {
        return preparer;
    }

    /**
     * 段容量。
     *
     * @return 字节数
     */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    /**
     * 是否已关闭。
     *
     * @return 关闭返回 true
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * 关闭管理器：停预建线程 → 封存并关闭所有段句柄。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        preparer.stop();
        for (Integer pid : new ArrayList<>(partitions.keySet())) {
            PartitionSegments ps = partitions.get(pid);
            if (ps == null) {
                continue;
            }
            int n = ps.snapshot.size;
            Segment[] snapSegs = ps.snapshot.segs;
            for (int i = 0; i < n; i++) {
                Segment s = snapSegs[i];
                try {
                    if (s.isActive()) {
                        s.sealToReadOnly(s.publishedWritePos());
                    }
                    s.closeHandle();
                } catch (IOException e) {
                    throw new WalException.IoException(
                            "关闭段句柄失败: " + s.handle().path(), e);
                }
                if (indexStore != null) {
                    try {
                        indexStore.closeSegment(s.firstLsn());
                    } catch (RuntimeException ignored) {
                        // 索引关闭失败不阻塞整体关闭
                    }
                }
            }
        }
    }

    /**
     * 单个分区的段容器：<b>不可变快照 + 1.5 倍容量增长</b>。
     *
     * <h2>为什么是"一个 volatile 引用"而不是"三个平行字段"</h2>
     * 段序列由三样东西共同描述：段数组、名字 LSN 数组、长度。读者必须读到
     * <b>同一代</b>的三者，否则二分用第 2 代的名字数组、却用第 1 代的段数组取结果，
     * 返回的段与目标 LSN 不匹配——游标就会静默跳过一整段区间。
     * 早期实现分别读三个字段（{@code size} 用 acquire/release 发布，数组是独立 volatile），
     * 在"保留策略删除"与"写入线程轮转"并发时会短暂看到错配的组合。
     *
     * <p>打包成一个不可变 {@link Snapshot} 后，读者只做<b>一次</b> volatile 读，
     * 三者天然一致；写者仍是"1.5 倍扩容 + 只写新槽位 + 换快照"，
     * 扩容的拷贝依然是 O(1) 分摊，轮转不会被变慢。
     *
     * <h2>写者纪律（唯一）</h2>
     * 只有持有 {@link #mutex} 的线程可以替换 {@link #snapshot} 或写其中的槽位。
     * 轮转（持分区写锁）与删除（持 {@code mutex}）都要进这把锁——这就是
     * T14 竞态修复的全部要求。
     */
    private static final class PartitionSegments {

        /** 不可变的段序列快照。 */
        static final class Snapshot {

            /** 空快照常量（避免空分配）。 */
            static final Snapshot EMPTY = new Snapshot(EMPTY_SEGMENTS, EMPTY_LSNS, 0);

            /** 段数组（容量 &gt;= {@link #size}，扩容时整体复制）。 */
            final Segment[] segs;

            /** 与 {@link #segs} 平行的名字 LSN 数组（二分用，避免指针追逐）。 */
            final long[] lsns;

            /** 有效元素个数。 */
            final int size;

            /**
             * 构造快照。
             *
             * @param segs 段数组
             * @param lsns 名字 LSN 数组
             * @param size 有效元素个数
             */
            Snapshot(Segment[] segs, long[] lsns, int size) {
                this.segs = segs;
                this.lsns = lsns;
                this.size = size;
            }
        }

        /** 分区 ID。 */
        final int pid;

        /** 序列化所有结构变更（轮转 / 删除 / 冻结）的互斥体（低频操作，用 synchronized 最简单）。 */
        final Object mutex = new Object();

        /** 当前段序列快照。<b>唯一的发布点</b>。 */
        volatile Snapshot snapshot = Snapshot.EMPTY;

        /** 当前活跃段。 */
        volatile Segment active;

        /** 轮转版本号。 */
        volatile long version;

        PartitionSegments(int pid) {
            this.pid = pid;
        }

        @Override
        public String toString() {
            return "PartitionSegments{pid=" + pid + ", segments=" + snapshot.size
                    + ", version=" + version + '}';
        }
    }
}
