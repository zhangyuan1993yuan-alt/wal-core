package com.walcore.internal.read;

import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.api.WalRecordVisitor;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.io.MappedWindowCache;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.segment.SparseIndex;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexHit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 顺序读游标实现（ARCH §8.1、§13.2）。
 *
 * <h2>定位三步（{@link #seekTo(long)}）</h2>
 * <ol>
 *   <li><b>段级二分</b>：{@code SegmentManager.findSegmentFor(lsn)} 在段的 {@code long[]}
 *       上二分，得到"首条 LSN ≤ 目标"的最后一个段；</li>
 *   <li><b>索引级二分</b>：{@code SparseIndex.lookup(lsn)} 得到段内最近的稀疏锚点；</li>
 *   <li><b>段内顺序扫</b>：从锚点偏移逐帧解码，直到 {@code record.lsn() >= target}。</li>
 * </ol>
 *
 * <h2>跨段与"段名 LSN 可能大于真实首 LSN"</h2>
 * 段文件名里的 LSN 由 T05 的 nameReservations 发号，空分区轮转时可能<b>大于</b>段内
 * 首条记录的真实 LSN。因此段级二分可能把游标定位到"前一个段"，此时本实现的
 * "扫到段尾就切下一段"逻辑会自然把游标带进正确的段——<b>结果正确，只是多扫一点</b>。
 * 游标本身不依赖文件名 LSN 做任何内容推断。
 *
 * <h2>只读已发布区间</h2>
 * 每帧读取前都检查 {@code offset + frameLen <= segment.publishedWritePos()}（release 语义发布），
 * 因此绝不可能读到"写了一半"的帧。追上写入方时 {@link #next()} 返回 {@code null}。
 *
 * <h2>读者登记粒度：<b>只登记"当前正在读的那一个段"，不是整个分区</b></h2>
 * 游标是<b>只进不退</b>的顺序读者：一旦离开某个段，它再也不会回头读那个段。因此
 * 正确做法是 {@link Segment#retainReader()} 只压在"当前位置所在的段"上，跨段前
 * {@code retainReader} 新段、随后 {@code releaseReader} 旧段（顺序不可颠倒，
 * 保证任何瞬间至少有一个段被持有）。
 *
 * <p><b>为什么这条粒度如此重要（实测过的真实缺陷）</b>：早期实现在构造时对分区
 * <b>全部</b>段 {@code retainReader}，直到 {@code close()} 才统一释放。后果不是"多占点
 * 计数"，而是<b>保留策略被彻底废掉</b>——
 * {@code RetentionManager.minPinnedLsn} 的兜底逻辑取"仍有读者的最旧段的首帧 LSN"，
 * 全部段都有读者时该值恒等于最旧段首帧 LSN（通常是 1），于是
 * {@code isPinned} 对所有段为 true、策略返回 {@code keepAll}。
 * <b>只要存在一个长活游标或 tailer，该分区一个段都删不掉</b>，即使配置了
 * {@code retentionMaxAgeMillis=1}。生产表现就是"默认 7 天保留 + 常驻订阅者 = 磁盘无限增长"。
 *
 * <p>反过来说，粒度放细之后语义才符合 T14 ②：<b>保护"当前位置及之后"，已读过的旧段可删</b>。
 * 删除旧段对游标无害——它不会回头读；即使 {@link #seekTo(long)} 把游标拉回到已删区间，
 * 也只是读不到（数据已被保留策略按配置丢弃），不会抛异常也不会读到错数据。
 *
 * <h2>两条保护必须同时具备：{@code readerCount}（段级）+ {@code pin}（LSN 级）</h2>
 * 只做段级登记仍有一个致命弱点：{@code RetentionManager.minPinnedLsn} 的兜底逻辑是
 * "扫段序列，取第一个 {@code readerCount > 0} 的段"。如果这个段因为<b>任何</b>原因
 * 不在段序列里了（删除、重建、恢复重排），兜底值就退化为 0（"无人读"），
 * 于是<b>剩余所有段立刻都变得可删</b>——一次误删会级联成"整段区间消失"，
 * 表现为游标续读时 LSN 直接跳号（实测签名：{@code expected 61 but was 145}）。
 *
 * <p>因此游标在<b>跨段</b>时还向保留策略登记一个 <b>LSN 围栏</b>
 * （{@link RetentionPin}）：它与段对象是否还在段序列里无关，
 * 只要围栏还在，{@code lastLsn >= 围栏值} 的段一律不可删。
 * 围栏随读位置前进（pin 新的、再 unpin 旧的），{@code close()} 时撤销。
 *
 * <p>另外，{@link Segment#tryRetainReader()} 与 {@link Segment#beginDelete()} 共用
 * 段级监视器，把"检查 {@code readerCount == 0}"与"读者登记"之间的
 * <b>check-then-act 窗口</b>关掉——这是同一个缺陷的第二半：删方与读方不可能同时成功。
 */
public final class CursorImpl implements WalCursor {

    /** 每多少条记录强制落一次 checkpoint。 */
    private static final long CHECKPOINT_RECORDS = 100_000L;

    private final int partitionId;
    private final SegmentManager segmentManager;
    private final FrameDecoder decoder;
    private final CursorOptions options;
    private final CursorCheckpointStore checkpointStore;
    private final MappedWindowCache windows;

    /**
     * 游标持有的段快照。<b>可增长</b>：追尾读（tailing）时新段会由
     * {@link #refreshSegments()} 追加进来。
     *
     * <p>为什么不用 {@code Collections.unmodifiableList(...)}：游标的语义是"一直读到追上
     * 写入方"，而段列表在轮转后会变长。早期版本在构造时取一次不可变快照，结果
     * "读到段尾 → {@code segmentIndex} 越过数组 → 永远返回 null"，即使之后又有
     * 100 万条新记录写进来也读不到——这是 tailer 场景下最致命的一类 bug。
     */
    private final java.util.List<Segment> segments = new java.util.ArrayList<>();
    private final WalRecord record = new WalRecord();
    private final DecodedFrame frame = new DecodedFrame();
    private final ByteBuffer smallRead = ByteBuffer.allocate(FrameLayout.HEADER_LEN);
    /**
     * codec 解码的落点缓冲（F4：按帧头声明大小<b>可增长</b>）。
     *
     * <p>初值 64KB 只覆盖常见小记录；非 RAW codec + 大 payload（或压缩 codec 的
     * "stored 小、payload 大"）在 {@link #decodeAt} 里按帧头 payloadLen 扩容。
     * 扩容有上限（{@link FrameLayout#MAX_PAYLOAD_LEN}），恶意帧头不会触发超大分配。
     */
    private ByteBuffer payloadSink = ByteBuffer.allocate(64 * 1024);
    private ByteBuffer wholeFrame = ByteBuffer.allocate(64 * 1024);

    private int segmentIndex;
    private long offset;
    private boolean pending;
    private long nextLsnWanted;
    private long lastReturnedLsn;
    private long recordsSinceCheckpoint;
    private long lastCheckpointNanos;
    private boolean closed;
    private WalCursor.SegmentRolloverListener rolloverListener;

    /**
     * 当前被本游标登记为读者（{@link Segment#tryRetainReader()}）的那个段的引用，没有则为 null。
     *
     * <p><b>始终只可能是一个段</b>（见类注释"读者登记粒度"）。所有权的转移集中在
     * {@link #ensureRetained(Segment)} 一处：先 retain 新段、再 release 旧段，
     * 因此不存在"两个段都没人持有"的瞬时窗口。
     */
    private Segment retainedSegment;

    /**
     * 向保留策略登记"不可删除 LSN"的出口（可能为 {@link RetentionPin#NONE}）。
     *
     * <p>与 {@link #retainedSegment} 配套：段对象级别的 {@code readerCount} 只解决
     * "别删我手上这一段"，LSN 级别的 pin 才解决"别删我后面还没读的段"，
     * 并且在段被误摘出段序列时仍能保持围栏（见 {@link RetentionPin} 的说明）。
     */
    private final RetentionPin retentionPin;

    /** 当前已登记的 pin（0 表示未登记）。 */
    private long pinnedLsn;

    /**
     * 构造游标（不登记任何保留策略 pin；保留策略不参与时使用）。
     *
     * @param partitionId 分区 ID
     * @param segmentManager 段管理器
     * @param decoder 帧解码器
     * @param options 选项
     * @param checkpointStore checkpoint 存储，可为 null（不持久化）
     * @param windows 只读映射窗口缓存，可为 null（退化为逐帧 read）
     */
    public CursorImpl(int partitionId, SegmentManager segmentManager, FrameDecoder decoder,
                      CursorOptions options, CursorCheckpointStore checkpointStore,
                      MappedWindowCache windows) {
        this(partitionId, segmentManager, decoder, options, checkpointStore, windows,
                RetentionPin.NONE);
    }

    /**
     * 构造游标。
     *
     * @param partitionId 分区 ID
     * @param segmentManager 段管理器
     * @param decoder 帧解码器
     * @param options 选项
     * @param checkpointStore checkpoint 存储，可为 null（不持久化）
     * @param windows 只读映射窗口缓存，可为 null（退化为逐帧 read）
     * @param retentionPin 保留策略 pin 出口，可为 null（等价于 {@link RetentionPin#NONE}）
     */
    public CursorImpl(int partitionId, SegmentManager segmentManager, FrameDecoder decoder,
                      CursorOptions options, CursorCheckpointStore checkpointStore,
                      MappedWindowCache windows, RetentionPin retentionPin) {
        this.partitionId = partitionId;
        this.segmentManager = Preconditions.checkNotNull(segmentManager, "segmentManager");
        this.decoder = Preconditions.checkNotNull(decoder, "decoder");
        this.options = Preconditions.checkNotNull(options, "options");
        this.checkpointStore = checkpointStore;
        this.windows = windows;
        this.retentionPin = retentionPin == null ? RetentionPin.NONE : retentionPin;
        // 只把段对象收进快照，<b>不</b>在这里登记读者：读者登记按"当前位置"惰性进行，
        // 见 ensureRetained()。构造期登记全部段会让保留策略在整个游标生命周期内失效。
        this.segments.addAll(segmentManager.segments(partitionId));

        long startLsn = options.fromLsn();
        if (options.resumeFromCheckpoint() && checkpointStore != null) {
            long restored = checkpointStore.restore(partitionId);
            if (restored > startLsn) {
                startLsn = restored;
            }
        }
        this.nextLsnWanted = startLsn;
        this.lastCheckpointNanos = System.nanoTime();
        seekTo(startLsn);
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public long position() {
        return nextLsnWanted;
    }

    @Override
    public boolean seekTo(long lsn) {
        checkOpen();
        Preconditions.checkArgument(lsn >= 0L, "lsn 不得为负: %s", lsn);
        pending = false;

        // 第一步就立围栏，而不是等到"找到段"或"第一次 advanceRaw"才立。
        //
        // 实测过的事故（T14 回归的第二种签名）：游标从 LSN 1 打开时，如果 1 所在的段
        // 已被保留策略删掉，findSegmentFor 返回 null，下面的早退分支不会调用
        // ensureRetained ——于是从"游标构造完成"到"第一次进入一个存活段"之间，
        // 本游标既没有 readerCount、也没有 pin，minPinnedLsn 退化为 0，
        // 保留策略把**游标马上要读的段**当成"无人使用"全部删掉，
        // 续读直接跳号（探针实测：pos=180 时下一批 181..288 被删，接着读到 289）。
        // 先按请求的 LSN 立围栏可以一次性关掉这段窗口：游标只进不退，
        // "从 lsn 往前读"的下界就是 lsn；真正进入存活段后 pinAt() 会把围栏推进到段首。
        pinLsn(Math.max(lsn, 1L));

        Segment target = segmentManager.findSegmentFor(partitionId, lsn);
        if (target == null) {
            // 分区还没有任何段，或目标在分区之前
            segmentIndex = 0;
            offset = 0L;
            nextLsnWanted = Math.max(lsn, 1L);
            return false;
        }
        int index = indexOf(target);
        if (index < 0) {
            index = 0;
        }
        index = stepBackToCoveringSegment(index, lsn);
        segmentIndex = index;
        offset = 0L;
        // 立刻把读者登记移到目标段：这样"从 seek 完成到第一次 advanceRaw 之间"也不存在
        // "没有任何段被持有"的窗口，保留策略不会在这段缝隙里删掉我们即将读的段。
        if (index < segments.size()) {
            ensureRetained(segments.get(index));
        }

        // 索引级二分：从最近的稀疏锚点起扫，避免从段首硬扫
        Segment anchorSegment = segments.get(index);
        SparseIndex sparseIndex = anchorSegment.index();
        if (sparseIndex != null) {
            IndexHit hit = sparseIndex.lookup(lsn);
            if (hit.isPresent() && hit.fileOffset() >= 0L) {
                offset = hit.fileOffset();
            }
        }
        nextLsnWanted = lsn;

        // 段内顺序扫，直到找到 >= lsn 的第一条
        while (true) {
            if (!advanceRaw()) {
                return false;
            }
            if (record.lsn() >= lsn) {
                pending = true;
                return true;
            }
        }
    }

    @Override
    public WalRecord next() {
        checkOpen();
        if (!pending && !advanceRaw()) {
            return null;
        }
        pending = false;
        onRecordReturned();
        return record;
    }

    @Override
    public int nextBatch(int maxRecords, WalRecordVisitor visitor) {
        checkOpen();
        Preconditions.checkNotNull(visitor, "visitor");
        Preconditions.checkPositive(maxRecords, "maxRecords");
        int count = 0;
        while (count < maxRecords) {
            if (!pending && !advanceRaw()) {
                break;
            }
            pending = false;
            onRecordReturned();
            count++;
            if (!visitor.accept(record)) {
                break;
            }
        }
        return count;
    }

    @Override
    public void onSegmentRollover(WalCursor.SegmentRolloverListener listener) {
        this.rolloverListener = listener;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // 只释放"当前段"这一份登记（粒度见类注释）。早期实现按快照逐段释放，
        // 与"构造期逐段登记"配套；现在登记是惰性且单份的，逐段释放会把计数打成负数
        // （Segment#releaseReader 已做钳位兜底，但根因在这里）。
        Segment held = retainedSegment;
        retainedSegment = null;
        if (held != null) {
            held.releaseReader();
        }
        // 撤销 LSN 围栏：不撤销的话保留策略会认为"还有人在读老位置"，
        // 该分区的磁盘将只增不减（实测表现：关闭游标后 minPinnedLsn 仍停在旧值，
        // 删除停摆，20 秒后目录里还留着一堆 .wal）。
        unpinCurrent();
        if (checkpointStore != null && lastReturnedLsn > 0L) {
            try {
                checkpointStore.persist(partitionId, nextLsnWanted);
            } catch (IOException ignored) {
                // checkpoint 失败不影响读的正确性，下次再试
            }
        }
    }

    // ==================================================================
    // 内部
    // ==================================================================

    /**
     * 把"读者登记"转移到 {@code segment} 上，并把保留策略的 LSN 围栏推到当前位置。
     *
     * <h2>四步顺序（不可调整）</h2>
     * <ol>
     *   <li><b>retain 新段</b>：与删除判定互斥（{@link Segment#tryRetainReader()}），
     *       失败说明该段在登记之前就已被删除；</li>
     *   <li><b>pin 新位置</b>：按 LSN 立围栏，保护"当前位置及之后"而不仅仅是当前段；</li>
     *   <li><b>release 旧段</b>：旧段可以立刻被回收（游标只进不退，不会回头读）；</li>
     *   <li><b>unpin 旧位置</b>：最后才撤销旧围栏。</li>
     * </ol>
     * 任意时刻都满足"新段已保护 或 旧段仍保护"：retain-new 之后旧段还在，
     * pin-new 之后新位置已被围栏覆盖，因此不会出现"两个段都没人保护"的空窗。
     *
     * <p><b>为什么第 2 步不能省</b>：{@code readerCount} 只能表达"别删我手上这一段"，
     * 且 {@code minPinnedLsn} 的兜底扫描是"段序列里第一个有读者的段"——
     * 一旦这个段被摘出序列，兜底值会退化为 0，剩余所有段立刻变得可删（级联误删）。
     * LSN 围栏与段序列无关，能把这条路径彻底封住。
     *
     * @param segment 目标段，可为 null
     * @return 已持有该段的读者登记返回 true；该段已被保留策略删除返回 false
     */
    private boolean ensureRetained(Segment segment) {
        if (segment == null) {
            return false;
        }
        if (segment == retainedSegment) {
            // 段在我们持有读者引用的期间被判定可删，只可能发生在"分区被整体冻结/删除"
            // 这类外部生命周期操作上（配置层面的旧数据丢弃）。按"该段已不可读"处理，
            // 由调用方前进到下一个段——绝不在这里静默返回一条错误数据。
            return !segment.isDeleted();
        }
        if (!segment.tryRetainReader()) {
            return false;
        }
        pinAt(segment);
        Segment previous = retainedSegment;
        retainedSegment = segment;
        if (previous != null) {
            previous.releaseReader();
        }
        return true;
    }

    /**
     * 把保留策略的围栏推到 {@code segment} 的保护点（并按需撤销上一个）。
     *
     * <p>保护点取 {@code min(段名 LSN, 段内真实首条记录 LSN)}：段名 LSN 由
     * {@code nameReservations} 发号，空分区反复轮转时它可能<b>大于</b>段内真实首条 LSN
     * （见 {@code SegmentManager#reserveNameLsn}）。只按段名登记会在这种段上
     * 少保护一截，因此这里用两者较小值——多保护无害，少保护会丢数据。
     *
     * @param segment 当前正在读的段
     */
    private void pinAt(Segment segment) {
        long protect = segment.firstLsn();
        long actualFirst = firstRecordLsn(segment);
        if (actualFirst > 0L && actualFirst < protect) {
            protect = actualFirst;
        }
        pinLsn(protect);
    }

    /**
     * 把围栏设到 {@code protect}（幂等；旧的围栏在新围栏生效之后才撤销）。
     *
     * <p>"先 pin 新、再 unpin 旧"的顺序必须保证：反过来的话，两次调用之间围栏值为 0
     * （无保护），保留线程恰好在这一刻评估就会把"游标还没读的数据"合法删掉。
     *
     * @param protect 保护点 LSN，{@code <= 0} 时忽略
     */
    private void pinLsn(long protect) {
        if (protect <= 0L || protect == pinnedLsn) {
            return;
        }
        retentionPin.pin(protect);
        long previous = pinnedLsn;
        pinnedLsn = protect;
        if (previous > 0L) {
            retentionPin.unpin(previous);
        }
    }

    /**
     * 撤销当前围栏（幂等）。
     */
    private void unpinCurrent() {
        long previous = pinnedLsn;
        pinnedLsn = 0L;
        if (previous > 0L) {
            retentionPin.unpin(previous);
        }
    }

    /**
     * 最近一次返回的 LSN（诊断用）。
     *
     * @return LSN，尚未返回过记录时为 0
     */
    public long lastReturnedLsn() {
        return lastReturnedLsn;
    }

    // ==================================================================
    // 内部
    // ==================================================================

    /**
     * 把"名字 LSN ≤ 目标"的候选段向后回退到<b>真正可能包含目标</b>的段。
     *
     * <h2>为什么需要回退（实测过的真实缺陷）</h2>
     * {@link SegmentManager#findSegmentFor} 做的是"名字 LSN 的 floor 二分"，它依赖一条
     * 隐藏不变量：<b>名字 LSN ≤ 目标 LSN 的最后一个段，就是包含目标的段（或更靠后的段）</b>。
     * 一旦段目录里夹着<b>没有数据的空段</b>（轮转内联降级与预建线程抢跑会留下这种空壳），
     * 不变量就破了。实测序列：
     * <pre>
     *   段名 1  → 记录 1..46
     *   段名 2  → 记录 47..92
     *   段名 48 → 记录 93..138
     *   段名 94 → 空段（0 帧）        ← 二分在这里命中
     *   段名 139→ 记录 139..184
     * </pre>
     * {@code seekTo(123)} 会命中空段 94，然后顺序前进到段 139，于是<b>直接返回 139</b>，
     * 123..138 被跳过——读出来的东西是错的，而且没有任何异常。
     *
     * <p>恢复流程现在会删掉这些空段，但<b>游标不能依赖恢复一定跑过</b>（同一进程内刚写完
     * 就可能建游标）。因此这里做一次有界的向后回退：只要当前候选段"是空的"或
     * "首条记录 LSN 已经大于目标"，就往回退一段再看，最多退到 0 号段。
     * 回退步数在健康目录下为 0，代价是一帧帧头读取（冷路径，可接受）。
     *
     * @param index 候选段下标
     * @param lsn 目标 LSN
     * @return 修正后的段下标
     */
    private int stepBackToCoveringSegment(int index, long lsn) {
        int i = Math.min(index, segments.size() - 1);
        while (i > 0) {
            long firstLsn = firstRecordLsn(segments.get(i));
            if (firstLsn > 0L && firstLsn <= lsn) {
                return i;
            }
            i--;
        }
        return Math.max(i, 0);
    }

    /**
     * 读取某段首条记录的 LSN（空段/不可读返回 0）。
     *
     * <p>该段可能<b>已被保留策略删除</b>（游标快照是只增不减的，{@code seekTo} 允许向后
     * 定位到已删区间）：此时句柄已关、映射已撤，读会抛 {@link IOException}（FileChannel 后端）
     * 或返回 0（mmap 后端映射已置空）。两种都归为"该段不可读"，返回 0 让调用方继续回退，
     * <b>绝不能让一次读失败把游标整个打挂</b>——数据被删是配置的预期结果，不是故障。
     *
     * @param segment 段
     * @return LSN；无记录或不可读返回 0
     */
    private long firstRecordLsn(Segment segment) {
        if (segment.publishedWritePos() < FrameLayout.HEADER_LEN) {
            return 0L;
        }
        try {
            smallRead.clear();
            smallRead.limit(FrameLayout.HEADER_LEN);
            int got = segment.handle().read(0L, smallRead, FrameLayout.HEADER_LEN);
            if (got < FrameLayout.HEADER_LEN) {
                return 0L;
            }
            if (smallRead.getInt(FrameLayout.OFFSET_MAGIC) != FrameLayout.MAGIC) {
                return 0L;
            }
            return smallRead.getLong(FrameLayout.OFFSET_LSN);
        } catch (IOException | RuntimeException e) {
            return 0L;
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new WalException.ClosedException("游标已关闭（partitionId=" + partitionId + "）");
        }
    }

    private int indexOf(Segment target) {
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i) == target) {
                return i;
            }
        }
        return -1;
    }

    private void onRecordReturned() {
        lastReturnedLsn = record.lsn();
        nextLsnWanted = record.lsn() + 1L;
        recordsSinceCheckpoint++;
        maybeCheckpoint();
    }

    private void maybeCheckpoint() {
        if (checkpointStore == null) {
            return;
        }
        boolean byRecords = recordsSinceCheckpoint >= CHECKPOINT_RECORDS;
        boolean byTime = options.checkpointIntervalMillis() > 0L
                && (System.nanoTime() - lastCheckpointNanos)
                        >= options.checkpointIntervalMillis() * 1_000_000L;
        if (!byRecords && !byTime) {
            return;
        }
        try {
            checkpointStore.persist(partitionId, nextLsnWanted);
            recordsSinceCheckpoint = 0L;
            lastCheckpointNanos = System.nanoTime();
        } catch (IOException ignored) {
            // 同 close()：不影响读路径
        }
    }

    /**
     * 段内/跨段前进一步，把结果放进 {@link #record}。
     *
     * <h2>两处"不能永久放弃"的边界（tailer 关键）</h2>
     * <ol>
     *   <li>{@code segmentIndex == segments.size()}：说明走过了已知全部段。此时<b>先
     *       刷新段列表</b>（可能刚发生轮转），刷新不到才是真的"追上写入方"；</li>
     *   <li>{@code offset >= end} 且当前<b>已经是最后一个段</b>：不能 {@code segmentIndex++}
     *       一走了之——同一段后续还会被追加。必须原地停在段尾（{@code offset = end}），
     *       下次调用时 {@code end} 增长就自然接着读。</li>
     * </ol>
     *
     * @return 还有记录返回 true；已追上写入方返回 false
     */
    private boolean advanceRaw() {
        while (true) {
            if (segmentIndex >= segments.size()) {
                if (!refreshSegments() || segmentIndex >= segments.size()) {
                    return false;
                }
                offset = 0L;
            }
            Segment segment = segments.get(segmentIndex);
            // 读者登记 + 保留围栏都跟着"当前位置"走：进入一个段就先登记它（并释放上一个），
            // 这样保留策略知道"这个段正在被读、不能删"，而已经读过的旧段可以被删。
            // 返回 false = 该段在登记之前就已被保留策略删除（配置允许丢弃最旧数据），
            // 那就跳到下一个段：绝不能停在原地，也绝不能返回它的残留字节。
            if (!ensureRetained(segment)) {
                segmentIndex++;
                offset = 0L;
                continue;
            }
            long end = segment.publishedWritePos();
            if (offset >= end) {
                if (segmentIndex == segments.size() - 1 && !refreshSegments()) {
                    // 最后一个已知段且没有新段：停在段尾等新数据
                    offset = end;
                    return false;
                }
                segmentIndex++;
                offset = 0L;
                if (rolloverListener != null && segmentIndex < segments.size()) {
                    rolloverListener.onRollover(partitionId,
                            segments.get(segmentIndex).firstLsn());
                }
                continue;
            }
            if (end - offset < FrameLayout.HEADER_LEN) {
                // 剩余字节不足一个帧头：尾部的填充区，跳过到下一段
                segmentIndex++;
                offset = 0L;
                continue;
            }
            int frameLen = readFrameLen(segment, offset);
            if (frameLen < 0 || offset + frameLen > end) {
                segmentIndex++;
                offset = 0L;
                continue;
            }
            int status = decodeAt(segment, offset, frameLen);
            if (status == FrameDecoder.STATUS_PAYLOAD_SINK_TOO_SMALL) {
                // ---- F4：sink 不足绝不当作"损坏"跳段 ----
                // decodeAt 已按帧头 payloadLen 扩容，正常不可能走到这里；万一
                // 出现（未来的新解码路径），跳段等于静默丢弃整段已发布的数据。
                // 停在原地返回"暂无新记录"，让问题以"读不到数据"的形式暴露，
                // 下一次调用仍从同一 offset 重试（扩容可能已生效）。
                return false;
            }
            if (status <= 0) {
                // 解不开：结束本段（正常关闭后不应出现；出现即由恢复流程负责）
                segmentIndex++;
                offset = 0L;
                continue;
            }
            offset += frameLen;
            if (frame.skipped) {
                // 未知 recordType：整帧跳过、不回调（ARCH §4.2）
                continue;
            }
            record.fill(frame.lsn, frame.timestampNanos, frame.partitionId,
                    (byte) frame.recordType, (byte) frame.flags, frame.codecId,
                    frame.attributes, frame.payloadLen, frame.storedLen, frame.payload);
            return true;
        }
    }

    /**
     * 把 {@code SegmentManager} 里新增的段追加进游标快照。
     *
     * <p>段列表是<b>只增不减</b>的（删除分区时整体作废，游标随之失效），因此按
     * "已有个数"往后追即可；仍用 identity 检查兜一层，避免极端情况下重复持有引用。
     *
     * <p><b>这里刻意不登记读者</b>：新段要等游标真正读到它时才
     * {@link #ensureRetained(Segment)}。提前对"未来才读的段"登记读者，等于又回到了
     * "整个分区都被 pin 住"的老问题上（tailer 尤其明显：它会不停 refresh 出所有新段）。
     *
     * @return 快照是否真的变长了
     */
    private boolean refreshSegments() {
        List<Segment> current = segmentManager.segments(partitionId);
        int before = segments.size();
        for (int i = 0; i < current.size(); i++) {
            Segment candidate = current.get(i);
            if (containsIdentity(candidate)) {
                continue;
            }
            segments.add(candidate);
        }
        return segments.size() > before;
    }

    /**
     * 快照里是否已持有该段（identity 比较）。
     *
     * @param target 段
     * @return 已持有返回 true
     */
    private boolean containsIdentity(Segment target) {
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i) == target) {
                return true;
            }
        }
        return false;
    }

    /**
     * 读帧长。优先走映射窗口（零拷贝），失败则回退到 {@code handle.read}。
     */
    private int readFrameLen(Segment segment, long at) {
        ByteBuffer header = readZeroCopy(segment, at, FrameLayout.HEADER_LEN);
        try {
            if (header != null) {
                return header.getInt(FrameLayout.OFFSET_FRAME_LEN);
            }
            smallRead.clear();
            smallRead.limit(FrameLayout.HEADER_LEN);
            int got = segment.handle().read(at, smallRead, FrameLayout.HEADER_LEN);
            if (got < FrameLayout.HEADER_LEN) {
                return -1;
            }
            return smallRead.getInt(FrameLayout.OFFSET_FRAME_LEN);
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * 解码一帧。
     *
     * @return 解码状态（&gt; 0 成功；&lt;= 0 失败）
     */
    private int decodeAt(Segment segment, long at, int frameLen) {
        ByteBuffer source = readZeroCopy(segment, at, frameLen);
        try {
            if (source == null) {
                if (wholeFrame.capacity() < frameLen) {
                    wholeFrame = ByteBuffer.allocate(frameLen);
                }
                wholeFrame.clear();
                wholeFrame.limit(frameLen);
                int got = segment.handle().read(at, wholeFrame, frameLen);
                if (got < frameLen) {
                    return FrameDecoder.STATUS_INCOMPLETE;
                }
                wholeFrame.limit(wholeFrame.capacity());
                source = wholeFrame;
            }
            // ---- F4：按帧头声明的大小扩容 payloadSink ----
            //
            // 旧代码在"容量 < frameLen"时只是 clear + limit(容量)，从不扩容：
            // 非 RAW codec 且 payload > 64KB 时 decode 返回 PAYLOAD_SINK_TOO_SMALL，
            // advanceRaw 把它当损坏处理而跳过整段——静默丢数据。
            // 压缩 codec 下 payloadLen 可以远大于 frameLen（stored 被压小了），
            // 所以取 frameLen 与 payloadLen 的大值；越界/负值的 payloadLen
            // 不参与扩容（decoder 会在结构校验里以 BAD_HEADER 拒掉它，
            // 不会走到 sink 检查），避免恶意帧头触发超大分配。
            int sinkNeed = frameLen;
            int payloadLen = source.getInt(FrameLayout.OFFSET_PAYLOAD_LEN);
            if (payloadLen > 0 && payloadLen <= FrameLayout.MAX_PAYLOAD_LEN) {
                sinkNeed = Math.max(sinkNeed, payloadLen);
            }
            if (payloadSink.capacity() < sinkNeed) {
                payloadSink = ByteBuffer.allocate(sinkNeed);
            }
            return decoder.decode(source, 0, frameLen, frame, payloadSink);
        } catch (IOException e) {
            return FrameDecoder.STATUS_INCOMPLETE;
        }
    }

    /**
     * 通过 {@link MappedWindowCache} 取只读视图；不适用时返回 {@code null}。
     *
     * <p>只对<b>非 mmap 后端</b>启用：mmap 句柄本身就是零拷贝，再套一层窗口只会
     * 多开一份映射（还多占一个文件句柄）。
     */
    private ByteBuffer readZeroCopy(Segment segment, long at, int len) {
        if (windows == null || !options.useMappedWindows() || segment.handle().isMapped()) {
            return null;
        }
        try {
            return windows.acquire(segment.handle().path(), at, len);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
