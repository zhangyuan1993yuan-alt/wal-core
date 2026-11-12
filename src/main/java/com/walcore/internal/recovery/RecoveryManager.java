package com.walcore.internal.recovery;

import com.walcore.api.PartitionInfo;
import com.walcore.api.ReplayListener;
import com.walcore.api.RecoveryReport;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.api.WalRecord;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.metrics.MetricsRegistry;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.partition.PartitionStateFile;
import com.walcore.internal.read.DecodedFrame;
import com.walcore.internal.read.FrameDecoder;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexStore;
import com.walcore.spi.RecordCodec;
import com.walcore.spi.SegmentHandle;
import com.walcore.spi.StorageBackend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 崩溃恢复编排（ARCH §9.1 的 {@code RecoveryManager}）。
 *
 * <h2>一个必须强调的恢复前提</h2>
 * 由于 T05 的 {@code nameReservations} 发号机制，<b>段文件名里的 LSN 可能大于段内第一条
 * 记录的真实 LSN</b>（空分区轮转时文件名按"预留号"生成）。因此本类：
 * <ul>
 *   <li>只用文件名排序来保证<b>段的相对顺序</b>（字典序 ≡ 数值序，T05 已验证）；</li>
 *   <li>段的<b>真实首条 LSN 与逻辑长度一律以扫描结果为准</b>，绝不拿文件名里的 LSN
 *       去推断内容；</li>
 *   <li>分区级 {@code lastLsn} 取所有段扫描结果的最大值，再 {@code +1} 作为
 *       {@code nextLsn}（ARCH §9.5 第 3 条：必须全分区 max 校验，防止分区之间逆序）。</li>
 * </ul>
 *
 * <h2>三条完成判定（ARCH §9.5）</h2>
 * <ol>
 *   <li>每分区 {@code lastGoodOffset} 已确定且文件长度已截断到位；</li>
 *   <li>每分区 {@code .idx} 存在、头 CRC 有效、最大 {@code fileOffset ≤ lastGoodOffset}；</li>
 *   <li>{@code nextLsn} 已计算并经全分区 max 校验，且 {@code wal.meta} 已重写
 *       （第 3 条由 {@code DefaultWal} 在拿到报告后完成）。</li>
 * </ol>
 * 任一不满足则抛 {@link WalException.RecoveryFailedException} 并附诊断。
 */
public final class RecoveryManager {

    private final StorageBackend backend;
    private final PartitionRegistry registry;
    private final SegmentManager segmentManager;
    private final IndexStore indexStore;
    private final RecordCodec codec;
    private final WalConfig config;
    private final MetricsRegistry metrics;

    private final SegmentScanner scanner = new SegmentScanner();
    private final TornWriteDetector detector = new TornWriteDetector();
    private final IndexRebuilder rebuilder = new IndexRebuilder();

    /**
     * 构造恢复管理器。
     *
     * @param backend 存储后端
     * @param registry 分区注册表
     * @param segmentManager 段管理器
     * @param indexStore 索引实现，可为 null
     * @param codec 解码用 codec（重放时需要），可为 null（此时只做完整性恢复）
     * @param config 配置
     * @param metrics 指标注册中心，可为 null
     */
    public RecoveryManager(StorageBackend backend, PartitionRegistry registry,
                           SegmentManager segmentManager, IndexStore indexStore,
                           RecordCodec codec, WalConfig config, MetricsRegistry metrics) {
        this.backend = Preconditions.checkNotNull(backend, "backend");
        this.registry = Preconditions.checkNotNull(registry, "registry");
        this.segmentManager = Preconditions.checkNotNull(segmentManager, "segmentManager");
        this.indexStore = indexStore;
        this.codec = codec;
        this.config = Preconditions.checkNotNull(config, "config");
        this.metrics = metrics;
    }

    /**
     * 执行恢复。
     *
     * @param cleanShutdown 上次是否干净关闭（{@code wal.meta} 里的标记）
     * @param listener 重放回调；为 null 时只做完整性恢复、不重放
     * @return 恢复报告
     * @throws IOException IO 失败
     */
    public RecoveryReport recover(boolean cleanShutdown, ReplayListener listener) throws IOException {
        long started = System.nanoTime();
        Path partitionsDir = config.baseDir().resolve(SegmentNaming.PARTITIONS_DIR);
        List<PartitionDiagnosisBuilder> builders = new ArrayList<>();
        List<RecoveryReport.CorruptionEvent> corruptions = new ArrayList<>();
        long totalTruncated = 0L;
        long maxLsn = 0L;

        boolean skipCrcOnClean = cleanShutdown
                && config.fastRecoveryOnCleanShutdown()
                && config.recoveryVerifyMode() == WalConfig.RecoveryVerifyMode.LAST_SEGMENT;

        for (int partitionId : listPartitionDirs(partitionsDir)) {
            PartitionDiagnosisBuilder builder = recoverPartition(
                    partitionId, skipCrcOnClean, corruptions);
            builders.add(builder);
            totalTruncated += builder.truncatedBytes;
            if (builder.lastLsn > maxLsn) {
                maxLsn = builder.lastLsn;
            }
        }

        long nextLsn = maxLsn + 1L;
        long replayed = 0L;
        if (listener != null && !builders.isEmpty()) {
            replayed = replayAll(listener, builders);
        }
        long elapsed = (System.nanoTime() - started) / 1_000_000L;

        List<RecoveryReport.PartitionDiagnosis> diagnoses = new ArrayList<>(builders.size());
        boolean clean = corruptions.isEmpty() && totalTruncated == 0L;
        for (PartitionDiagnosisBuilder b : builders) {
            clean &= !b.stopped && b.truncatedBytes == 0L;
            diagnoses.add(b.build());
        }
        RecoveryReport report = new RecoveryReport(clean, totalTruncated, replayed,
                diagnoses, corruptions, elapsed, nextLsn);
        if (listener != null) {
            listener.onRecoveryComplete(report);
        }
        return report;
    }

    // ==================================================================
    // 单分区恢复
    // ==================================================================

    /**
     * 恢复单个分区。
     *
     * <h2>为什么必须两遍扫描（"有效尾段"不是一个下标能算出来的）</h2>
     * {@code NextSegmentPreparer} 会在轮转前<b>预建下一段</b>，于是崩溃后磁盘上常见的形态是：
     * <pre>
     *   wal-p0000-...001.wal   ← 真正有数据（20 条，1620 字节）
     *   wal-p0000-...002.wal   ← 预建出来但一条都没写（全零，4096 字节）
     * </pre>
     * 若按"文件名最后一个 = 尾段"来判断，上面这个<b>有数据的段</b>就会被判成"中间段损坏"，
     * 于是每次崩溃重启都直接拒绝启动——这是最严重的可用性事故（数据完好却打不开）。
     *
     * <p>所以这里先扫完全部段，再取"<b>最后一条有帧的段</b>"作为有效尾段：
     * <ul>
     *   <li>下标 &lt; tailIndex 的段损坏 → 硬失败（中间段损坏 = 历史已确认数据丢失）；</li>
     *   <li>下标 == tailIndex 的段损坏 → 按 {@code CorruptionPolicy} 处理；</li>
     *   <li>下标 &gt; tailIndex 的段（预建未使用）→ 允许全零；若完全没有帧就<b>删掉文件</b>，
     *       否则每次崩溃都会多留一个 4096B 垃圾段，长期运行会积累出上万个空段。</li>
     * </ul>
     *
     * @param partitionId 分区 ID
     * @param skipCrcOnClean 干净关闭时是否跳过逐帧 CRC
     * @param corruptions 损坏事件收集器
     * @return 诊断构建器
     * @throws IOException IO 失败
     */
    private PartitionDiagnosisBuilder recoverPartition(int partitionId, boolean skipCrcOnClean,
                                                       List<RecoveryReport.CorruptionEvent> corruptions)
            throws IOException {
        Path dir = SegmentNaming.partitionDir(config.baseDir(), partitionId);
        List<SegmentFileRef> files = listSegments(partitionId, dir);
        PartitionDiagnosisBuilder builder = new PartitionDiagnosisBuilder(partitionId, files.size());
        if (files.isEmpty()) {
            return builder;
        }

        PartitionImpl partition = registry.get(partitionId);
        if (partition == null) {
            partition = new PartitionImpl(partitionId);
            try {
                registry.register(partition);
            } catch (WalException.PartitionException e) {
                throw new WalException.RecoveryFailedException(
                        "恢复期注册分区失败: pid=" + partitionId, e);
            }
        }
        // 把上次进程写下的分区状态接回来：FREEZE/ARCHIVE 是<b>运维意图</b>，
        // 不能因为重启就自动复活成可写分区（否则上游会往已下线的品种继续写）。
        PartitionInfo.State restored = PartitionStateFile.read(config.baseDir(), partitionId);
        if (restored != null && restored != partition.state()) {
            partition.transitionTo(restored);
        }

        // ---------- 第一遍：只扫描，不改文件 ----------
        int n = files.size();
        long[] physicalLengths = new long[n];
        SegmentScanner.ScanResult[] scans = new SegmentScanner.ScanResult[n];
        for (int i = 0; i < n; i++) {
            SegmentFileRef ref = files.get(i);
            physicalLengths[i] = physicalLengthOf(ref);
            // 干净关闭时的 CRC 跳过只对"有效尾段"有意义，而此时还不知道谁是尾段；
            // 所以干净关闭时先按"末个文件"给一次机会，真正的判定在第二遍用
            // scan.skippedBytes / 损坏事实来做（跳过 CRC 只影响速度，不影响正确性）。
            boolean verifyCrc = !(i == n - 1 && skipCrcOnClean)
                    || config.recoveryVerifyMode() == WalConfig.RecoveryVerifyMode.FULL;
            int resyncWindow = config.corruptionPolicy() == WalConfig.CorruptionPolicy.SKIP_RECORD
                    ? config.resyncWindowBytes() : 0;
            scans[i] = scanSegment(ref, physicalLengths[i], verifyCrc, resyncWindow,
                    partitionId, corruptions);
        }

        // 有效尾段 = 最后一条"至少扫到一帧"的段
        int tailIndex = -1;
        for (int i = 0; i < n; i++) {
            if (scans[i].frameCount > 0L) {
                tailIndex = i;
            }
        }
        if (tailIndex < 0) {
            // 整个分区一条完整记录都没有（全新目录，或数据区被彻底摧毁）。
            // 此时"尾段"应该是最后一条<b>有损坏迹象</b>的段——不能简单取最后一个文件，
            // 否则"数据段被截断到 40 字节 + 后面还有个预建空段"会被误判成
            // "中间段损坏"而拒绝启动（数据本来就没了，启动失败更是雪上加霜）。
            for (int i = n - 1; i >= 0; i--) {
                if (scans[i].stopped
                        && !TornWriteDetector.SUMMARY_ZERO_TAIL.equals(scans[i].stopRule)) {
                    tailIndex = i;
                    break;
                }
            }
            if (tailIndex < 0) {
                tailIndex = n - 1;
            }
        }

        // ---------- 第二遍：按策略处置 + 重开段 ----------
        for (int i = 0; i < n; i++) {
            SegmentFileRef ref = files.get(i);
            SegmentScanner.ScanResult scan = scans[i];
            boolean isTail = (i == tailIndex);
            boolean noFrames = scan.frameCount == 0L && scan.lastGoodOffset == 0L;

            if (i > tailIndex) {
                // 有效尾段之后还排着文件：只能是"预建但没用上"的空段（很常见），
                // 或者名字序与内容序矛盾（异常）。前者直接删，后者必须喊出来——
                // 静默处理会让"数据写进了错误的段"永远不被发现。
                if (noFrames && !scan.stopped) {
                    deleteSegmentFiles(partitionId, ref);
                    continue;
                }
                throw new WalException.RecoveryFailedException(
                        "尾段之后仍存在数据段（名字序与内容序矛盾）: pid=" + partitionId
                                + ", file=" + ref.path.getFileName()
                                + ", frames=" + scan.frameCount, null);
            }
            if (noFrames && !scan.stopped && i != tailIndex) {
                // 中间夹着的"预建空段"。这类文件是 rotateLocked 的内联降级与预建线程
                // 抢跑时留下的（已观察到每轮转泄漏一个）。它们<b>没有数据</b>，
                // 但会破坏 seek 的一条核心不变量："名字 LSN ≤ 目标 LSN 的最后一个段
                // 就是包含目标的段"。留着它们会让 seekTo 落进空段、跳到下一段，
                // 从而越过真正包含目标 LSN 的段（实测：seekTo(123) 读到 139）。
                deleteSegmentFiles(partitionId, ref);
                continue;
            }

            if (scan.stopped || scan.resyncCount > 0L) {
                if (!isTail) {
                    // 中间段损坏：绝不静默截断、也绝不静默重同步。重同步会把坏帧"跳过去"，
                    // 表面上启动成功了，实际却少了一条<b>早已 ACK 过</b>的记录，
                    // 而且 LSN 序列里出现空洞——下游按 LSN 幂等重放会永久错位。
                    // 这类情况只能人工介入（拷走现场、从备份恢复），没有"自动修好"的选项。
                    throw new WalException.CorruptedException(
                            "非最后段损坏，拒绝静默截断: pid=" + partitionId
                                    + ", file=" + ref.path.getFileName()
                                    + ", offset=" + scan.stopOffset
                                    + ", rule=" + scan.stopRule
                                    + (scan.resyncCount > 0L
                                            ? ", resync=" + scan.resyncCount + " 次" : "")
                                    + "（中间段损坏意味着历史已确认数据丢失）");
                }
                if (scan.stopped && config.corruptionPolicy() == WalConfig.CorruptionPolicy.FAIL_FAST) {
                    throw new WalException.CorruptedException(
                            "检测到损坏（FAIL_FAST 策略，文件未被修改）: pid=" + partitionId
                                    + ", file=" + ref.path.getFileName()
                                    + ", offset=" + scan.stopOffset
                                    + ", rule=" + scan.stopRule);
                }
                if (scan.stopped) {
                    // TAIL_TRUNCATE / SKIP_RECORD：把尾部垃圾<b>真正删掉</b>（逻辑 + 物理）。
                    //
                    // 早期版本只做"逻辑截断"（写位置回到 lastGoodOffset，文件长度不变），
                    // 理由是"段是 SPARSE 预分配的，把文件截短会让后续 mmap 写在文件末尾之外
                    // （Linux 上 SIGBUS）"。但纯逻辑截断有一个致命缺陷：<b>不幂等</b>——
                    // 尾部垃圾一直留在盘上，于是下一次重启会再次把它判成损坏；一旦这次恢复
                    // 之后又写入了新段，这个"带着垃圾的旧段"就变成了"非最后段"，
                    // 恢复流程会直接拒绝启动。等于"崩一次能起来，崩两次起不来"。
                    //
                    // 现在的做法：先记录 truncatedBytes，再对文件做物理截断。
                    // mmap 后端在 truncate 后会立刻把长度补回容量并重新映射
                    // （见 MappedSegmentHandle#truncate），因此不存在"写在 EOF 之外"的风险。
                    builder.truncatedBytes += Math.max(0L, physicalLengths[i] - scan.lastGoodOffset);
                    builder.stopped = true;
                    builder.detail = "尾部截断 @ " + scan.stopOffset + " (" + scan.stopRule + ')';
                    if (metrics != null) {
                        metrics.increment(MetricsRegistry.CORRUPTION_COUNT);
                    }
                    truncatePhysically(ref, scan.lastGoodOffset);
                } else {
                    // SKIP_RECORD 在尾段内部重同步成功：尾部本身是完好的，
                    // 只是中间有若干坏帧被跳过。这种情况下**不能**截断文件
                    // （lastGoodOffset 已经是数据区末尾），只如实记账。
                    builder.stopped = true;
                    builder.detail = "跳过 " + scan.resyncCount + " 处损坏（共 "
                            + scan.skippedBytes + " 字节）";
                    if (metrics != null) {
                        metrics.increment(MetricsRegistry.CORRUPTION_COUNT);
                    }
                }
            }

            long logicalLength = scan.lastGoodOffset;
            if (isTail) {
                builder.lastGoodOffset = logicalLength;
                builder.lastLsn = scan.lastLsn;
                builder.lastSegmentFirstLsn = ref.nameFirstLsn;
            } else if (scan.lastLsn > builder.lastLsn) {
                builder.lastLsn = scan.lastLsn;
            }
            builder.frames += scan.frameCount;

            // 索引校验/重建（只对有效尾段做全量重建，其余段只丢弃越界项）
            if (indexStore != null) {
                boolean rebuilt = ensureIndex(partitionId, ref, indexStore,
                        logicalLength, isTail);
                builder.indexRebuilt |= rebuilt;
            }

            // 恢复期一律"以可写句柄打开 → 立刻封存为只读"：
            //   - 用可写句柄是为了 sealToReadOnly 里的 force(true) 能生效；
            //   - 封存是为了让已恢复的数据不可变，同时避免 SPARSE 预分配下
            //     "逻辑长度 ≠ 物理长度" 导致续写位置错位（详见 SegmentManager.clearActive）。
            Segment recovered = segmentManager.reopenSegment(
                    partitionId, ref.nameFirstLsn, true, logicalLength);
            // 扫描结果才是权威：把条数与末条 LSN 补回段状态，否则重开后
            // WalStats.totalRecords 归零、PartitionInfo.lastLsn 显示 0（实测过）。
            recovered.restoreCounters(scan.frameCount, scan.lastLsn);
            recovered.sealToReadOnly(logicalLength);
        }
        // 全部既有段都只读 → 清空活跃指针，让上层新开一段写入
        if (builder.segmentCount > 0) {
            segmentManager.clearActive(partitionId);
        }
        return builder;
    }

    /**
     * 把段文件物理截断到 {@code newLength}（恢复期"真的删掉尾部垃圾"）。
     *
     * <p>用可写句柄打开后调用 {@code truncate}，因此后端可以自行处理"mmap 中不能 truncate"
     * 的约束（{@code MappedSegmentHandle} 会 unmap → truncate → 补回容量 → 重新映射）。
     * 失败不抛：数据正确性已由"逻辑长度"保证，物理垃圾最坏只是下次再被判一次损坏。
     *
     * @param ref 段引用
     * @param newLength 目标长度
     */
    private void truncatePhysically(SegmentFileRef ref, long newLength) {
        SegmentHandle handle = null;
        try {
            handle = backend.open(ref.path, ref.nameFirstLsn, true);
            handle.truncate(newLength);
        } catch (IOException e) {
            if (metrics != null) {
                metrics.increment("wal.recovery.truncate.failed");
            }
        } finally {
            if (handle != null) {
                try {
                    handle.close();
                } catch (IOException ignored) {
                    // 关闭失败不改变恢复结论
                }
            }
        }
    }

    /**
     * 读取段文件的物理长度（用可写句柄打开，SPARSE 预分配下可顺带把长度补齐）。
     *
     * @param ref 段引用
     * @return 物理长度
     * @throws IOException IO 失败
     */
    private long physicalLengthOf(SegmentFileRef ref) throws IOException {
        SegmentHandle handle = backend.open(ref.path, ref.nameFirstLsn, true);
        try {
            long len = handle.physicalLength();
            return Math.max(len, 0L);
        } finally {
            handle.close();
        }
    }

    /**
     * 删除一个"没有任何记录"的段文件（连同它的 {@code .idx}）。
     *
     * <p>这类文件是轮转内联降级与预建线程抢跑时留下的空壳：{@code backend.create} 已经把
     * {@code .tmp} 原子改名为 {@code .wal}，但那一份最终没被采用。它们的危害是
     * 破坏"段名 LSN ↔ 段内容"的单调对应关系，进而让 {@code findSegmentFor} 的
     * 二分落空（详见调用点注释）。空文件不承载任何数据，删除是安全的。
     *
     * @param partitionId 分区 ID
     * @param ref 段引用
     */
    private void deleteSegmentFiles(int partitionId, SegmentFileRef ref) {
        // 先让索引层放下这个段，否则 .idx 句柄会拖住删除
        if (indexStore != null) {
            try {
                indexStore.openSegment(partitionId, ref.nameFirstLsn, 0L);
                indexStore.closeSegment(ref.nameFirstLsn);
            } catch (RuntimeException ignored) {
                // 索引侧清理失败不影响数据删除
            }
        }
        try {
            backend.delete(ref.path);
        } catch (IOException e) {
            // 删不掉（Windows 映射未释放等）不算错误：它只是个空段，下次启动再试
            if (metrics != null) {
                metrics.increment("wal.recovery.emptysegment.delete.failed");
            }
        }
    }

    private SegmentScanner.ScanResult scanSegment(SegmentFileRef ref, long physicalLength,
                                                  boolean verifyCrc, int resyncWindow,
                                                  int partitionId,
                                                  List<RecoveryReport.CorruptionEvent> corruptions)
            throws IOException {
        SegmentHandle handle = backend.open(ref.path, ref.nameFirstLsn, false);
        try {
            return scanner.scan(handle, 0L, physicalLength, verifyCrc, resyncWindow, null,
                    (atOffset, skipped, rule) -> corruptions.add(
                            new RecoveryReport.CorruptionEvent(partitionId,
                                    ref.path.getFileName().toString(), atOffset, skipped, rule,
                                    "SKIP_RECORD 重同步")));
        } finally {
            handle.close();
        }
    }

    private boolean ensureIndex(int partitionId, SegmentFileRef ref, IndexStore store,
                                long logicalLength, boolean isLast) throws IOException {
        // 必须先告诉索引"现在在处理哪个段"，否则它的"当前段"还是未知（firstLsn=0），
        // 任何重建/查询都会去算一个非法的 .idx 文件名。
        store.openSegment(partitionId, ref.nameFirstLsn, Math.max(logicalLength, 0L));

        boolean needRebuild = !rebuilder.isConsistent(store, logicalLength);
        if (!needRebuild && isLast) {
            long lastIndexed = store.lastIndexedLsn();
            needRebuild = lastIndexed <= 0L && logicalLength > 0L;
        }
        if (!needRebuild && !isLast) {
            // 非最后段：只丢弃越界项，不必重建
            return false;
        }
        SegmentHandle handle = backend.open(ref.path, ref.nameFirstLsn, false);
        try {
            IndexRebuilder.RebuildResult result = rebuilder.rebuild(
                    partitionId, handle, store, config.indexIntervalRecords());
            if (metrics != null) {
                metrics.increment("wal.index.rebuild.count");
            }
            return result.entries >= 0;
        } finally {
            handle.close();
        }
    }

    // ==================================================================
    // 全局升序重放
    // ==================================================================

    /**
     * 按 LSN 全局升序重放各分区记录（k 路归并，k = 分区数）。
     */
    private long replayAll(ReplayListener listener, List<PartitionDiagnosisBuilder> builders)
            throws IOException {
        List<PartitionReader> readers = new ArrayList<>();
        FrameDecoder decoder = new FrameDecoder(
                codec == null ? new com.walcore.internal.spiimpl.RawRecordCodec() : codec,
                config.allowForwardCompatibleRead());
        for (PartitionDiagnosisBuilder b : builders) {
            List<Segment> segments = segmentManager.segments(b.partitionId);
            if (segments.isEmpty()) {
                continue;
            }
            PartitionReader reader = new PartitionReader(b.partitionId, segments, decoder);
            if (reader.advance()) {
                readers.add(reader);
            }
        }
        long replayed = 0L;
        long progressStep = 100_000L;
        while (!readers.isEmpty()) {
            PartitionReader min = null;
            for (PartitionReader r : readers) {
                if (min == null || r.currentLsn() < min.currentLsn()) {
                    min = r;
                }
            }
            WalRecord record = min.currentRecord();
            listener.onRecord(record);
            replayed++;
            if (replayed % progressStep == 0L) {
                listener.onProgress(min.partitionId(), record.lsn(), replayed);
            }
            try {
                if (!min.advance()) {
                    readers.remove(min);
                }
            } catch (IOException e) {
                throw new WalException.RecoveryFailedException(
                        "重放分区 " + min.partitionId() + " 失败（lsn=" + record.lsn() + "）", e);
            }
        }
        return replayed;
    }

    /**
     * 分区级"下一条记录"读取器。只读 {@code [0, publishedWritePos)}，不会读到半条记录。
     */
    private static final class PartitionReader {

        private final int partitionId;
        private final List<Segment> segments;
        private final FrameDecoder decoder;
        private final WalRecord record = new WalRecord();
        private final DecodedFrame frame = new DecodedFrame();
        private final ByteBuffer header = ByteBuffer.allocate(FrameLayout.HEADER_LEN);
        /** F4：可增长（非 RAW codec + 大 payload / 压缩 codec 的解压落点）。 */
        private ByteBuffer payloadSink = ByteBuffer.allocate(64 * 1024);
        private ByteBuffer entire = ByteBuffer.allocate(64 * 1024);

        private int segmentIndex;
        private long offset;
        private boolean ready;

        PartitionReader(int partitionId, List<Segment> segments, FrameDecoder decoder) {
            this.partitionId = partitionId;
            this.segments = segments;
            this.decoder = decoder;
        }

        int partitionId() {
            return partitionId;
        }

        long currentLsn() {
            return record.lsn();
        }

        WalRecord currentRecord() {
            return record;
        }

        /**
         * 前进到下一条可读记录。
         *
         * @return 还有记录返回 true
         * @throws IOException 读取失败
         */
        boolean advance() throws IOException {
            while (segmentIndex < segments.size()) {
                Segment segment = segments.get(segmentIndex);
                long end = segment.publishedWritePos();
                if (offset >= end) {
                    segmentIndex++;
                    offset = 0L;
                    continue;
                }
                SegmentHandle handle = segment.handle();
                if (end - offset < FrameLayout.HEADER_LEN) {
                    segmentIndex++;
                    offset = 0L;
                    continue;
                }
                header.clear();
                header.limit(FrameLayout.HEADER_LEN);
                int gotHeader = handle.read(offset, header, FrameLayout.HEADER_LEN);
                if (gotHeader < FrameLayout.HEADER_LEN) {
                    segmentIndex++;
                    offset = 0L;
                    continue;
                }
                header.limit(header.capacity());
                int frameLen = header.getInt(FrameLayout.OFFSET_FRAME_LEN);
                if (!FrameLayout.isValidFrameLen(frameLen) || offset + frameLen > end) {
                    segmentIndex++;
                    offset = 0L;
                    continue;
                }
                if (entire.capacity() < frameLen) {
                    entire = ByteBuffer.allocate(frameLen);
                }
                entire.clear();
                entire.limit(frameLen);
                int got = handle.read(offset, entire, frameLen);
                if (got < frameLen) {
                    segmentIndex++;
                    offset = 0L;
                    continue;
                }
                entire.limit(entire.capacity());
                // ---- F4：按帧头声明的大小扩容 payloadSink（与 CursorImpl.decodeAt 同源问题）----
                // 压缩 codec 下 payloadLen 可远大于 frameLen；旧代码从不扩容，
                // 大 payload 帧会以 PAYLOAD_SINK_TOO_SMALL 失败，这里把它当
                // 损坏帧跳过（offset += frameLen），恢复出来的 nextLsn 就低了——
                // 等于恢复期静默丢数据。越界/负值的 payloadLen 不参与扩容。
                int sinkNeed = frameLen;
                int payloadLen = entire.getInt(FrameLayout.OFFSET_PAYLOAD_LEN);
                if (payloadLen > 0 && payloadLen <= FrameLayout.MAX_PAYLOAD_LEN) {
                    sinkNeed = Math.max(sinkNeed, payloadLen);
                }
                if (payloadSink.capacity() < sinkNeed) {
                    payloadSink = ByteBuffer.allocate(sinkNeed);
                }
                int status = decoder.decode(entire, 0, frameLen, frame, payloadSink);
                if (status <= 0) {
                    // 解不开的帧（损坏/版本过高）：跳过，交给完整性校验去报
                    offset += Math.max(frameLen, FrameLayout.MIN_FRAME_LEN);
                    continue;
                }
                offset += frameLen;
                if (frame.skipped) {
                    // 未知 recordType：整帧跳过，不回调
                    continue;
                }
                record.fill(frame.lsn, frame.timestampNanos, frame.partitionId,
                        (byte) frame.recordType, (byte) frame.flags, frame.codecId,
                        frame.attributes, frame.payloadLen, frame.storedLen, frame.payload);
                ready = true;
                return true;
            }
            ready = false;
            return false;
        }

        boolean isReady() {
            return ready;
        }
    }

    // ==================================================================
    // 磁盘枚举
    // ==================================================================

    private static final class SegmentFileRef {
        private final Path path;
        private final long nameFirstLsn;

        SegmentFileRef(Path path, long nameFirstLsn) {
            this.path = path;
            this.nameFirstLsn = nameFirstLsn;
        }
    }

    private static final class PartitionDiagnosisBuilder {
        private final int partitionId;
        private final int segmentCount;
        private long lastSegmentFirstLsn;
        private long lastGoodOffset;
        private long lastLsn;
        private long truncatedBytes;
        private long frames;
        private boolean indexRebuilt;
        private boolean stopped;
        private String detail;

        PartitionDiagnosisBuilder(int partitionId, int segmentCount) {
            this.partitionId = partitionId;
            this.segmentCount = segmentCount;
        }

        RecoveryReport.PartitionDiagnosis build() {
            return new RecoveryReport.PartitionDiagnosis(partitionId, segmentCount,
                    lastSegmentFirstLsn, lastGoodOffset, lastLsn, truncatedBytes,
                    indexRebuilt, detail);
        }
    }

    private static int[] listPartitionDirs(Path partitionsDir) throws IOException {
        if (!Files.isDirectory(partitionsDir)) {
            return new int[0];
        }
        List<Integer> ids = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(partitionsDir, "p*")) {
            for (Path p : stream) {
                if (!Files.isDirectory(p)) {
                    continue;
                }
                String name = p.getFileName().toString();
                if (name.length() != 5 || name.charAt(0) != 'p') {
                    continue;
                }
                try {
                    ids.add(Integer.parseInt(name.substring(1)));
                } catch (NumberFormatException ignored) {
                    // 非 pXXXX 形式的目录，跳过
                }
            }
        }
        ids.sort(Comparator.naturalOrder());
        int[] out = new int[ids.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = ids.get(i);
        }
        return out;
    }

    private static List<SegmentFileRef> listSegments(int partitionId, Path dir)
            throws IOException {
        List<SegmentFileRef> refs = new ArrayList<>();
        if (!Files.isDirectory(dir)) {
            return refs;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.wal")) {
            for (Path p : stream) {
                Optional<Long> firstLsn = SegmentNaming.parseFirstLsn(p.getFileName().toString());
                if (firstLsn.isEmpty()) {
                    continue;
                }
                refs.add(new SegmentFileRef(p, firstLsn.get()));
            }
        }
        // 字典序 ≡ 数值序（T05 已验证），这里显式按 nameFirstLsn 排序以表达意图
        refs.sort(Comparator.comparingLong(r -> r.nameFirstLsn));
        return refs;
    }
}
