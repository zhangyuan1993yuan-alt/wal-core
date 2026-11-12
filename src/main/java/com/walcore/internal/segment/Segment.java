package com.walcore.internal.segment;

import com.walcore.api.WalException;
import com.walcore.internal.util.Preconditions;
import com.walcore.spi.IndexStore;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 单个段的运行时状态：写指针水位、封存长度、状态机、索引引用、读者引用计数。
 *
 * <h2>本类在 T05 一次定型（TASKS §1 并行冲突预警）</h2>
 * 公开方法集被刻意限制为"6 个状态方法 + 2 个访问器"，后续任务（T06 索引 / T07 组提交 /
 * T08 管线 / T09 游标 / T10 恢复）<b>只调用不修改</b>，避免多任务同时改一个类导致状态机错乱。
 * <pre>
 *   appendFrame / reserve / publishPosition / force / sealToReadOnly / hasRoom
 *   handle() / index()
 * </pre>
 *
 * <h2>两个水位别搞混</h2>
 * <ul>
 *   <li>{@link SegmentHandle#appendPosition()}：<b>段句柄</b>层面的已写入边界，由 IO 后端以
 *       release 语义发布。</li>
 *   <li>{@link #publishedWritePos()}：<b>段</b>层面的对外可见边界（本类以独立 VarHandle
 *       release 发布）。读者以 acquire 读它，保证只看到 {@code [0, publishedWritePos)}
 *       内的完整帧。两者应当恒等，独立保存是为了让"读者可见性"不依赖具体 IO 实现。</li>
 * </ul>
 *
 * <h2>状态机</h2>
 * {@code ACTIVE -> READONLY}（轮转时封存，之后任何 {@code appendFrame} 抛
 * {@link IllegalStateException}）；{@code READONLY -> FROZEN}（分区冻结/删除时释放映射）。
 * <b>不可逆</b>。
 */
public final class Segment {

    /**
     * 段状态。
     *
     * <p>{@link #DELETE_PENDING} 是保留策略删除路径上的<b>中间态</b>：它表示"这一段已经被
     * 判定为可删，正在 unmap/删文件"。它存在的唯一目的是把
     * <b>"检查 {@code readerCount == 0}"与"读者登记"放进同一把锁</b>，
     * 从而消除 check-then-act 窗口（详见 {@link #beginDelete()} 与
     * {@link #tryRetainReader()} 的注释）。
     */
    public enum SegmentState {
        /** 可写。 */
        ACTIVE,
        /** 已写满并 force 过，只读；长度被 {@code finalLength} 冻结。 */
        READONLY,
        /** 保留策略已判定可删、正在回收（读者不得再登记）。 */
        DELETE_PENDING,
        /** 分区被冻结/删除，映射已释放（只保留句柄元数据）。 */
        FROZEN
    }

    private static final VarHandle PUBLISHED_WRITE_POS;

    static {
        try {
            PUBLISHED_WRITE_POS = MethodHandles.lookup().findVarHandle(
                    Segment.class, "publishedWritePos", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final int partitionId;
    private final long firstLsn;
    private final SegmentHandle handle;
    private final long createdMillis;
    private final AtomicInteger readerCount = new AtomicInteger();
    private final AtomicReference<SegmentState> state =
            new AtomicReference<>(SegmentState.ACTIVE);

    private SparseIndex index;
    private long publishedWritePos;
    private volatile long finalLength = -1L;
    private volatile long lastLsn;
    private volatile long recordCount;
    private volatile int indexIntervalRecords = 1000;

    /** 外部索引插件（可选，逐条 onAppended 回调）。 */
    private volatile IndexStore externalIndex;

    /**
     * 构造段运行时。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录 LSN（命名用；真实首条 LSN 由扫描得出）
     * @param handle 段句柄
     */
    public Segment(int partitionId, long firstLsn, SegmentHandle handle) {
        this.partitionId = partitionId;
        this.firstLsn = firstLsn;
        this.handle = Preconditions.checkNotNull(handle, "handle");
        this.createdMillis = System.currentTimeMillis();
        PUBLISHED_WRITE_POS.setRelease(this, handle.appendPosition());
    }

    // ------------------------------------------------------------------
    // 写路径
    // ------------------------------------------------------------------

    /**
     * 检查是否还有足够空间容纳 {@code frameLen} 字节。
     *
     * <p>语义对齐"帧不跨段"硬约束：{@code remaining < frameLen} 时必须轮转，
     * <b>不做跨段拆分</b>。代价是每段最多浪费 16MB，换来读取与恢复逻辑大幅简化。
     *
     * @param frameLen 帧长
     * @return 有空间返回 true
     */
    public boolean hasRoom(int frameLen) {
        return state.get() == SegmentState.ACTIVE
                && publishedWritePos() + frameLen <= handle.capacityBytes();
    }

    /**
     * 返回当前可写位置（不预留、不前进）。供管线在写入前记录起始偏移。
     *
     * @return 段内字节偏移
     */
    public long reserve() {
        return publishedWritePos();
    }

    /**
     * 校验 {@code frameLen} 能放下并返回当前可写位置。
     *
     * @param frameLen 帧长
     * @return 段内字节偏移
     * @throws IllegalStateException 段已封存或空间不足
     */
    public long reserve(int frameLen) {
        SegmentState s = state.get();
        if (s != SegmentState.ACTIVE) {
            throw new IllegalStateException(
                    "段已不可写（state=" + s + "）: pid=" + partitionId + ", firstLsn=" + firstLsn);
        }
        long pos = publishedWritePos();
        if (pos + frameLen > handle.capacityBytes()) {
            throw new IllegalStateException(
                    "段空间不足: pid=" + partitionId + ", firstLsn=" + firstLsn
                            + ", pos=" + pos + ", frameLen=" + frameLen
                            + ", capacity=" + handle.capacityBytes());
        }
        return pos;
    }

    /**
     * 恢复期把"已有多少条记录 / 最后一条 LSN"补回段状态。
     *
     * <h2>为什么必须补</h2>
     * {@link #recordCount} 与 {@link #lastLsn} 只由 {@code appendFrame} 递增，
     * 而恢复期是"扫描已有文件"，不走写路径。若不补：
     * <ul>
     *   <li>{@code WalStats.totalRecords()} 在每次重启后归零，监控曲线变成锯齿；</li>
     *   <li>{@code PartitionInfo.lastLsn} 显示 0，"最后一条记录 LSN"这个最常用的
     *       运维指标直接失真（实测重开后 {@code lastLsn=0} 而文件里有 20 条记录）。</li>
     * </ul>
     * 调用方（{@code RecoveryManager}）用扫描结果填充，因此这里的值是<b>权威</b>的。
     *
     * @param restoredRecords 段内已有记录条数（&ge; 0）
     * @param restoredLastLsn 段内最后一条记录的 LSN（&gt; 0）
     */
    public void restoreCounters(long restoredRecords, long restoredLastLsn) {
        this.recordCount = Math.max(restoredRecords, 0L);
        this.lastLsn = Math.max(restoredLastLsn, this.lastLsn);
    }

    /**
     * 追加一条完整帧，并推进 {@code publishedWritePos}（release 语义）。
     *
     * @param frame 整帧
     * @return 追加后新的 {@code appendPosition}
     * @throws IOException IO 失败
     * @throws IllegalStateException 段已封存
     */
    public long appendFrame(ByteBuffer frame) throws IOException {
        SegmentState s = state.get();
        if (s != SegmentState.ACTIVE) {
            throw new IllegalStateException(
                    "段已不可写（state=" + s + "）: pid=" + partitionId + ", firstLsn=" + firstLsn);
        }
        handle.append(frame);
        long pos = handle.appendPosition();
        publishPosition(pos);
        recordCount++;
        return pos;
    }

    /**
     * 追加一条完整帧并维护稀疏索引与 {@code lastLsn}。
     *
     * <p>索引写入频率只有 {@code 1/indexIntervalRecords}（默认 0.1%），
     * 因此 {@link SparseIndex#append} 内部的小临界区不在 append 主路径的成本量级上
     * （ARCH §3.2 明确允许索引使用 {@code synchronized}）。
     *
     * @param frame 整帧
     * @param lsn 该帧的全局 LSN
     * @return 追加后新的 {@code appendPosition}
     * @throws IOException IO 失败
     */
    public long appendFrame(ByteBuffer frame, long lsn) throws IOException {
        long startPos = handle.appendPosition();
        long newPos = appendFrame(frame);
        this.lastLsn = lsn;
        SparseIndex idx = this.index;
        if (idx != null) {
            long count = recordCount;
            if (count == 1L || count % indexIntervalRecords == 0L) {
                idx.append(lsn, startPos);
            }
        }
        // 外部 IndexStore 插件（ARCH 契约修订，T11）：逐条通知，使其 lookup 真正可用。
        // 内置 DefaultIndexStore 的默认实现是空方法，JIT 会把它完全内联掉。
        IndexStore external = this.externalIndex;
        if (external != null) {
            external.onAppended(partitionId, firstLsn, lsn, startPos);
        }
        return newPos;
    }

    /**
     * 挂载外部索引插件（逐条 {@code onAppended} 回调给它）。
     *
     * @param store 索引插件，可为 null（内置稀疏索引已由 {@link #attachIndex} 处理）
     */
    public void attachIndexStore(IndexStore store) {
        this.externalIndex = store;
    }

    /**
     * 以 release 语义发布新的写位置。
     *
     * @param newPos 新的已写入边界
     */
    public void publishPosition(long newPos) {
        PUBLISHED_WRITE_POS.setRelease(this, newPos);
    }

    /**
     * 刷盘。Active 段走 {@code force(false)}；封存时走 {@code force(true)}。
     *
     * @param metaData 是否刷元数据
     * @throws IOException IO 失败
     */
    public void force(boolean metaData) throws IOException {
        handle.force(metaData);
    }

    /**
     * 封存为只读：先 {@code force(true)}，再冻结长度，最后迁移状态。
     *
     * <p>顺序不能变：必须"数据先落盘，再把长度冻结"，否则崩溃后可能出现
     * "文件里比 finalLength 多的字节"或反之，恢复期判定会摇摆。
     *
     * @param length 封存长度（通常取封存瞬间的 {@code publishedWritePos}）
     * @throws IOException IO 失败
     */
    public void sealToReadOnly(long length) throws IOException {
        SegmentState cur = state.get();
        if (cur == SegmentState.READONLY || cur == SegmentState.FROZEN
                || cur == SegmentState.DELETE_PENDING) {
            return; // 幂等（DELETE_PENDING 期间不得再改动长度）
        }
        handle.force(true);
        this.finalLength = length;
        publishPosition(length);
        state.set(SegmentState.READONLY);
    }

    /**
     * 冻结：释放映射，仅保留元数据（分区删除/冻结时调用）。
     */
    public void freeze() {
        SegmentState cur = state.get();
        if (cur == SegmentState.FROZEN) {
            return;
        }
        if (finalLength < 0L) {
            finalLength = publishedWritePos();
        }
        state.set(SegmentState.FROZEN);
        try {
            handle.close();
        } catch (IOException e) {
            throw new WalException.IoException(
                    "冻结段时关闭句柄失败: " + handle.path(), e);
        }
    }

    /**
     * 关闭句柄（释放映射），不改变状态。
     *
     * @throws IOException IO 失败
     */
    public void closeHandle() throws IOException {
        handle.close();
    }

    // ------------------------------------------------------------------
    // 读路径与访问器
    // ------------------------------------------------------------------

    /**
     * 以 acquire 语义读取已发布的写位置。读者<b>只能读</b> {@code [0, 该值)}。
     *
     * @return 已写入边界
     */
    public long publishedWritePos() {
        return (long) PUBLISHED_WRITE_POS.getAcquire(this);
    }

    /**
     * 段句柄。
     *
     * @return 句柄
     */
    public SegmentHandle handle() {
        return handle;
    }

    /**
     * 稀疏索引（可能为 null，例如只读恢复阶段尚未挂载）。
     *
     * @return 索引或 null
     */
    public SparseIndex index() {
        return index;
    }

    /**
     * 挂载稀疏索引。
     *
     * @param sparseIndex 索引
     */
    public void attachIndex(SparseIndex sparseIndex) {
        this.index = sparseIndex;
    }

    /**
     * 设置索引粒度（每多少条记录一个索引项）。
     *
     * @param interval 条数，必须为正
     */
    public void indexIntervalRecords(int interval) {
        Preconditions.checkPositive(interval, "indexIntervalRecords");
        this.indexIntervalRecords = interval;
    }

    /**
     * 分区 ID。
     *
     * @return 分区 ID
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * 段内首条记录 LSN（命名值）。
     *
     * @return LSN
     */
    public long firstLsn() {
        return firstLsn;
    }

    /**
     * 已写入的末条记录 LSN。
     *
     * @return LSN，尚未写入返回 0
     */
    public long lastLsn() {
        return lastLsn;
    }

    /**
     * 已写入记录条数。
     *
     * @return 条数
     */
    public long recordCount() {
        return recordCount;
    }

    /**
     * 段状态。
     *
     * @return 状态
     */
    public SegmentState state() {
        return state.get();
    }

    /**
     * 封存长度；未封存返回 -1。
     *
     * @return 长度
     */
    public long finalLength() {
        return finalLength;
    }

    /**
     * 创建时刻（epoch 毫秒），供保留策略按时间判断。
     *
     * @return 毫秒
     */
    public long createdMillis() {
        return createdMillis;
    }

    /**
     * 段容量。
     *
     * @return 字节数
     */
    public long capacityBytes() {
        return handle.capacityBytes();
    }

    /**
     * 是否可写。
     *
     * @return ACTIVE 返回 true
     */
    public boolean isActive() {
        return state.get() == SegmentState.ACTIVE;
    }

    /**
     * 增加读者引用（删除前必须检查归零）。
     *
     * <p><b>已进入删除流程（{@link SegmentState#DELETE_PENDING}）或已冻结的段不会再被登记</b>：
     * 这两种状态下句柄可能已经被关闭，再把读者计数抬起来只会制造"看似有人读、
     * 实际读到关闭句柄"的假象。此时返回当前计数（不递增），调用方若需要知道
     * "登记是否成功"应改用 {@link #tryRetainReader()}。
     *
     * @return 新的引用计数（登记失败时为当前值）
     */
    public synchronized int retainReader() {
        if (!retainable()) {
            return readerCount.get();
        }
        return readerCount.incrementAndGet();
    }

    /**
     * 尝试登记为读者（<b>与删除决策互斥</b>）。
     *
     * <h2>为什么必须与 {@link #beginDelete()} 用同一把锁</h2>
     * 保留策略的删除判定是"{@code readerCount == 0} 就删"。如果读者在
     * "读到计数为 0"与"真正把计数加一"之间存在窗口，就会出现
     * <b>删除方刚检查完、读者刚登记成功、文件已被删</b>的交错——读者拿到的是关闭的句柄，
     * 表现为"偶发读不动/跳过一段"，而且<b>没有任何异常能定位到保留策略</b>。
     *
     * <p>把两个动作放进同一个段级监视器后，只可能有两种交错：读者先（计数变为 1 →
     * 删除方放弃本轮、下轮重试）或删除方先（状态变 DELETE_PENDING → 读者登记失败、
     * 由读路径按"该段已被保留策略删除"处理）。不存在第三种。
     *
     * <p>代价：只在<b>跨段</b>时付一次无竞争监视器进入（每段一次，不是每条记录一次），
     * 与 {@code append} 热路径无关。
     *
     * @return 登记成功返回 true；该段已冻结/正在被删除返回 false
     */
    public synchronized boolean tryRetainReader() {
        if (!retainable()) {
            return false;
        }
        readerCount.incrementAndGet();
        return true;
    }

    /**
     * 释放读者引用（<b>不会减到负数</b>）。
     *
     * <p>负数计数曾经是一个真实事故的形态：早期游标"构造期逐段登记、close 时逐段释放"，
     * 一旦登记粒度改成"只登记当前段"，逐段释放就会把未登记的段减成负数，
     * 于是 {@code readerCount > 0} 的判定对<b>所有</b>段为真，保留策略静默失效。
     * 这里做钳位，宁可记一个偏大的计数也不能为负。
     *
     * @return 新的引用计数
     */
    public synchronized int releaseReader() {
        int now = readerCount.decrementAndGet();
        if (now < 0) {
            readerCount.set(0);
            return 0;
        }
        return now;
    }

    /**
     * 开始删除：<b>在同一把锁内</b>确认"状态可删 + 无读者"，然后把状态推进到
     * {@link SegmentState#DELETE_PENDING}。
     *
     * <p>一旦返回 {@code true}，之后任何 {@link #tryRetainReader()} 都会失败，
     * 因此调用方可以放心 unmap 与删文件，不会再有新读者进来看见半个段。
     * 返回 {@code false} 表示"本轮跳过"（仍有读者、仍是活跃段、或已冻结），
     * 保留策略下一轮再试——<b>等待或抢占都是错的</b>：等待会拖住保留线程，
     * 抢占会让读者拿到 {@code ClosedChannelException}。
     *
     * @return 允许删除返回 true
     */
    public synchronized boolean beginDelete() {
        if (state.get() != SegmentState.READONLY) {
            return false;
        }
        if (readerCount.get() != 0) {
            return false;
        }
        state.set(SegmentState.DELETE_PENDING);
        return true;
    }

    /**
     * 撤销删除（{@link #beginDelete()} 之后、真正 unmap 之前出错时回滚）。
     *
     * <p>只有"还没有关闭句柄"的阶段才能回滚；句柄一旦关闭，段就是不可用的，
     * 必须走完删除流程（否则会留下一个"在段序列里但读不出来"的幽灵段）。
     */
    public synchronized void cancelDelete() {
        if (state.get() == SegmentState.DELETE_PENDING) {
            state.set(SegmentState.READONLY);
        }
    }

    /**
     * 是否还能被登记为读者（{@code ACTIVE} 或 {@code READONLY}）。
     *
     * @return 可登记返回 true
     */
    private boolean retainable() {
        SegmentState s = state.get();
        return s == SegmentState.ACTIVE || s == SegmentState.READONLY;
    }

    /**
     * 是否已被保留策略删除（含"正在删除"）。
     *
     * @return 已删/删除中返回 true
     */
    public boolean isDeleted() {
        SegmentState s = state.get();
        return s == SegmentState.FROZEN || s == SegmentState.DELETE_PENDING;
    }

    /**
     * 当前读者数。
     *
     * @return 数量
     */
    public int readerCount() {
        return readerCount.get();
    }

    /**
     * 段标识（日志/异常消息用）。
     *
     * @return 形如 {@code p0003@131072}
     */
    public String tag() {
        return "p" + String.format(java.util.Locale.ROOT, "%04d", partitionId) + "@" + firstLsn;
    }

    @Override
    public String toString() {
        return "Segment{" + tag()
                + ", state=" + state.get()
                + ", publishedWritePos=" + publishedWritePos()
                + ", lastLsn=" + lastLsn
                + ", records=" + recordCount
                + ", finalLength=" + finalLength
                + ", readers=" + readerCount.get()
                + '}';
    }
}
