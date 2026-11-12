package com.walcore.internal.append;

import com.walcore.internal.util.Preconditions;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * 脏分区位图 + 脏字节/条数计数：<b>写线程与组提交线程之间唯一的通信机制</b>。
 *
 * <h2>为什么是"位图 + 计数"而不是队列</h2>
 * 写入线程绝不能为了"通知刷盘"而分配对象或进队列（ARCH §3.3、§7.1）：
 * <ul>
 *   <li>{@code ArrayBlockingQueue}：约 80ns + 每记录一个节点对象 → GC 压力，
 *       违背"堆内对象趋近 0"；</li>
 *   <li>{@code ConcurrentLinkedQueue}：约 30ns + 每记录一个 {@code Node}；</li>
 *   <li><b>位图</b>：一次 {@code getAndSet} 语义的 {@code set}（约 1~2ns），
 *       <b>零分配</b>，且天然去重——同一分区连续写 100 万条，位图里只有 1 bit。</li>
 * </ul>
 * 通信成本：生产者 1 次 {@code AtomicLongArray.set}，消费者 {@code wordCount} 次
 * {@code getAndSet(0)}。即使 4096 个分区也只有 64 个 word。
 *
 * <h2>为什么 {@code getAndSet(0)} 而不是 {@code get()} + {@code set(0)}</h2>
 * 后者会<b>丢失</b>"取走之后、清零之前"新置的脏位——那部分数据将永远不被刷盘，
 * 直到下一次同分区写入，表现为"偶发 flushAndWait 超时"。
 * {@code getAndSet} 是单个原子读-改-写，新置的位只会落到"下一轮"，绝不丢。
 *
 * <h2>两个高水位</h2>
 * <ul>
 *   <li>{@code dirtyBytes > maxDirtyBytes}（默认 32MB）：fsync 追不上写导致 page cache
 *       无界增长的防线；</li>
 *   <li>同一分区待刷记录数：用于"高负载下不等 200µs 时间窗"的强制唤醒。</li>
 * </ul>
 * 两者都只是<b>建议</b>（由 {@code AppendPipeline} 决定是否背压），本类不阻塞任何人——
 * 一旦在这里阻塞，append 路径就出现了不可控的等待点。
 */
public final class DirtyTracker {

    private static final int BITS_PER_WORD = 64;

    /** 脏分区位图：bit i 置位表示分区 i 有未刷盘的写入。 */
    private final AtomicLongArray dirtyMask;

    /** 全局待刷字节数。 */
    private final AtomicLong dirtyBytes = new AtomicLong();

    /**
     * 全局待刷记录数（F8：LongAdder，用于强制唤醒判断）。
     *
     * <p>这个计数被<b>每条 append</b> 打一次：多写入线程并发 RMW 同一个
     * {@code AtomicLong} 会在同一条 cache line 上乒乓（每次 RMW 都是
     * 读-改-写 + 缓存一致性广播）。{@code LongAdder} 把增量摊到每线程/每槽
     * 条带里，写入侧只碰自己的 Cell，读取侧（{@code sum()}）才汇总——
     * "写多读少"正是这里的访问模式（读只发生在 flusher 判定唤醒与 toString）。
     *
     * <p>语义妥协：{@code sumThenReset()} 在汇总与清零之间不是原子的，清零瞬间
     * 并发 {@code increment()} 的增量可能被一并清掉（漏记，不会多记）。这个
     * 计数只用于"高负载下是否提前唤醒 flusher"的<b>建议性</b>判断，漏记的
     * 后果最多是延迟一个 200µs 窗口；权威的"哪些分区还有脏数据"在
     * {@link #dirtyMask}（原子位图，不丢）与 {@link #pendingRecords}
     * （每分区计数，force 后精确清零）。
     */
    private final LongAdder pendingRecordCount = new LongAdder();

    /** 每分区的待刷记录数（用于单分区背压与强制唤醒）。 */
    private final AtomicLongArray pendingRecords;

    private final int maxPartitions;
    private final long maxDirtyBytes;

    /**
     * 构造脏追踪器。
     *
     * @param maxPartitions 分区 ID 上限（决定位图 word 数）
     * @param maxDirtyBytes 脏字节高水位（{@code <=0} 表示不限制）
     */
    public DirtyTracker(int maxPartitions, long maxDirtyBytes) {
        Preconditions.checkInRange(maxPartitions, 1, 65_536, "maxPartitions");
        this.maxPartitions = maxPartitions;
        this.dirtyMask = new AtomicLongArray(wordCountFor(maxPartitions));
        this.pendingRecords = new AtomicLongArray(maxPartitions);
        this.maxDirtyBytes = maxDirtyBytes;
    }

    /**
     * 只取走"属于我"的脏位（多 flusher 场景，按 {@code pid % count == index} 分片）。
     *
     * <h2>为什么必须"只取自己的"，而不是"取全部再把别人的放回去"</h2>
     * 后者有<b>活锁/饥饿</b>风险：flusher A 每轮都把 flusher B 的分区抢走再放回，
     * 若 A 的唤醒频率高于 B，B 可能长期抢不到自己那份，表现为"某些分区的
     * {@code flushAndWait} 偶发超时"——而这个现象只在多 flusher + 单分区高写时出现，
     * 极难定位（本工程实测踩过一次）。
     * 正确做法：每个 flusher 只对<b>自己的位掩码</b>做原子 clear，彼此不重叠。
     *
     * <p>空闲路径是纯 {@code get}：先读一次判断"有没有我的位"，没有就跳过，
     * 避免每 200µs 对 64 个 word 各做一次 CAS。
     *
     * @param out 输出数组（长度需 {@code >= wordCount()}）
     * @param myMask 本 flusher 的位掩码（长度 {@code >= wordCount()}，由调用方预计算）
     * @return 非零 word 的最高下标 + 1；0 表示本轮没有本 flusher 的脏分区
     */
    public int drainMine(long[] out, long[] myMask) {
        Preconditions.checkNotNull(out, "out");
        Preconditions.checkNotNull(myMask, "myMask");
        int upper = 0;
        for (int w = 0; w < dirtyMask.length(); w++) {
            long mine = myMask[w];
            if (mine == 0L) {
                out[w] = 0L;
                continue;
            }
            if ((dirtyMask.get(w) & mine) == 0L) {
                out[w] = 0L;
                continue;
            }
            long taken = dirtyMask.getAndUpdate(w, cur -> cur & ~mine);
            out[w] = taken & mine;
            if (out[w] != 0L) {
                upper = w + 1;
            }
        }
        return upper;
    }

    /**
     * 计算某个 flusher 在某个 word 上负责的位掩码。
     *
     * <p>{@code count == 1} 时返回全 1（单 flusher 不需要分片）。
     *
     * @param word word 下标
     * @param index flusher 下标
     * @param count flusher 总数
     * @param maxPartitions 分区上限（用于裁掉越界位）
     * @return 位掩码
     */
    public static long bitsForWord(int word, int index, int count, int maxPartitions) {
        if (count <= 1) {
            long all = -1L;
            int firstPid = word << 6;
            if (firstPid + 64 > maxPartitions) {
                int valid = maxPartitions - firstPid;
                all = valid >= 64 ? -1L : ((1L << valid) - 1L);
            }
            return all;
        }
        long mask = 0L;
        for (int bit = 0; bit < 64; bit++) {
            int pid = (word << 6) + bit;
            if (pid >= maxPartitions) {
                break;
            }
            if (pid % count == index) {
                mask |= 1L << bit;
            }
        }
        return mask;
    }

    /**
     * 给定分区上限时需要的 word 数。
     *
     * @param maxPartitions 分区上限
     * @return word 数
     */
    public static int wordCountFor(int maxPartitions) {
        return (maxPartitions + BITS_PER_WORD - 1) / BITS_PER_WORD;
    }

    /**
     * 标记某分区为脏（<b>写线程热路径</b>：1 次原子 OR + 2 次计数，零分配）。
     *
     * <p><b>必须用原子 {@code OR}，不能用 {@code set}</b>：位图是"按 word 打包 64 个分区"的，
     * {@code set} 会把同一 word 里其他分区的脏位<b>整片覆盖掉</b>。
     * 一个写入线程先写 pid0 再写 pid1 时，pid0 的脏位就会消失 → 那个分区永远不被刷盘
     * → {@code flushAndWait} 一直超时。这类 bug 只在"同一 word 内多分区交替写"时出现，
     * 单分区压测完全测不出来（本工程实测发现过一次，见 T07 验收记录）。
     *
     * @param partitionId 分区 ID
     * @param bytes 本次写入的字节数
     */
    public void markDirty(int partitionId, int bytes) {
        if (partitionId < 0 || partitionId >= maxPartitions) {
            return;
        }
        redirty(partitionId);
        dirtyBytes.addAndGet(bytes);
        pendingRecordCount.increment();
        pendingRecords.incrementAndGet(partitionId);
    }

    /**
     * 只把脏位置回去（不动任何计数），用于 force 失败后的重试标记。
     *
     * <p>失败重试时数据并没有"又写了一遍"，所以不能走 {@link #markDirty}——
     * 那会把 {@code pendingRecords} 越滚越大，让背压判断失真。
     *
     * @param partitionId 分区 ID
     */
    public void redirty(int partitionId) {
        if (partitionId < 0 || partitionId >= maxPartitions) {
            return;
        }
        dirtyMask.accumulateAndGet(partitionId >>> 6, bitOf(partitionId), (a, b) -> a | b);
    }

    /**
     * 取走整张位图并清零（消费者侧，每批一次）。
     *
     * @param out 输出数组，长度必须 {@code >= wordCount()}
     * @return 非零 word 的最高下标 + 1（即需要遍历的上界）；0 表示本轮无脏分区
     */
    public int drain(long[] out) {
        Preconditions.checkNotNull(out, "out");
        Preconditions.checkArgument(out.length >= wordCount(), "out 长度不足: %s < %s",
                out.length, wordCount());
        int upper = 0;
        for (int w = 0; w < dirtyMask.length(); w++) {
            long word = dirtyMask.getAndSet(w, 0L);
            out[w] = word;
            if (word != 0L) {
                upper = w + 1;
            }
        }
        return upper;
    }

    /**
     * 单 word 版 {@link #drain(long[])}（仅 {@code maxPartitions <= 64} 时可用）。
     *
     * @return 脏位图
     * @throws UnsupportedOperationException 分区数超过 64
     */
    public long drainMask() {
        if (dirtyMask.length() != 1) {
            throw new UnsupportedOperationException(
                    "drainMask() 只适用于 <= 64 个分区；当前 " + maxPartitions
                            + " 个分区请用 drain(long[])");
        }
        return dirtyMask.getAndSet(0, 0L);
    }

    /**
     * 取走并清零脏字节计数。
     *
     * @return 上一轮累计的脏字节
     */
    public long drainBytes() {
        return dirtyBytes.getAndSet(0L);
    }

    /**
     * 取走并清零全局待刷记录数。
     *
     * <p>注意（F8）：{@code sumThenReset()} 非原子——清零瞬间并发新增的计数可能
     * 被一并清掉。该计数是唤醒判断的<b>建议值</b>，权威脏分区集合在位图里，
     * 见字段声明的语义说明。
     *
     * @return 记录数
     */
    public long drainPendingRecords() {
        return pendingRecordCount.sumThenReset();
    }

    /**
     * 某个分区当前待刷记录数。
     *
     * @param partitionId 分区 ID
     * @return 条数
     */
    public long pendingRecords(int partitionId) {
        if (partitionId < 0 || partitionId >= maxPartitions) {
            return 0L;
        }
        return pendingRecords.get(partitionId);
    }

    /**
     * 清零某分区的待刷记录数（该分区被 force 之后）。
     *
     * @param partitionId 分区 ID
     */
    public void clearPendingRecords(int partitionId) {
        if (partitionId < 0 || partitionId >= maxPartitions) {
            return;
        }
        pendingRecords.set(partitionId, 0L);
    }

    /**
     * 全局待刷记录数（不清零）。
     *
     * @return 条数
     */
    public long totalPendingRecords() {
        return pendingRecordCount.sum();
    }

    /**
     * 全局待刷字节数（不清零）。
     *
     * @return 字节数
     */
    public long pendingBytes() {
        return dirtyBytes.get();
    }

    /**
     * 是否超过脏字节高水位（写线程据此背压）。
     *
     * @return 超过返回 true
     */
    public boolean overHighWatermark() {
        return maxDirtyBytes > 0L && dirtyBytes.get() > maxDirtyBytes;
    }

    /**
     * 脏字节高水位。
     *
     * @return 字节数
     */
    public long maxDirtyBytes() {
        return maxDirtyBytes;
    }

    /**
     * word 数。
     *
     * @return 数量
     */
    public int wordCount() {
        return dirtyMask.length();
    }

    /**
     * 分区上限。
     *
     * @return 数量
     */
    public int maxPartitions() {
        return maxPartitions;
    }

    /**
     * 遍历掩码中的脏分区：返回 {@code >= fromPid} 的第一个脏分区 ID。
     *
     * @param mask 由 {@link #drain(long[])} 填充的数组
     * @param fromPid 起始分区 ID（含）
     * @return 脏分区 ID；没有则返回 -1
     */
    public int nextDirtyPartition(long[] mask, int fromPid) {
        if (fromPid < 0) {
            fromPid = 0;
        }
        int w = fromPid >>> 6;
        if (w >= mask.length) {
            return -1;
        }
        long bits = mask[w] & (-1L << (fromPid & 63));
        while (true) {
            if (bits != 0L) {
                return (w << 6) + Long.numberOfTrailingZeros(bits);
            }
            w++;
            if (w >= mask.length) {
                return -1;
            }
            bits = mask[w];
        }
    }

    /**
     * 分区 ID 对应的位掩码。
     *
     * @param partitionId 分区 ID
     * @return 单个 bit 的 long
     */
    public static long bitOf(int partitionId) {
        return 1L << (partitionId & 63);
    }

    /**
     * 重置全部计数（仅测试与恢复期使用）。
     */
    public void reset() {
        for (int w = 0; w < dirtyMask.length(); w++) {
            dirtyMask.set(w, 0L);
        }
        dirtyBytes.set(0L);
        pendingRecordCount.reset();
        for (int i = 0; i < pendingRecords.length(); i++) {
            pendingRecords.set(i, 0L);
        }
    }

    @Override
    public String toString() {
        return "DirtyTracker{words=" + dirtyMask.length()
                + ", pendingBytes=" + pendingBytes()
                + ", pendingRecords=" + totalPendingRecords()
                + ", highWatermark=" + maxDirtyBytes + '}';
    }
}
