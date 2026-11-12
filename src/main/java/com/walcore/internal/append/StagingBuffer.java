package com.walcore.internal.append;

import com.walcore.api.WalException;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.internal.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 有界<b>堆外暂存区</b>：{@code FileChannel} 后端专用（mmap 后端不需要它）。
 *
 * <h2>为什么 FileChannel 后端需要 staging</h2>
 * <ul>
 *   <li>mmap 后端：{@code append} 就是一次 {@code memcpy} 到页缓存，零系统调用，
 *       逐条写完全没问题。</li>
 *   <li>{@code FileChannel} 后端：每次 {@code write} 都是一次系统调用（约 1~2µs），
 *       逐条写会把 p50 直接打到微秒级。<b>攒批</b>后一次 {@code write} 写几十 KB，
 *       系统调用被摊薄到每条 20~40ns。</li>
 * </ul>
 * 因此本类只服务于 {@code supportsZeroCopyAppend()==false} 的后端。
 *
 * <h2>为什么必须有界（并且会阻塞）</h2>
 * 无界缓冲意味着"写入速度 > 落盘速度"时堆外内存无限增长 → 最终 OOM 或被 OOM killer
 * 干掉。有界后超额只能等：{@link #awaitSpace} 会 park 到刷盘进度推进或超时，
 * 超时抛 {@link WalException.BackpressureException}（由 {@code AppendPipeline}
 * 按 {@code BackpressurePolicy} 决定是 FAIL 还是 BLOCK）。
 *
 * <h2>单写者约束</h2>
 * {@link #reserve} 与 {@link #commit} 必须由<b>同一线程</b>成对调用：
 * {@code reserve} 返回的偏移只有在 {@code commit} 之前有效（中间可能发生压缩）。
 * 这是刻意的取舍——多写者需要 CAS 抢占区间，会把每次 append 的成本从 ~5ns 抬到 ~30ns，
 * 而 WAL 的单分区写入本来就由分区写锁串行化。
 *
 * <h2>压缩策略</h2>
 * 线性缓冲 + 前移压缩（而不是环形缓冲）：环形缓冲的"跨界拆分写入"会让一次逻辑写入
 * 变成两次 {@code put}，还要处理"帧被切成两段"，复杂度高且容易出错。
 * 线性缓冲在"整批写完后"把未刷部分前移，实测分摊成本可忽略（每 4MB 一次 8MB 的
 * {@code arraycopy} ≈ 1ms/1024 次写 = 1µs/次... 实际用 {@code ByteBuffer} 的
 * 绝对拷贝，4MB 约 300µs，摊到 8 万条记录上 ≈ 4ns/条）。
 */
public final class StagingBuffer implements AutoCloseable {

    private final ByteBuffer buffer;
    private final int capacity;

    /** 已提交（可被刷盘线程读取）的上界。 */
    private final AtomicInteger writePos = new AtomicInteger();

    /** 已被刷盘线程确认刷到哪（可回收的上界）。 */
    private final AtomicInteger flushedPos = new AtomicInteger();

    private final AtomicLong backpressureCount = new AtomicLong();
    private final AtomicLong backpressureNanos = new AtomicLong();

    /** 当前在 {@link #awaitSpace} 中等待的写线程（单写者，最多 1 个）。 */
    private volatile Thread waiter;

    private volatile boolean closed;

    /**
     * 构造暂存区。
     *
     * @param capacityBytes 容量（字节），必须为正
     */
    public StagingBuffer(int capacityBytes) {
        Preconditions.checkPositive(capacityBytes, "capacityBytes");
        this.capacity = capacityBytes;
        this.buffer = MemoryUtils.allocateDirect(capacityBytes);
    }

    /**
     * 预留 {@code len} 字节的写入区间（空间不足时先等待）。
     *
     * <p>返回的是<b>绝对下标</b>，调用方用
     * {@code MemoryUtils.putByte/putLong/copy(..., dstIndex=offset, ...)} 写入。
     * 返回值在 {@link #commit(int)} 之前有效。
     *
     * @param len 需要的字节数
     * @param timeoutMillis 等待空间的上限
     * @return 写入起始偏移
     * @throws WalException.BackpressureException 超时仍无空间或 {@code len > capacity}
     * @throws WalException.ClosedException 已关闭
     */
    public int reserve(int len, long timeoutMillis) {
        // 热路径：用 if + throw 代替可变参数校验，确保零分配
        if (len <= 0) {
            throw new IllegalArgumentException("len 必须为正: " + len);
        }
        if (closed) {
            throw new WalException.ClosedException("staging buffer 已关闭");
        }
        if (len > capacity) {
            throw new WalException.BackpressureException(
                    "单条记录 " + len + " 字节超过 staging buffer 容量 " + capacity
                            + "（调大 stagingBufferBytes 或改用 mmap 后端）");
        }
        int pos = writePos.get();
        if (pos + len > capacity) {
            // 先尝试压缩（把 [flushedPos, writePos) 前移到 0）
            compact();
            pos = writePos.get();
        }
        if (pos + len > capacity) {
            awaitSpace(len, timeoutMillis);
            pos = writePos.get();
            if (pos + len > capacity) {
                compact();
                pos = writePos.get();
            }
        }
        return pos;
    }

    /**
     * 提交已写入的区间，推进可读上界（release 语义由 {@code AtomicInteger.set} 保证）。
     *
     * @param len 实际写入字节数
     */
    public void commit(int len) {
        Preconditions.checkArgument(len >= 0, "len 不得为负");
        if (len == 0) {
            return;
        }
        int pos = writePos.get();
        int next = pos + len;
        if (next > capacity) {
            throw new IllegalStateException(
                    "commit 越界: pos=" + pos + ", len=" + len + ", capacity=" + capacity);
        }
        writePos.set(next);
    }

    /**
     * 刷盘线程确认已刷到 {@code pos}，此后 {@code [0, pos)} 可被回收。
     *
     * @param pos 已刷盘位置
     * @return 回收后的可写偏移
     */
    public int flushedTo(int pos) {
        if (pos > flushedPos.get()) {
            flushedPos.set(Math.min(pos, writePos.get()));
        }
        return writePos.get();
    }

    /**
     * 把未刷部分前移到缓冲区开头，回收前导空间。
     *
     * <p>只允许单写者调用（与 {@link #reserve} 同一线程）。
     */
    private void compact() {
        int flushed = flushedPos.get();
        int write = writePos.get();
        if (flushed <= 0) {
            return;
        }
        int remaining = write - flushed;
        if (remaining > 0) {
            // 用绝对下标拷贝，避免改动 position/limit（多线程共享同一个 ByteBuffer 时最安全）
            ByteBuffer src = buffer.duplicate();
            src.position(flushed);
            src.limit(write);
            ByteBuffer dst = buffer.duplicate();
            dst.position(0);
            dst.put(src);
        }
        writePos.set(remaining);
        flushedPos.set(0);
    }

    /**
     * 等待至少 {@code len} 字节可用空间。
     *
     * @param len 需要的字节数
     * @param timeoutMillis 超时
     * @throws WalException.BackpressureException 超时
     */
    public void awaitSpace(int len, long timeoutMillis) {
        long started = System.nanoTime();
        long deadline = started + Math.max(1L, timeoutMillis) * 1_000_000L;
        while (writePos.get() + len > capacity) {
            if (closed) {
                throw new WalException.ClosedException("staging buffer 已关闭");
            }
            int flushed = flushedPos.get();
            if (flushed > 0) {
                return; // 冒泡到调用方去压缩
            }
            long remain = deadline - System.nanoTime();
            if (remain <= 0L) {
                backpressureCount.incrementAndGet();
                backpressureNanos.addAndGet(System.nanoTime() - started);
                throw new WalException.BackpressureException(
                        "staging buffer 等待空间超时: 需要 " + len + " 字节，可用 "
                                + (capacity - writePos.get()) + " 字节，timeout="
                                + timeoutMillis + "ms");
            }
            waiter = Thread.currentThread();
            try {
                LockSupport.parkNanos(this, Math.min(remain, 500_000L));
            } finally {
                waiter = null;
            }
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                throw new WalException.BackpressureException(
                        "等待 staging buffer 空间时被中断（需要 " + len + " 字节）");
            }
        }
    }

    /**
     * 唤醒可能在 {@link #awaitSpace} 中等待的写线程。
     *
     * <p>由刷盘线程在每批 {@link #flushedTo(int)} 之后调用。即使 {@code unpark}
     * 恰好丢失，等待方最长 500µs 后也会因 {@code parkNanos} 超时醒来自行复查，
     * 不会永久挂死。
     */
    public void wakeUpWaiters() {
        Thread t = waiter;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * 缓冲区（供调用方用绝对下标写入/读取）。
     *
     * @return 直接缓冲区
     */
    public ByteBuffer buffer() {
        return buffer;
    }

    /**
     * 已提交的可读上界。
     *
     * @return 偏移
     */
    public int position() {
        return writePos.get();
    }

    /**
     * 已被确认刷盘的上界。
     *
     * @return 偏移
     */
    public int flushedPosition() {
        return flushedPos.get();
    }

    /**
     * 容量。
     *
     * @return 字节数
     */
    public int capacity() {
        return capacity;
    }

    /**
     * 是否还有未刷数据。
     *
     * @return 有返回 true
     */
    public boolean hasPending() {
        return writePos.get() > flushedPos.get();
    }

    /**
     * 背压触发次数（{@code wal.backpressure.count} 的原始来源）。
     *
     * @return 次数
     */
    public long backpressureCount() {
        return backpressureCount.get();
    }

    /**
     * 背压累计等待时间（纳秒）。
     *
     * @return 纳秒
     */
    public long backpressureNanos() {
        return backpressureNanos.get();
    }

    /**
     * 重置（关闭前清空，仅单写者可用）。
     */
    public void reset() {
        writePos.set(0);
        flushedPos.set(0);
    }

    /**
     * 关闭并释放堆外内存。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        LockSupport.unpark(Thread.currentThread());
        MemoryUtils.unmap(buffer);
    }

    /**
     * 是否已关闭。
     *
     * @return 关闭返回 true
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "StagingBuffer{capacity=" + capacity
                + ", writePos=" + writePos.get()
                + ", flushedPos=" + flushedPos.get()
                + ", backpressure=" + backpressureCount.get() + '}';
    }
}
