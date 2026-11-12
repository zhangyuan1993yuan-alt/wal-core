package com.walcore.internal.io;

import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * mmap 段句柄：{@code 活跃段 = 一次映射满整个 segmentSizeBytes，永不 remap}。
 *
 * <h2>最关键的一条纪律：{@code force()} 走 {@link FileChannel#force}</h2>
 * <b>绝不</b>使用 {@link MappedByteBuffer#force()}。原因（ARCH §16.4 / TASKS 坑 #5）：
 * <ol>
 *   <li>{@code MappedByteBuffer.force()} 是 {@code msync(MS_SYNC)}，它只保证"这一份映射"的
 *       脏页落盘，语义与 {@code fdatasync(fd)} 不完全等价；同一文件既有 mmap 又有
 *       {@link FileChannel} 写入时，两套刷新路径会让水位线变得不可推理。</li>
 *   <li>Windows 上 {@code FlushViewOfFile} 与 {@code FlushFileBuffers} 的差异更明显，
 *       混用会导致"以为刷了其实没刷"的假持久性。</li>
 *   <li>统一到 {@code FileChannel.force(false)}（即 {@code fdatasync}）后，
 *       三后端的持久化语义完全一致，{@code durableLsn} 才可推理。</li>
 * </ol>
 * mmap 只负责"写数据"（一次 memcpy，无系统调用），不负责刷盘。
 *
 * <h2>{@code close()} 必须 unmap</h2>
 * Windows 上被映射的文件<b>无法删除</b>（{@code Files.delete} 抛
 * {@code AccessDeniedException}）。因此 {@link #close()} 一定调用
 * {@link MemoryUtils#unmapWithRetry};失败时把路径交给调用方登记到 {@code pendingDelete}。
 *
 * <h2>容量限制</h2>
 * {@link ByteBuffer} 的索引是 {@code int}，因此单段容量上限为 {@code Integer.MAX_VALUE}
 * （2GB）。默认 256MB 远低于此限；{@code create()} 会显式拒绝超限配置。
 */
public final class MappedSegmentHandle implements SegmentHandle {

    private static final VarHandle APPEND_POSITION;

    static {
        try {
            APPEND_POSITION = MethodHandles.lookup().findVarHandle(
                    MappedSegmentHandle.class, "appendPosition", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Path path;
    private final long firstLsn;
    private final long capacity;
    private final FileChannel channel;
    private final boolean writable;

    /** 已发布的可追加位置，写方 setRelease、读方 getAcquire。 */
    private long appendPosition;

    /**
     * 当前映射（F6：必须 volatile）。
     *
     * <p>写方路径：构造器初次映射、{@link #truncate} 先 unmap 再重映射、
     * {@link #unmapQuietly}（close 路径）置 null。读方路径：{@link #append} /
     * {@link #read} 先把字段读到局部变量再解引用。若字段是普通引用，重映射/关闭
     * 与并发读之间没有 happens-before：读线程可能无限期看到<b>已被 unmap 的旧引用</b>
     * （对已 unmap 的 MappedByteBuffer 解引用在部分平台是 native 崩溃），或者看到
     * truncate 中"先置 null 再重映射"的中间态。
     *
     * <p>volatile 保证：unmapQuietly 置 null 之后，任何新发起的读都必然看到 null
     * 并走"未就绪/空读"分支。它<b>不能</b>彻底消除"读线程在置 null 前一瞬拿到旧
     * 引用、随后旧映射被 unmap"的窗口——那个窗口由上层（close 前先拿分区写锁、
     * truncate 只发生在恢复期单线程）负责关闭，这里的可见性是同一防御的第三层。
     */
    private volatile MappedByteBuffer mapped;
    private volatile boolean closed;

    /**
     * 打开或创建一个 mmap 段句柄。
     *
     * @param path 段路径
     * @param firstLsn 段内首条记录 LSN
     * @param capacity 段容量（≤ {@link Integer#MAX_VALUE}）
     * @param writable 是否可写
     * @throws IOException 打开/映射失败
     */
    public MappedSegmentHandle(Path path, long firstLsn, long capacity, boolean writable)
            throws IOException {
        Preconditions.checkNotNull(path, "path");
        Preconditions.checkArgument(capacity > 0L && capacity <= Integer.MAX_VALUE,
                "mmap 段容量必须在 (0, 2GB] 内，实际 = %s", capacity);
        this.path = path;
        this.firstLsn = firstLsn;
        this.capacity = capacity;
        this.writable = writable;
        if (writable) {
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            ensureFileLength(channel, capacity);
            this.mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0L, capacity);
            this.appendPosition = 0L;
        } else {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            long size = channel.size();
            long mapLen = Math.min(size, capacity);
            if (mapLen <= 0L) {
                // 空文件无可映射区域：保持 mapped == null，read() 直接返回 0
                this.mapped = null;
                this.appendPosition = 0L;
            } else {
                this.mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0L, mapLen);
                // 有效上界不得超过映射长度，否则 read 会越过映射区（IndexOutOfBounds）
                this.appendPosition = Math.min(size, mapLen);
            }
        }
    }

    /**
     * 保证文件长度至少为 {@code target}（SPARSE 预分配：只改长度，不写满数据）。
     *
     * @param ch 文件通道
     * @param target 目标长度
     * @throws IOException IO 失败
     */
    static void ensureFileLength(FileChannel ch, long target) throws IOException {
        long size = ch.size();
        if (size >= target) {
            return;
        }
        // 在 target-1 处写一个字节即把文件扩展到 target，且不会真的占用 target 字节磁盘块
        ByteBuffer one = ByteBuffer.allocate(1);
        one.put(0, (byte) 0);
        long written = 0L;
        while (written < 1L) {
            int n = ch.write(one, target - 1L);
            if (n <= 0) {
                throw new IOException("预分配失败：无法扩展 " + ch + " 到 " + target + " 字节");
            }
            written += n;
        }
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public long firstLsn() {
        return firstLsn;
    }

    @Override
    public long capacityBytes() {
        return capacity;
    }

    @Override
    public long appendPosition() {
        return (long) APPEND_POSITION.getAcquire(this);
    }

    @Override
    public void append(ByteBuffer frame) throws IOException {
        if (closed) {
            throw new IOException("段句柄已关闭: " + path);
        }
        if (!writable) {
            throw new IOException("只读段不可追加: " + path);
        }
        int len = frame.remaining();
        if (len <= 0) {
            return;
        }
        long pos = (long) APPEND_POSITION.getAcquire(this);
        if (pos + len > capacity) {
            throw new IOException("段空间不足: path=" + path + ", pos=" + pos
                    + ", len=" + len + ", capacity=" + capacity);
        }
        MappedByteBuffer m = this.mapped;
        if (m == null) {
            throw new IOException("段映射尚未就绪（可能正在 truncate）: " + path);
        }
        try {
            MemoryUtils.copy(m, (int) pos, frame, frame.position(), len);
        } catch (RuntimeException e) {
            throw new IOException("写入 mmap 段失败: " + path + " @ " + pos, e);
        }
        // release 语义发布：并发读者 getAcquire 后能看到 [0, pos+len) 的完整字节
        APPEND_POSITION.setRelease(this, pos + len);
    }

    @Override
    public void restoreAppendPosition(long position) throws IOException {
        if (position < 0L || position > capacity) {
            throw new IOException("恢复写入位置越界: path=" + path
                    + ", position=" + position + ", capacity=" + capacity);
        }
        long cur = (long) APPEND_POSITION.getAcquire(this);
        if (position < cur) {
            throw new IOException("恢复写入位置不得后退（后退请用 truncate）: path=" + path
                    + ", position=" + position + ", current=" + cur);
        }
        if (mapped == null && position > 0L) {
            throw new IOException("段映射尚未就绪，无法恢复写入位置: " + path);
        }
        // release 发布：读者 acquire 后能看到 [0, position) 内的完整字节
        APPEND_POSITION.setRelease(this, position);
    }

    @Override
    public int read(long fileOffset, ByteBuffer dst, int len) {
        if (len <= 0) {
            dst.limit(dst.position());
            return 0;
        }
        MappedByteBuffer m = this.mapped;
        if (m == null) {
            dst.limit(dst.position());
            return 0;
        }
        long end = appendPosition();
        long available = end - fileOffset;
        if (available <= 0L) {
            dst.limit(dst.position());
            return 0;
        }
        int toRead = (int) Math.min((long) len, available);
        int dstPos = dst.position();
        if (dstPos + toRead > dst.capacity()) {
            toRead = dst.capacity() - dstPos;
            if (toRead <= 0) {
                dst.limit(dstPos);
                return 0;
            }
        }
        dst.limit(dstPos + toRead);
        MemoryUtils.copy(dst, dstPos, m, (int) fileOffset, toRead);
        return toRead;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // 纪律：走 FileChannel.force，不用 MappedByteBuffer.force()
        channel.force(metaData);
    }

    @Override
    public void truncate(long newLength) throws IOException {
        Preconditions.checkArgument(newLength >= 0L, "newLength 不得为负");
        unmapQuietly();
        channel.truncate(newLength);
        if (writable) {
            // mmap 要求"映射长度 == 文件长度"，所以截断后必须把长度补回容量再重新映射
            ensureFileLength(channel, capacity);
            mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0L, capacity);
        } else {
            long mapLen = Math.min(Math.max(newLength, 0L), capacity);
            // 注意：map(mode, 0, 0) 会抛 IllegalArgumentException，长度为 0 只能不映射
            mapped = mapLen <= 0L ? null : channel.map(FileChannel.MapMode.READ_ONLY, 0L, mapLen);
        }
        APPEND_POSITION.setRelease(this, newLength);
    }

    @Override
    public long physicalLength() throws IOException {
        return channel.size();
    }

    @Override
    public boolean isMapped() {
        return true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            unmapQuietly();
        } finally {
            channel.close();
        }
    }

    /**
     * 解除映射（失败不抛，交给调用方的 pendingDelete 机制）。
     */
    private void unmapQuietly() {
        MappedByteBuffer m = this.mapped;
        this.mapped = null;
        if (m != null) {
            MemoryUtils.unmapWithRetry(m);
        }
    }

    /**
     * 映射缓冲区（测试/诊断用，正常业务不应直接触碰）。
     *
     * @return 映射缓冲区
     */
    MappedByteBuffer mappedBuffer() {
        return mapped;
    }

    @Override
    public String toString() {
        return "MappedSegmentHandle{path=" + path
                + ", firstLsn=" + firstLsn
                + ", capacity=" + capacity
                + ", appendPosition=" + appendPosition()
                + ", writable=" + writable
                + '}';
    }
}
