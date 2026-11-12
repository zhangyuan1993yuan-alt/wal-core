package com.walcore.internal.io;

import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * FileChannel 段句柄：用<b>定位写</b>（positional write）实现追加，全平台可移植。
 *
 * <h2>为什么用定位写而不是 {@code channel.position()} + {@code write()}</h2>
 * 定位写（{@link FileChannel#write(ByteBuffer, long)} / {@link FileChannel#read(ByteBuffer, long)}）
 * <b>不改变也不依赖</b>通道的共享 position 指针，因此在多线程读同一句柄时天然安全
 * （游标线程读 + 组提交线程 force 是常态）。而 {@code position()+write()} 需要用锁保护
 * 共享 position，在 append 路径上加锁是明令禁止的。
 *
 * <h2>为什么它是 Windows 的默认后端</h2>
 * 唯一的额外成本是每条记录一次 {@code write} 系统调用（约 1~2µs）。对"Windows 不承诺性能、
 * 只承诺正确性"的定位来说，这是最省心的选择：没有 mmap 生命周期管理、没有
 * "映射中无法删除文件"的坑、{@code force(false)} 直接就是 {@code fdatasync}。
 */
public final class FileChannelSegmentHandle implements SegmentHandle {

    private static final VarHandle APPEND_POSITION;

    static {
        try {
            APPEND_POSITION = MethodHandles.lookup().findVarHandle(
                    FileChannelSegmentHandle.class, "appendPosition", long.class);
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

    private volatile boolean closed;

    /**
     * 打开或创建一个 FileChannel 段句柄。
     *
     * @param path 段路径
     * @param firstLsn 段内首条记录 LSN
     * @param capacity 段容量
     * @param writable 是否可写
     * @throws IOException 打开失败
     */
    public FileChannelSegmentHandle(Path path, long firstLsn, long capacity, boolean writable)
            throws IOException {
        Preconditions.checkNotNull(path, "path");
        Preconditions.checkArgument(capacity > 0L, "capacity 必须为正数");
        this.path = path;
        this.firstLsn = firstLsn;
        this.capacity = capacity;
        this.writable = writable;
        if (writable) {
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            this.appendPosition = 0L;
        } else {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            // 只读打开时，可读上界 == 文件长度
            this.appendPosition = channel.size();
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
        // 直接用调用方的缓冲区做定位写：FileChannel.write(ByteBuffer,long) 会推进
        // src 的 position，因此写完必须还原。这样 append 路径上零对象分配
        // （早先用 frame.duplicate()，每条记录多分配一个对象）。
        int savedPos = frame.position();
        long cursor = pos;
        long deadline = pos + len;
        try {
            while (cursor < deadline) {
                int written = channel.write(frame, cursor);
                if (written <= 0) {
                    throw new IOException("定位写未推进: path=" + path + ", offset=" + cursor);
                }
                cursor += written;
            }
        } catch (IOException e) {
            throw new IOException("写入段失败: " + path + " @ " + pos, e);
        } finally {
            frame.position(savedPos);
        }
        APPEND_POSITION.setRelease(this, deadline);
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
        APPEND_POSITION.setRelease(this, position);
    }

    @Override
    public int read(long fileOffset, ByteBuffer dst, int len) throws IOException {
        if (len <= 0) {
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
        ByteBuffer view = dst.duplicate();
        view.limit(dstPos + toRead);
        view.position(dstPos);
        int copied = 0;
        while (copied < toRead) {
            int n = channel.read(view, fileOffset + copied);
            if (n < 0) {
                break;
            }
            copied += n;
        }
        dst.limit(dstPos + copied);
        return copied;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public void truncate(long newLength) throws IOException {
        Preconditions.checkArgument(newLength >= 0L, "newLength 不得为负");
        channel.truncate(newLength);
        APPEND_POSITION.setRelease(this, newLength);
    }

    @Override
    public long physicalLength() throws IOException {
        return channel.size();
    }

    @Override
    public boolean isMapped() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        channel.close();
    }

    @Override
    public String toString() {
        return "FileChannelSegmentHandle{path=" + path
                + ", firstLsn=" + firstLsn
                + ", capacity=" + capacity
                + ", appendPosition=" + appendPosition()
                + ", writable=" + writable
                + '}';
    }
}
