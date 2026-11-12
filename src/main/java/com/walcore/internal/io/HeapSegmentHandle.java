package com.walcore.internal.io;

import com.walcore.internal.util.Preconditions;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 纯堆内段句柄：分块 {@code byte[][]} 存储，<b>零文件系统副作用</b>。
 *
 * <h2>为什么需要它</h2>
 * <ul>
 *   <li>单元测试：不需要 {@code @TempDir}，不产生任何磁盘 IO，跑得快且不受 CI 磁盘配额影响；</li>
 *   <li>嵌入式小规模场景：用户不想落盘，只要"进程内 WAL"；</li>
 *   <li>三后端契约测试的"对照组"：如果某个行为在 Heap 上对、在 mmap 上错，
 *       立刻能定位到是 IO 实现问题而不是上层逻辑问题。</li>
 * </ul>
 *
 * <p>分块而不是单个 {@code byte[]}：一个 256MB 的段直接 {@code new byte[256MB]} 会让
 * 测试瞬间吃掉大堆内存并触发 Full GC；分块（默认 1MB/块）按需分配，用多少占多少。
 *
 * <p>{@link #force(boolean)} 是 no-op，但会累加计数（{@link #forceCount()}），
 * 这让组提交的测试可以精确断言"BATCH 模式下写入线程从不调用 force"。
 */
public final class HeapSegmentHandle implements SegmentHandle {

    private static final VarHandle APPEND_POSITION;

    static {
        try {
            APPEND_POSITION = MethodHandles.lookup().findVarHandle(
                    HeapSegmentHandle.class, "appendPosition", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** 每块大小（1MB）。 */
    static final int CHUNK_SIZE = 1024 * 1024;

    private final Path path;
    private final long firstLsn;
    private final long capacity;
    private final boolean writable;
    private final HeapStore store;
    private final AtomicLong forceCount = new AtomicLong();
    private final AtomicLong truncatedCount = new AtomicLong();

    private long appendPosition;
    private volatile boolean closed;

    /**
     * 构造堆内段句柄。
     *
     * @param path 逻辑路径（不会真的创建文件）
     * @param firstLsn 段内首条记录 LSN
     * @param capacity 段容量
     * @param writable 是否可写
     * @param store 共享的底层存储
     */
    public HeapSegmentHandle(Path path, long firstLsn, long capacity, boolean writable,
                             HeapStore store) {
        this.path = Preconditions.checkNotNull(path, "path");
        this.firstLsn = firstLsn;
        this.capacity = Preconditions.checkPositive(capacity, "capacity");
        this.writable = writable;
        this.store = Preconditions.checkNotNull(store, "store");
        this.appendPosition = writable ? 0L : store.length();
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
        store.write(pos, frame, frame.position(), len);
        APPEND_POSITION.setRelease(this, pos + len);
    }

    @Override
    public int read(long fileOffset, ByteBuffer dst, int len) {
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
        store.read(fileOffset, dst, dstPos, toRead);
        dst.limit(dstPos + toRead);
        return toRead;
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
    public void force(boolean metaData) {
        forceCount.incrementAndGet();
    }

    @Override
    public void truncate(long newLength) {
        Preconditions.checkArgument(newLength >= 0L, "newLength 不得为负");
        store.truncate(newLength);
        APPEND_POSITION.setRelease(this, newLength);
        truncatedCount.incrementAndGet();
    }

    @Override
    public long physicalLength() {
        return store.length();
    }

    @Override
    public boolean isMapped() {
        return false;
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * {@code force()} 被调用次数（测试断言用）。
     *
     * @return 次数
     */
    public long forceCount() {
        return forceCount.get();
    }

    /**
     * {@code truncate()} 被调用次数（测试断言用）。
     *
     * @return 次数
     */
    public long truncateCount() {
        return truncatedCount.get();
    }

    @Override
    public String toString() {
        return "HeapSegmentHandle{path=" + path
                + ", firstLsn=" + firstLsn
                + ", capacity=" + capacity
                + ", appendPosition=" + appendPosition()
                + ", forceCount=" + forceCount.get()
                + '}';
    }

    // ------------------------------------------------------------------
    // 底层存储
    // ------------------------------------------------------------------

    /**
     * 分块堆内存储。多个句柄可共享同一 store（模拟"关闭后重新 open 同一段"）。
     */
    public static final class HeapStore {

        private final List<byte[]> chunks = new ArrayList<>();
        private long length;

        /**
         * 跨 chunk 搬运用的暂存数组，复用而非每次分配。
         *
         * <p>heap 后端主要服务于单元测试与"完全内存运行"的部署；即便如此，append
         * 热路径也要求零分配（P0-01 ③），因此这里按 {@code CHUNK_SIZE} 惰性分配一次。
         */
        private byte[] scratch;

        /**
         * 当前逻辑长度。
         *
         * @return 字节数
         */
        public long length() {
            return length;
        }

        /**
         * 写入一段字节。
         *
         * @param pos 起始偏移
         * @param src 源缓冲区
         * @param srcOffset 源起始绝对索引
         * @param len 字节数
         */
        synchronized void write(long pos, ByteBuffer src, int srcOffset, int len) {
            int done = 0;
            byte[] tmp = scratch;
            if (tmp == null) {
                tmp = new byte[CHUNK_SIZE];
                scratch = tmp;
            }
            while (done < len) {
                long p = pos + done;
                int chunkIndex = (int) (p / CHUNK_SIZE);
                int inChunk = (int) (p % CHUNK_SIZE);
                int n = Math.min(len - done, CHUNK_SIZE - inChunk);
                // 绝对批量读：不改变 src 的 position/limit，也不分配视图对象
                src.get(srcOffset + done, tmp, 0, n);
                System.arraycopy(tmp, 0, chunk(chunkIndex), inChunk, n);
                done += n;
            }
            if (pos + len > length) {
                length = pos + len;
            }
        }

        /**
         * 读取一段字节。
         *
         * @param pos 起始偏移
         * @param dst 目标缓冲区
         * @param dstPos 目标起始绝对索引
         * @param len 字节数
         */
        synchronized void read(long pos, ByteBuffer dst, int dstPos, int len) {
            int done = 0;
            byte[] tmp = new byte[Math.min(len, CHUNK_SIZE)];
            while (done < len) {
                long p = pos + done;
                int chunkIndex = (int) (p / CHUNK_SIZE);
                int inChunk = (int) (p % CHUNK_SIZE);
                int n = Math.min(len - done, CHUNK_SIZE - inChunk);
                byte[] c = chunkIndex < chunks.size() ? chunks.get(chunkIndex) : null;
                if (c == null) {
                    java.util.Arrays.fill(tmp, 0, n, (byte) 0);
                } else {
                    System.arraycopy(c, inChunk, tmp, 0, n);
                }
                ByteBuffer view = dst.duplicate();
                view.limit(dstPos + done + n);
                view.position(dstPos + done);
                view.put(tmp, 0, n);
                done += n;
            }
        }

        /**
         * 截断并把尾部清零。
         *
         * @param newLength 新长度
         */
        synchronized void truncate(long newLength) {
            if (newLength >= length) {
                length = newLength;
                return;
            }
            int fromChunk = (int) (newLength / CHUNK_SIZE);
            int inChunk = (int) (newLength % CHUNK_SIZE);
            for (int i = fromChunk; i < chunks.size(); i++) {
                byte[] c = chunks.get(i);
                if (c == null) {
                    continue;
                }
                int start = (i == fromChunk) ? inChunk : 0;
                java.util.Arrays.fill(c, start, c.length, (byte) 0);
            }
            length = newLength;
        }

        private byte[] chunk(int index) {
            while (chunks.size() <= index) {
                chunks.add(null);
            }
            byte[] c = chunks.get(index);
            if (c == null) {
                c = new byte[CHUNK_SIZE];
                chunks.set(index, c);
            }
            return c;
        }
    }
}
