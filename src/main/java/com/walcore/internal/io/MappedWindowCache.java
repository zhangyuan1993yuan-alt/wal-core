package com.walcore.internal.io;

import com.walcore.internal.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 只读段的 mmap <b>窗口缓存</b>（LRU）。
 *
 * <h2>为什么只读段要窗口化</h2>
 * 单机 10TB 日志意味着可能有数万个只读段。若每个都整段 mmap，虚拟地址空间与内核页表
 * 会被撑爆。因此只读段按 {@code mappedWindowBytes}（默认 32MB）窗口映射，
 * 同时最多保留 {@code mappedWindowCount}（默认 8）个窗口 —— 顺带把"同时打开的文件句柄数"
 * 也硬约束在 8 个以内（ARCH §14 失败模式 #3 的 EMFILE 防线）。
 *
 * <h2>句柄数上界的实现要点</h2>
 * 每个 {@link Window} 持有一个 {@link FileChannel}，而窗口总数由构造参数 {@code windowCount}
 * 限制，因此 {@link #openFileCount()} <b>恒 ≤ windowCount</b>，可被测试直接断言。
 * 逐出时先 {@link MemoryUtils#unmapWithRetry} 再 {@code channel.close()} —— 顺序不能反，
 * 否则 Windows 上会留下删不掉的映射。
 *
 * <h2>与 ARCH 类图签名的差异</h2>
 * 类图写的是 {@code acquire(int pid, long fileOffset, int len)}，但这无法定位文件，
 * 因此实际签名增加了 {@code Path} 参数。这是必要偏离，已记录。
 */
public final class MappedWindowCache implements AutoCloseable {

    private final int windowCount;
    private final long windowBytes;

    /** accessOrder=true 的 LRU。全部访问都在 synchronized 下，无需并发容器。 */
    private final LinkedHashMap<WindowKey, Window> windows = new LinkedHashMap<>(16, 0.75f, true);

    private long evictedCount;
    private boolean closed;

    /**
     * 构造窗口缓存。
     *
     * @param windowCount 窗口数量上限（同时打开的文件句柄上限）
     * @param windowBytes 单窗口字节数
     */
    public MappedWindowCache(int windowCount, long windowBytes) {
        this.windowCount = Preconditions.checkInRange(windowCount, 1, 4096, "windowCount");
        this.windowBytes = Preconditions.checkPositive(windowBytes, "windowBytes");
    }

    /**
     * 取得覆盖 {@code [fileOffset, fileOffset+len)} 的只读视图。
     *
     * <p>窗口内命中 = mmap 切片（零拷贝）；跨窗口边界 = 堆拷贝（F3，防映射泄漏）。
     *
     * @param path 段文件路径
     * @param fileOffset 起始文件偏移
     * @param len 需要的字节数，必须 ≤ {@link #windowBytes()}
     * @return 只读视图（position=0，limit=len）
     * @throws IOException 读取/映射失败
     */
    public synchronized ByteBuffer acquire(Path path, long fileOffset, int len) throws IOException {
        Preconditions.checkNotNull(path, "path");
        Preconditions.checkArgument(fileOffset >= 0L, "fileOffset 不得为负");
        Preconditions.checkArgument(len >= 0, "len 不得为负");
        if (closed) {
            throw new IOException("MappedWindowCache 已关闭");
        }
        if (len == 0) {
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
        }
        Preconditions.checkArgument((long) len <= windowBytes,
                "单次请求长度 %s 超过窗口大小 %s，请调大 mappedWindowBytes", len, windowBytes);

        long base = (fileOffset / windowBytes) * windowBytes;
        WindowKey key = new WindowKey(path, base);
        Window window = windows.get(key);
        if (window == null) {
            window = load(path, base);
            windows.put(key, window);
            evictIfNeeded();
        }
        int offsetInWindow = (int) (fileOffset - window.base());
        if (offsetInWindow + len > window.length()) {
            // 请求跨窗口边界：一次性堆拷贝读取（不缓存、不 mmap，见 mapExact 的 F3 说明）
            return mapExact(path, fileOffset, len);
        }
        return window.slice(offsetInWindow, len);
    }

    /**
     * 逐出某个文件的全部窗口（段被删除/冻结时调用）。
     *
     * @param path 段文件路径
     */
    public synchronized void evict(Path path) {
        Iterator<Map.Entry<WindowKey, Window>> it = windows.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<WindowKey, Window> e = it.next();
            if (e.getKey().path().equals(path)) {
                e.getValue().release();
                it.remove();
                evictedCount++;
            }
        }
    }

    /**
     * 当前打开的窗口数（== 打开的文件句柄数）。
     *
     * @return 数量，恒 ≤ {@link #windowCount()}
     */
    public synchronized int openFileCount() {
        return windows.size();
    }

    /**
     * 窗口数量上限。
     *
     * @return 上限
     */
    public int windowCount() {
        return windowCount;
    }

    /**
     * 单窗口字节数。
     *
     * @return 字节数
     */
    public long windowBytes() {
        return windowBytes;
    }

    /**
     * 累计逐出次数（测试断言 LRU 生效）。
     *
     * @return 次数
     */
    public synchronized long evictedCount() {
        return evictedCount;
    }

    /**
     * 当前映射占用的堆外字节数。
     *
     * @return 字节数
     */
    public synchronized long mappedBytes() {
        long total = 0L;
        for (Window w : windows.values()) {
            total += w.length();
        }
        return total;
    }

    /**
     * 关闭全部窗口并解除映射。
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Window w : windows.values()) {
            w.release();
        }
        windows.clear();
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 映射一个窗口。
     *
     * @param path 文件
     * @param base 窗口起始偏移（按 windowBytes 对齐）
     * @return 窗口
     * @throws IOException 映射失败
     */
    private Window load(Path path, long base) throws IOException {
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = ch.size();
            long len = Math.min(windowBytes, size - base);
            if (len <= 0L) {
                ch.close();
                return new Window(path, base, 0L, null, null);
            }
            MappedByteBuffer mapped = ch.map(FileChannel.MapMode.READ_ONLY, base, len);
            return new Window(path, base, len, ch, mapped);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /**
     * 精确读取一段（跨窗口边界时的一次性路径，不进入 LRU）。
     *
     * <p><b>F3：堆拷贝，不再 mmap。</b>旧实现按精确范围 mmap 后关闭通道、返回映射切片：
     * 映射本身无人释放（{@code MappedByteBuffer} 没有 unmap API），切片被持有期间
     * 映射常驻、切片被丢弃后要等 GC 触发 {@code Cleaner} 才回收。游标每次跨窗口
     * 边界读都会<b>泄漏一次映射</b>——长时间运行的 tailer 逐条跨边界推进时，泄漏
     * 速率约等于读取速率，虚拟地址空间（64 位 Linux ~128TB 也好、压缩Oops 的
     * 32GB 也好）与内核页表终会被耗尽。
     *
     * <p>改为定位读进堆缓冲：分配一次（长度即请求长度，不放大）、一次
     * {@code pread}、返回只读视图。代价是丢掉这条<b>罕见路径</b>的零拷贝（窗口内的
     * 读取仍是零拷贝 mmap 切片）；换来的是不依赖 GC 释放任何映射。
     *
     * @param path 文件
     * @param fileOffset 起始偏移
     * @param len 长度
     * @return 只读堆缓冲（position=0，limit=len）
     * @throws IOException 读取失败
     */
    private ByteBuffer mapExact(Path path, long fileOffset, int len) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = ch.size();
            if (fileOffset + len > size) {
                throw new IOException("越界读取: path=" + path + ", offset=" + fileOffset
                        + ", len=" + len + ", size=" + size);
            }
            ByteBuffer heap = ByteBuffer.allocate(len);
            int read = 0;
            while (read < len) {
                int n = ch.read(heap, fileOffset + read);
                if (n <= 0) {
                    throw new IOException("短读: path=" + path + ", offset=" + fileOffset
                            + ", len=" + len + ", read=" + read);
                }
                read += n;
            }
            heap.flip();
            return heap.asReadOnlyBuffer();
        }
    }

    /**
     * 超过上限时逐出最久未使用的窗口。
     */
    private void evictIfNeeded() {
        while (windows.size() > windowCount) {
            Iterator<Map.Entry<WindowKey, Window>> it = windows.entrySet().iterator();
            if (!it.hasNext()) {
                return;
            }
            Map.Entry<WindowKey, Window> eldest = it.next();
            eldest.getValue().release();
            it.remove();
            evictedCount++;
        }
    }

    /**
     * 窗口键：文件 + 对齐后的起始偏移。
     *
     * @param path 文件
     * @param base 起始偏移
     */
    private record WindowKey(Path path, long base) {
    }

    /**
     * 一个已映射的窗口。
     */
    private static final class Window {
        private final Path path;
        private final long base;
        private final long length;
        private FileChannel channel;
        private MappedByteBuffer mapped;

        Window(Path path, long base, long length, FileChannel channel, MappedByteBuffer mapped) {
            this.path = path;
            this.base = base;
            this.length = length;
            this.channel = channel;
            this.mapped = mapped;
        }

        long base() {
            return base;
        }

        long length() {
            return length;
        }

        ByteBuffer slice(int offsetInWindow, int len) {
            if (mapped == null) {
                return ByteBuffer.allocate(0).asReadOnlyBuffer();
            }
            ByteBuffer dup = mapped.duplicate();
            dup.limit(offsetInWindow + len);
            dup.position(offsetInWindow);
            return dup.slice().asReadOnlyBuffer();
        }

        void release() {
            MappedByteBuffer m = this.mapped;
            this.mapped = null;
            if (m != null) {
                MemoryUtils.unmapWithRetry(m);
            }
            FileChannel ch = this.channel;
            this.channel = null;
            if (ch != null) {
                try {
                    ch.close();
                } catch (IOException ignored) {
                    // 关闭失败的通道会在下次 GC 时被回收，这里不向上抛
                }
            }
        }

        @Override
        public String toString() {
            return "Window{" + path.getFileName() + " base=" + base + ", len=" + length + '}';
        }
    }

    @Override
    public String toString() {
        return "MappedWindowCache{windowCount=" + windowCount
                + ", windowBytes=" + windowBytes
                + ", open=" + openFileCount()
                + '}';
    }
}
