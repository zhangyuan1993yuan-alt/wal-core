package com.walcore.internal.io;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * 堆外内存与缓冲区读写辅助。
 *
 * <h2>关于 ARCH 里的 {@code putInt(long addr, int v)} 签名（重要偏离，已在你看到的文档里标注）</h2>
 * ARCH §12 类图给出的是"裸地址"版本（{@code putLong(long addr, long v)} 等）。在 JDK 17 上
 * <b>无法在不用 {@code --add-opens java.base/java.nio=ALL-UNNAMED} 的前提下</b>从一个
 * {@link ByteBuffer} 取到它的裸地址（{@code java.nio.Buffer#address} 不对外开放、
 * {@code sun.nio.ch.DirectBuffer} 未导出）。而 TASKS-wal.md T02 ⑦ 又明确禁止直接调用
 * {@code sun.misc.Unsafe.putLong}。两条约束交集的结果只有一个：<b>读写走
 * {@link ByteBuffer} 的绝对索引 API</b>。
 *
 * <p>这不是性能退步：{@code ByteBuffer.putLong(int, long)} / {@code getLong(int)} 在 Direct /
 * Mapped 缓冲区上会被 HotSpot 内联成单条 8 字节访存指令，与 {@code Unsafe.putLong}
 * 生成的机器码相同（都是 intrinsics）。真正的差别只在"能否拿到地址做指针运算"，而本项目
 * 并不需要指针运算——帧的绝对偏移永远是相对段起点的整数。
 *
 * <p>{@link #unmap(ByteBuffer)} 是唯一允许、也确实是唯一可行地使用 {@code sun.misc.Unsafe}
 * 的地方（{@code invokeCleaner}），且通过反射调用，编译期不产生 internal API 警告。
 * 没有它，Windows 上删除已 close 的 {@code *.wal} 会抛 {@code AccessDeniedException}。
 */
public final class MemoryUtils {

    /** 反射取得的 {@code sun.misc.Unsafe} 单例，取不到则为 null。 */
    private static final Object UNSAFE;
    /** {@code Unsafe.invokeCleaner(ByteBuffer)} 方法，取不到则为 null。 */
    private static final Method INVOKE_CLEANER;

    static {
        Object unsafe = null;
        Method invokeCleaner = null;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            java.lang.reflect.Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = theUnsafe.get(null);
            invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (Throwable ignored) {
            // 受限运行环境（某些安全沙箱 / 未来的 JDK）退化为 System.gc() 方案
            unsafe = null;
            invokeCleaner = null;
        }
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    private MemoryUtils() {
    }

    /**
     * 是否具备强制 unmap 的能力（{@code Unsafe.invokeCleaner} 可用）。
     *
     * @return 可用返回 true
     */
    public static boolean supportsExplicitUnmap() {
        return UNSAFE != null && INVOKE_CLEANER != null;
    }

    // ------------------------------------------------------------------
    // 基本读写（全部走 ByteBuffer 绝对索引 API）
    // ------------------------------------------------------------------

    /**
     * 在绝对索引处写入 int（小端）。
     *
     * @param buf 目标缓冲区
     * @param index 绝对索引
     * @param value 值
     */
    public static void putInt(ByteBuffer buf, int index, int value) {
        buf.putInt(index, value);
    }

    /**
     * 在绝对索引处写入 long（小端）。
     *
     * @param buf 目标缓冲区
     * @param index 绝对索引
     * @param value 值
     */
    public static void putLong(ByteBuffer buf, int index, long value) {
        buf.putLong(index, value);
    }

    /**
     * 在绝对索引处写入 byte。
     *
     * @param buf 目标缓冲区
     * @param index 绝对索引
     * @param value 值
     */
    public static void putByte(ByteBuffer buf, int index, byte value) {
        buf.put(index, value);
    }

    /**
     * 从绝对索引处读取 int（小端）。
     *
     * @param buf 源缓冲区
     * @param index 绝对索引
     * @return 值
     */
    public static int getInt(ByteBuffer buf, int index) {
        return buf.getInt(index);
    }

    /**
     * 从绝对索引处读取 long（小端）。
     *
     * @param buf 源缓冲区
     * @param index 绝对索引
     * @return 值
     */
    public static long getLong(ByteBuffer buf, int index) {
        return buf.getLong(index);
    }

    /**
     * 从绝对索引处读取无符号字节。
     *
     * @param buf 源缓冲区
     * @param index 绝对索引
     * @return 0..255
     */
    public static int getUnsignedByte(ByteBuffer buf, int index) {
        return buf.get(index) & 0xFF;
    }

    /**
     * 从绝对索引处读取字节。
     *
     * @param buf 源缓冲区
     * @param index 绝对索引
     * @return 字节
     */
    public static byte getByte(ByteBuffer buf, int index) {
        return buf.get(index);
    }

    // ------------------------------------------------------------------
    // 批量拷贝
    // ------------------------------------------------------------------

    /**
     * 从源缓冲区的绝对索引处拷贝 {@code length} 字节到目标缓冲区的绝对索引处。
     *
     * <p>不修改两个缓冲区的 position/limit，因此可安全用于共享缓冲区（例如活跃段的
     * mmap 缓冲区被多个写入线程共享，各自持不同偏移）。
     *
     * <h2>为什么不用 {@code slice()} + {@code put()}</h2>
     * 那种写法每次调用要分配 1~2 个视图对象，在 append 热路径上（百万次/秒）就是每秒
     * 几 MB 的垃圾。也不能用"保存 position/limit → 相对 put → 恢复"的写法：虽然零分配，
     * 但它会在拷贝期间<b>临时篡改 src 的 position/limit</b>，另一个线程此时读
     * {@code src.remaining()} 会看到 0 —— 实测并发追加同一个 payload 时会直接抛
     * {@code codec 实际帧长 -1 与预估 64 不一致}（见 T19 基准里发现的问题）。
     * {@code src} 是<b>调用方的</b>缓冲区，我们只有读的权限，连"短暂"都不该改。
     *
     * <p>改用 Java 16+ 的绝对批量 {@code put(int, ByteBuffer, int, int)}：两侧都是绝对索引，
     * 一次内存搬运（Direct→Direct 走 {@code Unsafe.copyMemory}），零分配、零状态副作用。
     * 项目按 Java 17 编译，这个重载可用。
     *
     * @param dst 目标缓冲区
     * @param dstIndex 目标起始绝对索引
     * @param src 源缓冲区
     * @param srcIndex 源起始绝对索引
     * @param length 字节数
     */
    public static void copy(ByteBuffer dst, int dstIndex, ByteBuffer src, int srcIndex, int length) {
        if (length <= 0) {
            return;
        }
        dst.put(dstIndex, src, srcIndex, length);
    }

    /**
     * 用指定值填充目标缓冲区的绝对区间。
     *
     * @param dst 目标缓冲区
     * @param fromIndex 起始绝对索引（含）
     * @param toIndex 结束绝对索引（不含）
     * @param value 填充值
     */
    public static void fill(ByteBuffer dst, int fromIndex, int toIndex, byte value) {
        for (int i = fromIndex; i < toIndex; i++) {
            dst.put(i, value);
        }
    }

    /**
     * 返回 {@code [from, to)} 的只读切片视图（共享底层存储，零拷贝）。
     *
     * @param buf 源缓冲区
     * @param from 起始绝对索引（含）
     * @param to 结束绝对索引（不含）
     * @return 只读切片
     */
    public static ByteBuffer readOnlySlice(ByteBuffer buf, int from, int to) {
        ByteBuffer dup = buf.duplicate();
        dup.limit(to);
        dup.position(from);
        return dup.slice().asReadOnlyBuffer();
    }

    // ------------------------------------------------------------------
    // unmap
    // ------------------------------------------------------------------

    /**
     * 强制解除一个 Direct / Mapped 缓冲区的内存映射并释放其引用。
     *
     * <p>这一步在 Linux 上"只是及时释放虚拟地址空间"，在 Windows 上则是
     * <b>能否删除文件的前提条件</b>：文件被映射时 {@code Files.delete} 会抛
     * {@code AccessDeniedException} 或 {@code FileSystemException}。
     *
     * @param buffer 待 unmap 的缓冲区，可为 null
     * @return 成功解除（或本来就无需解除）返回 true
     */
    public static boolean unmap(ByteBuffer buffer) {
        if (buffer == null) {
            return true;
        }
        if (!buffer.isDirect()) {
            return false;
        }
        if (UNSAFE != null && INVOKE_CLEANER != null) {
            try {
                INVOKE_CLEANER.invoke(UNSAFE, buffer);
                return true;
            } catch (Throwable ignored) {
                // 落到下面的 GC 兜底
            }
        }
        // 兜底：反射 cleaner()（需要 --add-opens，多数环境不可用，但试一下不亏）
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                Method clean = cleaner.getClass().getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner);
                return true;
            }
        } catch (Throwable ignored) {
            // 继续兜底
        }
        return false;
    }

    /**
     * 带重试的 unmap：先直接 unmap，失败则 {@code System.gc()} 后再试，最多 3 轮。
     *
     * <p>{@code System.gc()} 可能被 {@code -XX:+DisableExplicitGC} 关闭，因此本方法不保证
     * 一定成功；调用方（{@code MappedSegmentHandle.close()}）在失败时应把路径登记到
     * {@code pendingDelete}，下次 {@code open()} 时清理（ARCH §16.4）。
     *
     * @param buffer 待 unmap 的缓冲区
     * @return 成功返回 true
     */
    public static boolean unmapWithRetry(ByteBuffer buffer) {
        if (unmap(buffer)) {
            return true;
        }
        for (int attempt = 0; attempt < 3; attempt++) {
            System.gc();
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (unmap(buffer)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 创建一个 Direct 缓冲区（{@link ByteBuffer#allocateDirect(int)} 的语义化包装，
     * 便于未来统一接入堆外预算统计）。
     *
     * @param capacity 容量
     * @return Direct 缓冲区
     */
    public static ByteBuffer allocateDirect(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }
}
