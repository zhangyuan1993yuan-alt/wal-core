package com.walcore.internal.util;

import com.walcore.internal.append.FrameLayout;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * CRC32C（Castagnoli）计算工具 —— <b>全项目唯一入口</b>。
 *
 * <h2>覆盖区间（写错一次，恢复测试会以极难定位的方式失败）</h2>
 * <pre>
 *   [0, 20) ∪ [24, frameLen)
 * </pre>
 * 也就是"帧头前 20 字节 + 跳过 crc 字段自身的 4 字节 + crc 字段之后直到帧尾的全部内容"。
 * 之所以跳过 crc 字段：算 CRC 时该字段还是 0 占位，写完再回填；若把它也算进去会形成自引用。
 * 之所以覆盖 {@code frameLen} 与 {@code headerLen}：这两个字段决定了帧的边界，它们被改写
 * 就会导致后续所有帧错位，属于必须检出的错误。
 *
 * <p><b>禁止</b>在任何别处自己拼 {@link CRC32C}：本类为每个线程缓存一个 {@link CRC32C} 实例
 * （{@link CRC32C#reset()} 复用，避免每条记录一次 {@code new}），并集中处理
 * {@code ByteBuffer} 的 position/limit 语义。
 *
 * <h2>为什么选 CRC32C 而不是 CRC32</h2>
 * {@code java.util.zip.CRC32C}（JDK 9+）在 x86 上走 SSE4.2 硬件指令（{@code crc32q}），
 * 吞吐约 1.5GB/s；{@code CRC32}（zlib 多项式）没有对应硬件指令，慢约 3 倍。
 * 对 320B 的典型帧，CRC32C 耗时约 0.35µs，正好落在 PRD WAL-P0-03 ② "p99 ≤ 1µs" 的预算内。
 */
public final class Crc32cUtil {

    /** 每线程一个 CRC32C 实例，避免每条记录分配。 */
    private static final ThreadLocal<CRC32C> PER_THREAD = ThreadLocal.withInitial(CRC32C::new);

    /** 每线程的 {@code duplicate()} 视图缓存（见 {@link #viewOf}）。 */
    private static final ThreadLocal<ViewCache> VIEW_CACHE = ThreadLocal.withInitial(ViewCache::new);

    private Crc32cUtil() {
    }

    /**
     * 计算整帧的 CRC32C，覆盖 {@code [0,20) ∪ [24, frameLen)}。
     *
     * @param frame 帧所在缓冲区，帧起始于绝对索引 0
     * @param frameLen 整帧长度
     * @return CRC32C 值（按 int 看待）
     * @throws IllegalArgumentException 缓冲区容量不足
     */
    public static int computeFrame(ByteBuffer frame, int frameLen) {
        return computeFrame(frame, 0, frameLen);
    }

    /**
     * 计算整帧的 CRC32C，覆盖 {@code [frameOffset, frameOffset+20) ∪ [frameOffset+24, frameOffset+frameLen)}。
     *
     * @param src 帧所在缓冲区（不会被修改 position/limit）
     * @param frameOffset 帧起始绝对偏移
     * @param frameLen 整帧长度
     * @return CRC32C 值
     * @throws IllegalArgumentException 参数越界
     */
    public static int computeFrame(ByteBuffer src, int frameOffset, int frameLen) {
        // 热路径（每帧一次）：刻意用 if + throw 代替 Preconditions 的可变参数重载。
        // 可变参数形式即使被 JIT 逃逸分析消掉，也依赖内联深度；这里显式写死，
        // 保证"零分配"不依赖 JIT 决策（实测可变参数形式在这里会漏 16 字节/帧）。
        if (frameOffset < 0) {
            throw new IllegalArgumentException("frameOffset 不得为负: " + frameOffset);
        }
        if (frameLen < FrameLayout.MIN_FRAME_LEN) {
            throw new IllegalArgumentException("frameLen 过小: " + frameLen);
        }
        int end = frameOffset + frameLen;
        if (end > src.limit() || end > src.capacity()) {
            throw new IllegalArgumentException("缓冲区不足以容纳整帧: need=" + end
                    + ", limit=" + src.limit() + ", capacity=" + src.capacity());
        }
        CRC32C crc = PER_THREAD.get();
        crc.reset();
        ByteBuffer view = viewOf(src);
        // 第一段：[frameOffset, frameOffset + 20)
        view.limit(frameOffset + FrameLayout.CRC_SKIP_START);
        view.position(frameOffset);
        crc.update(view);
        // 第二段：[frameOffset + 24, frameOffset + frameLen)
        view.limit(end);
        view.position(frameOffset + FrameLayout.CRC_SKIP_END);
        crc.update(view);
        return (int) crc.getValue();
    }

    /**
     * 取一个"与 {@code src} 共享内容"的视图，并<b>复用</b>它以避免每条记录分配。
     *
     * <p>为什么需要这个缓存：{@code src.duplicate()} 每次都会 new 一个 {@code ByteBuffer}
     * （约 20~32 字节）。append 热路径上每秒百万次调用就是每秒几十 MB 垃圾，直接违反
     * P0-01 ③ 的"零对象分配"（实测 10 万次 append 产生约 2MB 分配，全部来自这里）。
     *
     * <p>缓存策略：按线程记住"上一次传入的 Buffer 对象 + 它的 duplicate"。写入热点上
     * 传入的永远是同一个 staging 区实例，因此命中率 100%，稳态零分配；一旦换成别的
     * Buffer（例如解码路径）只多一次 duplicate，行为与不缓存完全等价。
     */
    private static ByteBuffer viewOf(ByteBuffer src) {
        ViewCache cache = VIEW_CACHE.get();
        if (cache.source == src && cache.view != null) {
            return cache.view;
        }
        ByteBuffer view = src.duplicate();
        cache.source = src;
        cache.view = view;
        return view;
    }

    /**
     * 每个线程一份的视图缓存（只缓存最近一个源 Buffer）。
     */
    private static final class ViewCache {
        private ByteBuffer source;
        private ByteBuffer view;
    }

    /**
     * 计算一段<b>连续</b>字节的 CRC32C（用于 {@code .idx} 文件头、{@code wal.meta} 正文等
     * 不存在"跳过字段"结构的场景）。
     *
     * @param src 缓冲区
     * @param offset 起始绝对偏移
     * @param length 字节数
     * @return CRC32C 值
     */
    public static int computeRange(ByteBuffer src, int offset, int length) {
        Preconditions.checkNotNull(src, "src");
        Preconditions.checkArgument(offset >= 0 && length >= 0, "offset/length 不得为负");
        Preconditions.checkArgument(offset + length <= src.capacity(), "缓冲区容量不足");
        CRC32C crc = PER_THREAD.get();
        crc.reset();
        if (length == 0) {
            return (int) crc.getValue();
        }
        ByteBuffer view = src.duplicate();
        view.limit(offset + length);
        view.position(offset);
        crc.update(view);
        return (int) crc.getValue();
    }

    /**
     * 计算字节数组的 CRC32C。
     *
     * @param data 字节数组
     * @param offset 起始下标
     * @param length 字节数
     * @return CRC32C 值
     */
    public static int computeRange(byte[] data, int offset, int length) {
        Preconditions.checkNotNull(data, "data");
        Preconditions.checkArgument(offset >= 0 && length >= 0, "offset/length 不得为负");
        Preconditions.checkArgument(offset + length <= data.length, "数组越界");
        CRC32C crc = PER_THREAD.get();
        crc.reset();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    /**
     * 计算整个字节数组的 CRC32C。
     *
     * @param data 字节数组
     * @return CRC32C 值
     */
    public static int computeRange(byte[] data) {
        return computeRange(data, 0, data.length);
    }

    /**
     * 增量计算的起点：重置并返回一个尚未累积任何字节的 CRC32C 值（即 0）。
     *
     * <p>注意：本类按线程缓存 {@link CRC32C}，因此<b>不支持</b>把两次独立计算的 CRC "拼接"
     * 成一个 CRC（CRC 不是可组合运算）。需要组合多个区域时，请把它们放进同一个缓冲区后用
     * {@link #computeRange(ByteBuffer, int, int)} 一次算完。
     *
     * @return 初值 0
     */
    public static int begin() {
        return 0;
    }
}
