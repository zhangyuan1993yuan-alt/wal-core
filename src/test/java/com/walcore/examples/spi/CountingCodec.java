package com.walcore.examples.spi;

import com.walcore.api.WalVersion;
import com.walcore.spi.PluginContext;
import com.walcore.spi.RecordCodec;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;

/**
 * 示例插件 ①：<b>计数型编码器</b>——零依赖、零拷贝，只在 RAW 的恒等映射外加两个计数器。
 *
 * <h2>这个例子要教会什么</h2>
 * <ol>
 *   <li>{@link RecordCodec} 的<b>三个数值契约</b>：{@code encode} 返回"实际存储字节数"，
 *       {@code decode} 返回"还原出的逻辑字节数"，{@code maxStoredSize} 返回
 *       "最坏情况下存储字节数的上界"。框架用 {@code maxStoredSize} <b>预分配</b>帧空间，
 *       因此它<b>必须</b>是上界而不是典型值——返回偏小会让 {@code encode} 装不下而返回
 *       {@code -1}，框架会抛 {@code codec 实际帧长 -1 与预估 N 不一致}。</li>
 *   <li>编码器<b>不能</b>改动 {@code src} 的 position/limit：那是调用方的缓冲区，
 *       多线程可能同时读它。</li>
 *   <li>编码器必须是<b>无状态或线程安全</b>的：{@code encode} 会被多个写线程并发调用，
 *       且位于分区写锁<b>之内</b>——在这里做任何阻塞操作都会直接拖垮写入延迟。</li>
 * </ol>
 *
 * <h2>怎么用</h2>
 * <pre>{@code
 * // ① 直接指定（无需 META-INF/services）
 * Wal wal = new WalBuilder().codec(CountingCodec.ID).build();
 * // ② 或者在插件 jar 里注册 META-INF/services/com.walcore.spi.RecordCodec
 * }</pre>
 *
 * <p><b>注意</b>：本类放在 {@code src/test/java} 下，是<b>示例代码</b>而不是交付 API；
 * 使用方应当把它复制到自己的插件工程里（编译依赖 wal-core 且 scope=provided）。
 */
public final class CountingCodec implements RecordCodec {

    /** 插件 id：约定的 {@code <组织>:<名字>} 形式，全库唯一。 */
    public static final String ID = "example:codec-counting";

    private final LongAdder encodedFrames = new LongAdder();
    private final LongAdder encodedBytes = new LongAdder();
    private final LongAdder decodedFrames = new LongAdder();
    private volatile PluginContext context;

    /**
     * 无参构造：{@code ServiceLoader} 要求 provider 有 public 无参构造。
     */
    public CountingCodec() {
        // 无状态初始化，计数器字段已就绪
    }

    @Override
    public String id() {
        return ID;
    }

    /**
     * 帧头里的数值 id。
     *
     * <p>约定：{@code 0 = RAW}（保留）；{@code 1~127} 内置保留；{@code 128~255} 第三方。
     * 发布后<b>绝不</b>能改这个数字，也不能把同一个数字分配给另一个语义的编码器，
     * 否则历史数据会被解错（帧头只记数字，不记类名）。
     *
     * @return 128（示例占用的第三方区间起点）
     */
    @Override
    public int codecId() {
        return 128;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext pluginContext) {
        // 框架保证在首次 encode/decode 之前调用一次；这里保存下来只是为了演示
        // "插件可以读配置/打日志"，真正的编码路径不该依赖 context。
        this.context = pluginContext;
    }

    @Override
    public void close() {
        this.context = null;
    }

    @Override
    public int encode(ByteBuffer src, int srcOffset, int srcLen,
                      ByteBuffer dst, int dstOffset, int dstCapacity) {
        if (srcLen < 0) {
            throw new IllegalArgumentException("srcLen 不得为负: " + srcLen);
        }
        if (dstCapacity < srcLen) {
            // 契约：装不下就返回 -1，由框架决定抛错还是换路径；不要抛异常中断写入
            return -1;
        }
        dst.put(dstOffset, src, srcOffset, srcLen);
        encodedFrames.increment();
        encodedBytes.add(srcLen);
        return srcLen;
    }

    @Override
    public int decode(ByteBuffer src, int srcOffset, int storedLen, int logicalLen,
                      ByteBuffer dst, int dstOffset) {
        if (storedLen != logicalLen) {
            throw new IllegalArgumentException(
                    "计数型编码器是恒等映射，storedLen(" + storedLen
                            + ") 必须等于 logicalLen(" + logicalLen + ")");
        }
        dst.put(dstOffset, src, srcOffset, storedLen);
        decodedFrames.increment();
        return logicalLen;
    }

    @Override
    public int maxStoredSize(int logicalLen) {
        // 恒等映射：上界就是逻辑长度本身
        return logicalLen;
    }

    /**
     * 已编码帧数（示例的可观测点，业务代码可据此暴露自定义指标）。
     *
     * @return 帧数
     */
    public long encodedFrames() {
        return encodedFrames.sum();
    }

    /**
     * 已编码的逻辑字节数。
     *
     * @return 字节数
     */
    public long encodedBytes() {
        return encodedBytes.sum();
    }

    /**
     * 已解码帧数。
     *
     * @return 帧数
     */
    public long decodedFrames() {
        return decodedFrames.sum();
    }

    @Override
    public String toString() {
        return "CountingCodec{encoded=" + encodedFrames() + ", bytes=" + encodedBytes() + '}';
    }
}
