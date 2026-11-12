package com.walcore.it;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalConfig;
import com.walcore.api.WalStats;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T19 骨架：延迟基准（<b>默认不跑</b>，见类名后缀 {@code IT}）。
 *
 * <h2>这个类要回答的两个问题（口径必须分开）</h2>
 * <ol>
 *   <li>{@code append()} 的 p50/p99/p999 —— <b>纯内存路径</b>，不含 fsync。
 *       ARCH §15.1 预算：p50 0.48µs / p99 1.52µs；PRD §5 目标 p50 ≤ 5µs、p99 ≤ 100µs。</li>
 *   <li>{@code flushAndWait()} 的 p50/p99/p999 —— <b>持久确认路径</b>，
 *       含攒批等待 + {@code fdatasync}。ARCH §15.2 预算：单分区 p50 ~180µs / p99 ~500µs；
 *       16 分区、2 flusher 时 p99 ~1.4ms。</li>
 * </ol>
 *
 * <p>把两者混成一个"延迟"数字是 WAL 领域最常见的口径错误：一个数量级的差距会被
 * 平均成一个谁也不认的数。这个类输出的是两张独立的表。
 *
 * <h2>计数方式</h2>
 * 延迟用库自己的 {@link LatencyHistogram}（固定桶 + 桶内 min/max 插值），
 * 与生产监控完全同一套算法——这样基准数字和线上看板数字才可比。
 * 同时输出"原始数组排序"得到的精确分位，两者互相印证。
 */
@Tag("perf")
class LatencyBenchIT {

    private static final int OPS = 200_000;
    private static final int PAYLOAD = 256;

    @Test
    @DisplayName("append 纯内存路径延迟：p50/p99/p999")
    void appendLatency(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 64L << 20, b -> {
        })) {
            ByteBuffer payload = TestWal.payload(PAYLOAD, 3);
            for (int i = 0; i < 100_000; i++) {
                wal.append(0, payload, AppendOptions.DEFAULTS);
            }

            // 清一次：让库内直方图的 p50/p99 只反映测量窗口，与下面的精确分位可比
            wal.metrics().reset();
            long[] samples = new long[OPS];
            for (int i = 0; i < OPS; i++) {
                long t0 = System.nanoTime();
                wal.append(0, payload, AppendOptions.DEFAULTS);
                samples[i] = System.nanoTime() - t0;
            }

            printTable("append()（纯内存路径）", samples);

            WalStats.LatencyView view = wal.stats().latency("wal.append.latency");
            assertTrue(view != null && view.count() > 0L, "库内直方图必须采到样本");
            System.out.printf(Locale.ROOT,
                    "[PERF]   库内直方图（1/64 抽样，n=%d）：p50=%d ns, p99=%d ns, p999=%d ns%n",
                    view.count(), view.p50Nanos(), view.p99Nanos(), view.p999Nanos());

            // 冒烟：Windows 上 append p99 约 1~5µs；超过 200µs 说明某处退化成了系统调用
            assertTrue(percentile(samples, 0.99d) < 200_000L,
                    "append p99 异常高：" + percentile(samples, 0.99d) + " ns");
        }
    }

    @Test
    @DisplayName("flushAndWait 持久确认延迟：p50/p99/p999（与 append 分开计）")
    void flushLatency(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 64L << 20, b -> b
                .groupCommitThreads(2)
                .batchIntervalMicros(200)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH))) {
            ByteBuffer payload = TestWal.payload(PAYLOAD, 4);
            for (int i = 0; i < 1_000; i++) {
                long lsn = wal.append(0, payload, AppendOptions.DEFAULTS);
                wal.flushAndWait(0, lsn, 10_000L);
            }

            wal.metrics().reset();
            // 持久确认单次就是毫秒级，2000 条已足够估 p50/p99（再多只是拉长跑时）
            int ops = 2_000;
            long[] samples = new long[ops];
            for (int i = 0; i < ops; i++) {
                long lsn = wal.append(0, payload, AppendOptions.DEFAULTS);
                long t0 = System.nanoTime();
                wal.flushAndWait(0, lsn, 10_000L);
                samples[i] = System.nanoTime() - t0;
            }

            printTable("flushAndWait()（持久确认，groupCommitThreads=2）", samples);
            WalStats.LatencyView view = wal.stats().latency("wal.flush.latency");
            assertTrue(view != null && view.count() > 0L, "库内直方图必须采到样本");
            System.out.printf(Locale.ROOT,
                    "[PERF]   库内直方图（全量，n=%d）：p50=%d ns, p99=%d ns, p999=%d ns%n",
                    view.count(), view.p50Nanos(), view.p99Nanos(), view.p999Nanos());

            // 冒烟：持久确认必然 >= 攒批间隔量级；若 p50 小于 1µs 说明根本没等到 fsync
            assertTrue(percentile(samples, 0.50d) > 1_000L,
                    "flushAndWait p50 小于 1µs —— 组提交可能返回了假确认");
            assertEquals(ops + 1_000L, wal.stats().totalRecords());
        }
    }

    /**
     * 打印延迟表（精确分位 + 均值）。
     *
     * @param title 标题
     * @param samples 原始样本（纳秒）
     */
    private static void printTable(String title, long[] samples) {
        long[] sorted = samples.clone();
        Arrays.sort(sorted);
        double meanMicros = 0.0d;
        for (long s : sorted) {
            meanMicros += s;
        }
        meanMicros = meanMicros / sorted.length / 1000.0d;
        System.out.printf(Locale.ROOT, """
                [PERF] %s
                  n=%d  mean=%.2f µs
                  p50=%.2f µs  p90=%.2f µs  p99=%.2f µs  p999=%.2f µs  max=%.2f µs
                """, title, sorted.length, meanMicros,
                percentile(sorted, 0.50d) / 1000.0d,
                percentile(sorted, 0.90d) / 1000.0d,
                percentile(sorted, 0.99d) / 1000.0d,
                percentile(sorted, 0.999d) / 1000.0d,
                sorted[sorted.length - 1] / 1000.0d);
    }

    /**
     * 精确分位（对已排序数组取最近秩，不做插值）。
     *
     * @param sorted 已升序排列的样本
     * @param p 分位
     * @return 纳秒
     */
    private static long percentile(long[] sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        if (idx < 0) {
            idx = 0;
        }
        if (idx >= sorted.length) {
            idx = sorted.length - 1;
        }
        return sorted[idx];
    }
}
