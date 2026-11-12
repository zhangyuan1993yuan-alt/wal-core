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
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T19 骨架：写入吞吐基准（<b>默认不跑</b>）。
 *
 * <h2>为什么默认不跑</h2>
 * 类名以 {@code IT} 结尾，不在 surefire 的默认 include 里（默认只匹配
 * {@code *Test}/{@code Test*}/{@code *Tests}/{@code *TestCase}），因此
 * {@code mvn -o test} 不会执行它；手工触发方式见 {@code docs/PERF-wal.md}。
 *
 * <h2>Windows 上的结果不可当验收</h2>
 * 本类在 Windows 上能跑通，但数字只用于"有没有数量级错误"的冒烟判断：
 * <ul>
 *   <li>{@code append} 走 {@code FileChannel} 后端（Windows 无 mmap 写路径），
 *       单条延迟比 Linux 高一个量级；</li>
 *   <li>Windows 的 {@code FlushFileBuffers} 与 NVMe 的 {@code fdatasync} 不是一回事，
 *       单次 force 在这台开发机上量到 ~20ms 量级；</li>
 *   <li>共享 CI/开发机上还叠加了杀毒、索引、其他进程的 IO。</li>
 * </ul>
 * 因此断言只保留"明显不对就报警"的量级下限，真正的验收数字必须在 Linux 目标机上
 * 用 {@code PERF-wal.md} 里的命令跑出来并归档。
 */
@Tag("perf")
class ThroughputIT {

    /** 每次测量的记录条数（不是时间驱动，保证可复现）。 */
    private static final int OPS = 200_000;

    /** payload 大小，与 ARCH §15.3 的 256B/条 预算口径对齐。 */
    private static final int PAYLOAD = 256;

    @Test
    @DisplayName("append 吞吐（单线程 + 多线程并发，纯内存路径）")
    void appendThroughput(@TempDir Path dir) {
        try (DefaultWal wal = TestWal.open(dir, 64L << 20, b -> {
        })) {
            ByteBuffer payload = TestWal.payload(PAYLOAD, 1);
            // 预热 10 万条：JIT、staging、段映射、LongAdder Cell 全部就位
            for (int i = 0; i < 100_000; i++) {
                wal.append(0, payload, AppendOptions.DEFAULTS);
            }

            // 预热期的样本不代表稳态，清一次指标让下面的分位数只反映测量窗口
            wal.metrics().reset();
            double single = measureAppendOps(wal, 0, payload, OPS);

            int threads = Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));
            double[] results = runParallel(threads, t ->
                    measureAppendOps(wal, t % 2, payload, OPS / threads));
            double multi = sum(results);

            WalStats stats = wal.stats();
            System.out.printf(Locale.ROOT, """
                    [PERF] append 吞吐（payload=%d B，纯内存路径）
                      单线程      : %,14.0f 条/s
                      %d 线程合计 : %,14.0f 条/s（分摊 2 个分区）
                      累计条数    : %,14d
                      日志大小    : %,14d B
                    """, PAYLOAD, single, threads, multi,
                    stats.totalRecords(), stats.totalLogBytes());

            assertTrue(single > 50_000.0d,
                    "单线程 append 吞吐异常低：" + single + " 条/s（疑似加了锁或系统调用）");
            assertTrue(multi > 100_000.0d,
                    "多线程 append 吞吐异常低：" + multi + " 条/s");
        }
    }

    @Test
    @DisplayName("flushAndWait（持久确认）吞吐：逐条确认 vs 流水线确认（两个口径都要写清）")
    void flushAndWaitThroughput(@TempDir Path dir) {
        // groupCommitThreads=4 + batchIntervalMicros=200：ARCH §15.2 的"低延迟档"。
        try (DefaultWal wal = TestWal.open(dir, 64L << 20, b -> b
                .groupCommitThreads(4)
                .batchIntervalMicros(200)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH))) {
            ByteBuffer payload = TestWal.payload(PAYLOAD, 2);
            for (int i = 0; i < 3_000; i++) {
                long lsn = wal.append(0, payload, AppendOptions.DEFAULTS);
                wal.flushAndWait(0, lsn, 10_000L);
            }

            // 预热（逐条确认）的样本会把 p50 拉到几十毫秒，清一次再测两个口径
            wal.metrics().reset();
            int threads = 4;
            // ---- 口径 A：严格逐条确认（sync-per-record）----
            // 每个写者 append 一条就等它持久。吞吐上限 ≈ 1 / (攒批间隔 + fsync)，
            // 这是"每笔都要等落盘"的业务（如转账）的真实口径，组提交在这里帮不上忙，
            // 因为任何时刻在途记录都只有 1 条。
            int strictPerThread = 500;
            long startedA = System.nanoTime();
            double[] a = runParallel(threads, t -> {
                int pid = t % 2;
                for (int i = 0; i < strictPerThread; i++) {
                    long lsn = wal.append(pid, payload, AppendOptions.DEFAULTS);
                    if (!wal.flushAndWait(pid, lsn, 10_000L)) {
                        throw new IllegalStateException("flushAndWait 超时: pid=" + pid);
                    }
                }
                return strictPerThread;
            });
            long elapsedA = System.nanoTime() - startedA;
            double strictOps = sum(a) * 1_000_000_000.0d / Math.max(1L, elapsedA);

            // ---- 口径 B：流水线确认（pipelined）----
            // 每个写者连写 64 条，最后只等一次"第 64 条持久"。这一条等回来时，
            // 前 63 条必然也在同一批里落了盘（LSN 单调 + 同批共用 ACK 点），
            // 这才是组提交真正的收益口径，也是撮合/行情推送类业务的典型形态。
            int batch = 64;
            int rounds = 200;
            long startedB = System.nanoTime();
            double[] b = runParallel(threads, t -> {
                int pid = t % 2;
                for (int r = 0; r < rounds; r++) {
                    long lsn = 0L;
                    for (int i = 0; i < batch; i++) {
                        lsn = wal.append(pid, payload, AppendOptions.DEFAULTS);
                    }
                    if (!wal.flushAndWait(pid, lsn, 10_000L)) {
                        throw new IllegalStateException("流水线 flushAndWait 超时: pid=" + pid);
                    }
                }
                return (double) rounds * batch;
            });
            long elapsedB = System.nanoTime() - startedB;
            double pipelinedOps = sum(b) * 1_000_000_000.0d / Math.max(1L, elapsedB);

            WalStats stats = wal.stats();
            WalStats.LatencyView flush = stats.latency("wal.flush.latency");
            System.out.printf(Locale.ROOT, """
                    [PERF] flushAndWait（groupCommitThreads=4, batchIntervalMicros=200, %d 写者）
                      口径 A 逐条确认  : %,14.0f 条/s（平均确认 %.1f µs）
                      口径 B 流水线确认: %,14.0f 条/s（每条平均 %.1f µs，含 64 条摊薄）
                      库内 flush 分位  : p50=%d ns, p99=%d ns, p999=%d ns
                      刷盘批次数        : %,14d（平均每批 %.1f 条）
                    """, threads, strictOps, elapsedA / 1000.0d / (threads * (double) strictPerThread),
                    pipelinedOps, elapsedB / 1000.0d / sum(b),
                    flush == null ? -1L : flush.p50Nanos(),
                    flush == null ? -1L : flush.p99Nanos(),
                    flush == null ? -1L : flush.p999Nanos(),
                    stats.flushBatches(),
                    stats.flushBatches() == 0L ? 0.0d
                            : (double) stats.totalRecords() / (double) stats.flushBatches());

            // 冒烟：两个口径都不该退化成"完全串行 + 每次都真 fsync 到爆"
            assertTrue(strictOps > 50.0d, "逐条确认吞吐异常低：" + strictOps + " 条/s");
            assertTrue(pipelinedOps > strictOps * 5.0d, String.format(Locale.ROOT,
                    "流水线确认应当显著高于逐条确认（组提交没起作用？）：%.0f vs %.0f",
                    pipelinedOps, strictOps));
            assertEquals(3_000L + (long) threads * strictPerThread
                    + (long) threads * rounds * batch, stats.totalRecords());
        }
    }

    /**
     * 并行跑同一段逻辑，收集每线程返回值；任一线程抛异常即失败。
     *
     * @param threads 线程数
     * @param body 逻辑（入参是线程下标，返回值计入结果）
     * @return 每线程返回值
     * @throws AssertionError 有线程抛异常
     */
    private static double[] runParallel(int threads, ThreadBody body) {
        double[] results = new double[threads];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch start = new CountDownLatch(1);
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int index = t;
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    results[index] = body.run(index);
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "bench-" + t);
            workers[t].start();
        }
        start.countDown();
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
        try {
            for (Thread w : workers) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    throw new AssertionError("基准线程未能在 5 分钟内结束");
                }
                w.join(Math.max(1L, remaining / 1_000_000L));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("基准被中断", e);
        }
        assertNull(failure.get(), "基准线程抛出异常: " + failure.get());
        return results;
    }

    /**
     * 求和。
     *
     * @param values 数组
     * @return 总和
     */
    private static double sum(double[] values) {
        double total = 0.0d;
        for (double v : values) {
            total += v;
        }
        return total;
    }

    /**
     * 测一段 append 的吞吐。
     *
     * @param wal WAL 实例
     * @param pid 分区
     * @param payload 复用的 payload
     * @param ops 条数
     * @return 条/秒
     */
    private static double measureAppendOps(DefaultWal wal, int pid, ByteBuffer payload, int ops) {
        long started = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            wal.append(pid, payload, AppendOptions.DEFAULTS);
        }
        long elapsed = System.nanoTime() - started;
        return ops * 1_000_000_000.0d / Math.max(1L, elapsed);
    }

    /** 可抛异常、带下标入参的逻辑。 */
    @FunctionalInterface
    private interface ThreadBody {
        /**
         * 执行。
         *
         * @param index 线程下标
         * @return 结果值
         * @throws Exception 失败
         */
        double run(int index) throws Exception;
    }
}
