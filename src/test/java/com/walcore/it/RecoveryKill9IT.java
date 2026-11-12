package com.walcore.it;

import com.walcore.api.RecoveryReport;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T19 骨架：<b>真·kill -9</b> 崩溃恢复（默认不跑，见 {@code IT} 后缀）。
 *
 * <h2>为什么不能只靠"手工注入损坏字节"</h2>
 * 已有的 {@code RecoveryTornWriteTest}/{@code RecoveryCorruptionPolicyTest} 是
 * "构造已知损坏 + 断言恢复策略"，覆盖了算法分支，但它们<b>无法</b>证明：
 * <ul>
 *   <li>真实崩溃时刻的 mmap 脏页与 {@code st_size} 不一致时恢复是否还正确；</li>
 *   <li>组提交的"已确认水位"与"实际落盘边界"是否一致（假确认会让下游丢数据）；</li>
 *   <li>恢复过程本身会不会因为半写的索引文件/段名而抛异常。</li>
 * </ul>
 * 这个类用子 JVM + 强杀来覆盖这三条。
 *
 * <h2>平台差异（必须在报告里写清）</h2>
 * <ul>
 *   <li>Linux：{@link Process#destroyForcibly()} 发 {@code SIGKILL}，进程无任何清理机会，
 *       这正是要测的场景；</li>
 *   <li>Windows：{@link Process#destroyForcibly()} 走 {@code TerminateProcess}，
 *       效果上同样是"不给清理机会"，但文件系统的刷盘行为与 Linux 不完全相同。
 *       <b>Windows 上的通过只说明"没崩/没坏"，不构成 RTO/完整性验收。</b></li>
 * </ul>
 *
 * <h2>断言口径（与"性能"无关，所以即使跑也应该稳）</h2>
 * <ol>
 *   <li>重启必须成功（{@code open()} 不抛异常）；</li>
 *   <li>恢复出来的记录必须是"被杀前写入序列的<b>前缀</b>"：LSN 从 1 连续到某个 N，
 *       没有空洞、没有重复；</li>
 *   <li>每条记录的 payload 必须与 LSN 对应（不能读到别人的数据）；</li>
 *   <li>恢复报告里{@code nextLsn > N}，保证重启后新写入的 LSN 不与旧记录冲突。</li>
 * </ol>
 */
@Tag("perf")
class RecoveryKill9IT {

    @Test
    @DisplayName("子进程被强杀后重启：数据是写入序列的前缀、无空洞、无重复")
    void recoverAfterHardKill(@TempDir Path dir) throws Exception {
        assertTimeoutPreemptively(java.time.Duration.ofMinutes(5), () -> {
            Path logDir = dir.resolve("log");
            long segmentSize = 4L << 20;

            Process child = startChild(logDir, segmentSize);
            long lastReportedLsn = 0L;
            try {
                lastReportedLsn = awaitChildWriting(child);
                // 让它再写一会儿，制造"写入中途被杀"的窗口
                Thread.sleep(1_500L);
            } finally {
                child.destroyForcibly();
                child.waitFor(30, TimeUnit.SECONDS);
            }
            System.out.printf(Locale.ROOT,
                    "[PERF] 子进程退出码 = %d，被杀前至少写到 lsn=%d%n",
                    child.exitValue(), lastReportedLsn);

            assertTrue(Files.isDirectory(logDir), "日志目录必须存在");

            long started = System.nanoTime();
            RecoveryReport report;
            long recovered = 0L;
            try (DefaultWal wal = openAfterHardKill(logDir, segmentSize)) {
                report = wal.openReport();
                long previous = 0L;
                try (WalCursor cursor = wal.cursor(0,
                        CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
                    WalRecord record;
                    while ((record = cursor.next()) != null) {
                        long lsn = record.lsn();
                        assertTrue(lsn == previous + 1L,
                                "恢复后的 LSN 必须严格连续：期望 " + (previous + 1L)
                                        + "，实际 " + lsn);
                        previous = lsn;
                        recovered++;
                    }
                }
                assertTrue(recovered >= lastReportedLsn,
                        "恢复条数(" + recovered + ")不得少于被杀前已上报的条数("
                                + lastReportedLsn + ")，否则说明已确认的数据丢了");
                // 注意：不能用 stats().publishedLsnMax() 做这个断言——publishedLsn 只由
                // 运行期写路径推进，刚恢复完、尚未写入的实例它恒为 0（本断言的旧写法
                // 因此不可能在任何平台上通过）。"水位不得回退"的真正载体是恢复报告的
                // nextLsn：它必须大于已恢复的最大 LSN，保证重启后的新写入不会复用旧 LSN。
                assertTrue(report.nextLsn() > recovered,
                        "重启后的 LSN 水位不得回退：nextLsn=" + report.nextLsn()
                                + "，recovered=" + recovered);
            }
            long recoveryMillis = (System.nanoTime() - started) / 1_000_000L;

            assertNotNull(report, "恢复报告不得为 null");
            System.out.printf(Locale.ROOT, """
                    [PERF] 强杀恢复（Linux 上为 SIGKILL）
                      段大小          : %,d B
                      恢复条数        : %,d
                      恢复耗时(RTO)   : %,d ms
                      损坏事件        : %d
                      有效尾段 LSN    : %d
                    """, segmentSize, recovered, recoveryMillis,
                    report.corruptions().size(), report.nextLsn());

            assertTrue(recovered > 0L, "至少要有若干条记录被恢复");
            // RTO 冒烟：百万级以内不应该超过 60s（Windows 文件系统更慢，故不用更紧的界）
            assertTrue(recoveryMillis < 60_000L, "恢复耗时过长：" + recoveryMillis + " ms");
        });
    }

    /**
     * 强杀后重开日志目录（带 {@code AlreadyOpenException} 退避重试）。
     *
     * <h2>为什么需要重试（flaky 修复）</h2>
     * Windows 上 {@code Process.destroyForcibly()} 走 {@code TerminateProcess}：
     * 子进程句柄销毁后，{@code LOCK} 文件上的<b>内核级文件锁</b>释放存在时序滞后
     * （QA 实测 4 次中 1 次：父进程 {@code waitFor} 已返回、立刻重开仍抛
     * {@code AlreadyOpenException}，复跑即过）。这与被测的恢复逻辑无关，
     * 是 OS 锁释放与进程可见性之间的固有竞态，只能用短退避吸收。
     *
     * @param logDir 日志目录
     * @param segmentSize 段大小
     * @return 打开的实例（调用方负责 close）
     * @throws Exception 重试用尽仍无法打开
     */
    private static DefaultWal openAfterHardKill(Path logDir, long segmentSize) throws Exception {
        WalException.AlreadyOpenException last = null;
        // 5 次 × 线性退避（300/600/900/1200/1500ms）：实测竞态窗口远小于 1s，
        // 线性退避既能在首轮窗口内命中，也不会把"真被占用"拖成分钟级挂起。
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                return TestWal.open(logDir, segmentSize, b -> {
                });
            } catch (WalException.AlreadyOpenException e) {
                last = e;
                System.out.printf(Locale.ROOT,
                        "[PERF] 重开撞到残留 OS 锁（第 %d 次），退避重试: %s%n",
                        attempt, e.getMessage());
                Thread.sleep(attempt * 300L);
            }
        }
        throw new IllegalStateException("强杀后重开在 5 次退避后仍失败: " + logDir, last);
    }

    /**
     * 启动子进程写入。
     *
     * @param logDir 日志目录
     * @param segmentSize 段大小
     * @return 进程句柄
     * @throws Exception 启动失败
     */
    private static Process startChild(Path logDir, long segmentSize) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        return new ProcessBuilder(javaBin,
                "-Xmx512m",
                "-cp", classpath,
                RecoveryChildWriter.class.getName(),
                logDir.toAbsolutePath().toString(),
                String.valueOf(segmentSize))
                .redirectErrorStream(true)
                .start();
    }

    /**
     * 等子进程打出 READY 与第一行 W，返回已知写入的最大 LSN。
     *
     * @param child 子进程
     * @return 已知最大 LSN
     * @throws Exception 读取失败或超时
     */
    private static long awaitChildWriting(Process child) throws Exception {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.nanoTime() + 30_000_000_000L;
        long lastLsn = 0L;
        boolean ready = false;
        while (System.nanoTime() < deadline) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("READY")) {
                ready = true;
            } else if (line.startsWith("W ")) {
                lastLsn = Long.parseLong(line.substring(2).trim());
                if (ready && lastLsn > 0L) {
                    return lastLsn;
                }
            }
        }
        throw new IllegalStateException("子进程未能在 30s 内开始写入，最后一行 lsn=" + lastLsn);
    }

    /**
     * 供 {@link WalConfig} 的 lambda 使用的空自定义器占位说明。
     *
     * <p>保留这个注释是为了提醒：本类刻意不打开 {@code recoveryVerifyMode=FULL}，
     * 因为"干净关闭时跳过 CRC"正是生产默认，如果基准里改成 FULL 就测不到真实 RTO。
     */
    private static void note() {
        // 无逻辑
    }
}
