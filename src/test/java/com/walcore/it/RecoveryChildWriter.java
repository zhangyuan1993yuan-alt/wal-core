package com.walcore.it;

import com.walcore.api.AppendOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalConfig;
import com.walcore.support.TestWal;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * kill -9 恢复基准的<b>子进程</b>：疯狂写入直到被杀。
 *
 * <h2>为什么必须用子进程</h2>
 * "进程被 SIGKILL 时日志是否可恢复"这件事无法在同一进程内验证——同一个 JVM 里
 * {@code Thread.stop()} 不会让文件系统处于"半写"状态，也测不出 mmap 的脏页是否落盘。
 * 唯一可信的办法是：起一个子 JVM 真写，然后给它发 SIGKILL（Linux）/ TerminateProcess
 * （Windows），再在父进程里重新 {@code open()} 并校验。
 *
 * <p>子进程每写一条就把 LSN 与 payload 的可辨识前缀 {@code println} 到 stdout，
 * 父进程据此知道"被杀之前它至少写了多少条"，用于判定日志是否是<b>前缀</b>而不是"能打开就行"。
 *
 * <h2>输出格式（父进程按此解析）</h2>
 * {@code READY <baseDir>} 一行表示已打开日志；之后每条是 {@code W <lsn>}。
 */
public final class RecoveryChildWriter {

    private RecoveryChildWriter() {
    }

    /**
     * 子进程入口。
     *
     * @param args {@code [0] = baseDir}，{@code [1] = 段大小字节数}（可选）
     * @throws Exception 打开或写入失败
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: RecoveryChildWriter <baseDir> [segmentSizeBytes]");
            System.exit(2);
        }
        Path baseDir = Paths.get(args[0]);
        long segmentSize = args.length > 1 ? Long.parseLong(args[1]) : (4L << 20);
        int payloadLen = 128;

        try (Wal wal = TestWal.builder(baseDir, segmentSize)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
                // 攒批窗口压到 200µs：让"被杀之前落了哪些盘"这件事更接近"最新数据"，
                // 更能暴露"确认了但没落盘"这类假确认问题
                .batchIntervalMicros(200)
                .build()) {
            System.out.println("READY " + baseDir.toAbsolutePath());
            System.out.flush();
            // 无限写，直到被外部杀掉
            for (long i = 0; ; i++) {
                byte[] bytes = new byte[payloadLen];
                String tag = "rec-" + i;
                byte[] tagBytes = tag.getBytes(StandardCharsets.US_ASCII);
                System.arraycopy(tagBytes, 0, bytes, 0, Math.min(tagBytes.length, bytes.length));
                long lsn = wal.append(0, java.nio.ByteBuffer.wrap(bytes), AppendOptions.DEFAULTS);
                if ((i & 0x3F) == 0L) {
                    // 每 64 条打一行，避免 stdout 成为瓶颈；父进程只需要一个下界
                    System.out.println("W " + lsn);
                    System.out.flush();
                }
            }
        }
    }
}
