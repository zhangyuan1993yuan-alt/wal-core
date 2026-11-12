package com.walcore.examples;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * README「5 分钟上手」那段代码的<b>可执行副本</b>：读者照抄就能跑。
 *
 * <h2>为什么要有一个专门的测试来跑 README 的代码</h2>
 * README 里的示例是最容易被"顺手改签名后忘记同步"的部分，而读者第一次接触产品看的就是它。
 * 一旦它编译不过，产品的第一印象就是"这库跑不起来"。把这段代码原封不动放进测试，
 * 签名一改 CI 就红，比任何 review 都可靠。
 *
 * <p>唯一的差别：README 用 {@code Path.of("./demo-wal")}，这里用 {@code @TempDir}
 * （CI 上不能污染固定路径）。
 *
 * <h2>这段代码同时验证了两条容易被忽略的契约</h2>
 * <ol>
 *   <li>{@link Wal} 是 {@link AutoCloseable}：<b>必须</b>用 try-with-resources。
 *       不 close 会留下段文件的 mmap，Windows 上连临时目录都删不掉；</li>
 *   <li>{@link WalRecord#payload()} 是<b>只读视图</b>（底层可能是段文件 mmap 片段），
 *       {@code array()} 会抛 {@link UnsupportedOperationException}；
 *       要拿字节请用 {@link WalRecord#payloadBytes()} / {@link WalRecord#payloadAsUtf8()}。</li>
 * </ol>
 */
class QuickStartSmokeTest {

    @Test
    @DisplayName("README 的 10 行上手代码：写一条 -> 等落盘 -> 读回来")
    void readmeQuickStartRuns(@TempDir Path baseDir) {
        List<String> read = new ArrayList<>();

        // ---- 以下与 README.md「5 分钟上手」代码块逐字一致（仅 baseDir 来源不同）----
        try (Wal wal = new WalBuilder()
                .baseDir(baseDir)
                .partitions(4)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
                .build()) {
            long lsn = wal.append(0, ByteBuffer.wrap("order-created".getBytes(StandardCharsets.UTF_8)),
                    AppendOptions.DEFAULTS);
            assertTrue(wal.flushAndWait(0, lsn, 1_000L), "flushAndWait 应在 1s 内确认持久化");
            try (WalCursor cursor = wal.cursor(0, CursorOptions.builder().fromLsn(1L).build())) {
                WalRecord record;
                while ((record = cursor.next()) != null) {
                    read.add(record.payloadAsUtf8());
                }
            }
        }
        // ---- 上手代码结束 ----

        assertEquals(List.of("order-created"), read, "读回来的内容必须与写入一致");
    }

    @Test
    @DisplayName("上手代码同时验证 LSN 从 1 起，且重开实例能读回旧数据")
    void quickStartSurvivesReopen(@TempDir Path baseDir) {
        long lsn;
        try (Wal wal = new WalBuilder().baseDir(baseDir).partitions(1).build()) {
            lsn = wal.append(0, ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
                    AppendOptions.DEFAULTS);
        }
        assertEquals(1L, lsn, "第一个 LSN 是 1（从 1 起，不是 0）");

        List<String> read = new ArrayList<>();
        try (Wal wal = new WalBuilder().baseDir(baseDir).partitions(1).build();
             WalCursor cursor = wal.cursor(0, CursorOptions.builder().fromLsn(1L).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                read.add(record.payloadAsUtf8());
            }
        }
        assertEquals(List.of("a"), read, "close 后数据必须可被新实例读回");
    }
}
