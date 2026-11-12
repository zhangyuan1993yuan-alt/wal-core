package com.walcore.qa;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QA 独立复核：README §1"5 分钟上手"代码原样抄录成测试。
 *
 * <p>README 承诺这段代码输出 {@code 1 -> order-created}。本用例不使用任何工程侧
 * 已有的烟测（{@code QuickStartSmokeTest}），完全照 README 抄一遍并断言预期输出，
 * 防止"示例签名改了但文档没同步"或"烟测被改得和 README 不一致"这两类事故。
 */
@DisplayName("QA: README 5 分钟上手代码逐字复核")
class QaReadmeQuickStartTest {

    @Test
    @DisplayName("README §1 十行代码：输出恰为 '1 -> order-created'，flushAndWait=true，close 后写入被拒")
    void readmeQuickStartVerbatim(@TempDir Path dir) {
        List<String> printedLines = new ArrayList<>();

        // ---- 以下与 README §1 逐字等价（仅把 System.out 收集起来、目录换 @TempDir）----
        Wal wal = new WalBuilder()
                .baseDir(dir)
                .partitions(4)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
                .build();

        long lsn = wal.append(0,
                ByteBuffer.wrap("order-created".getBytes(StandardCharsets.UTF_8)),
                AppendOptions.DEFAULTS);

        boolean durable = wal.flushAndWait(0, lsn, 1_000L);

        try (WalCursor cursor = wal.cursor(0,
                CursorOptions.builder().fromLsn(1L).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                printedLines.add(record.lsn() + " -> " + record.payloadAsUtf8());
            }
        }

        wal.close();
        // ---- 抄录结束 ----

        assertEquals(1L, lsn, "首条记录 LSN 必须为 1");
        assertTrue(durable, "flushAndWait 必须返回 true（这是唯一可回 ACK 的口径）");
        assertEquals(List.of("1 -> order-created"), printedLines,
                "输出必须与 README 写的预期完全一致");

        // P0-10 ②：close 后新 append 必须被拒绝并给出明确错误码
        WalException ex = assertThrows(WalException.ClosedException.class,
                () -> wal.append(0, ByteBuffer.wrap("after-close".getBytes(StandardCharsets.UTF_8)),
                        AppendOptions.DEFAULTS),
                "close 后 append 必须抛 ClosedException");
        assertEquals("WAL_CLOSED", ex.errorCode(), "错误码必须是 WAL_CLOSED");
        assertTrue(wal.isClosed(), "close 后 isClosed 必须为 true");
    }
}
