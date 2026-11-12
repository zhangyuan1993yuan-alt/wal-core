package com.walcore.qa;

import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * QA（严过关）对抗性测试的公共辅助。
 *
 * <p>与 {@code com.walcore.support.TestWal} 的分工：TestWal 是工程交付的装配助手，
 * 本类只放 QA 自己的校验逻辑（期望值构造、全量读回、轮询等待），
 * 避免为了 QA 断言去修改工程师已有的测试文件。
 */
public final class QaSupport {

    private QaSupport() {
    }

    /**
     * 与 {@code TestWal.payload(len, seed)} 同构，但种子为 long（覆盖大 LSN 空间）。
     *
     * @param len payload 字节数
     * @param seed 种子（LSN 或序号）
     * @return 字节数组
     */
    public static byte[] seededPayload(int len, long seed) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) ((seed * 31L + i * 7L) & 0xFFL);
        }
        return bytes;
    }

    /**
     * 读回某分区全部记录并深拷贝（安全持有）。
     *
     * @param wal 实例
     * @param partitionId 分区
     * @return 记录副本列表（按文件顺序）
     */
    public static List<WalRecord> readAll(Wal wal, int partitionId) {
        List<WalRecord> out = new ArrayList<>();
        try (WalCursor cursor = wal.cursor(partitionId,
                CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.copy());
            }
        }
        return out;
    }

    /**
     * 轮询等待条件成立（对抗性测试里大量场景是异步的：tailer 追平、保留策略删段）。
     *
     * @param condition 条件
     * @param timeoutMillis 超时
     * @param message 失败消息
     * @return 成立返回 true；超时返回 false（由调用方决定断言还是分支）
     */
    public static boolean awaitTrue(BooleanSupplier condition, long timeoutMillis, String message) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待被打断: " + message, e);
            }
        }
        return false;
    }

    /**
     * 断言用：等待条件成立，超时直接失败。
     *
     * @param condition 条件
     * @param timeoutMillis 超时
     * @param message 失败消息
     */
    public static void mustAwait(BooleanSupplier condition, long timeoutMillis, String message) {
        if (!awaitTrue(condition, timeoutMillis, message)) {
            throw new AssertionError("超时(" + timeoutMillis + "ms)未达成: " + message);
        }
    }
}
