package com.walcore.support;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.Wal;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 测试期装配助手（<b>测试支持代码，不属于交付 API</b>）。
 *
 * <p>把"造一个能在 Windows 上跑、段很小、后台线程不拖慢测试"的 WAL 配置集中到一处，
 * 避免 8 个测试类各写一遍 40 行 {@code WalBuilder} 链。
 *
 * <p>刻意选的默认值及理由：
 * <ul>
 *   <li>{@code segmentSizeBytes = 4096}：让"跨段 / 轮转 / 恢复多段"在几百条记录内就发生；</li>
 *   <li>{@code partitionCount = 2}：够测多分区隔离，又不会在 {@code @TempDir} 里堆几十个目录；</li>
 *   <li>{@code stagingBufferBytes = 1MB}、{@code mappedWindowBytes = 1MB}：
 *       堆外占用可控，测试机不会因为并行跑 8 个测试类而 OOM；</li>
 *   <li>{@code batchIntervalMicros = 2000}：BATCH 策略下也能在 {@code flushAndWait} 超时内刷完；</li>
 *   <li>{@code FsyncPolicy.BATCH}：与生产默认一致，且是唯一能同时验证"写线程不 fsync"的策略。</li>
 * </ul>
 */
public final class TestWal {

    private TestWal() {
    }

    /**
     * 造一个测试用配置构建器。
     *
     * @param dir 日志目录
     * @param segmentSizeBytes 段大小
     * @return 构建器
     */
    public static WalBuilder builder(Path dir, long segmentSizeBytes) {
        return new WalBuilder()
                .baseDir(dir)
                .partitions(2)
                .maxPartitions(16)
                .segmentSizeBytes(segmentSizeBytes)
                .segmentMaxRecords(10_000_000)
                .segmentMaxAgeMillis(3_600_000L)
                .timeBasedRotation(false)
                .stagingBufferBytes(1L << 20)
                .mappedWindowBytes(1L << 20)
                .mappedWindowCount(4)
                .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)
                .batchIntervalMicros(2000)
                .batchMaxRecords(4096)
                .batchMaxBytes(1L << 20 > Integer.MAX_VALUE ? Integer.MAX_VALUE : (1 << 20))
                .flushIntervalMillis(50)
                .groupCommitThreads(1)
                .indexIntervalRecords(200)
                .maxDirtyBytes(32L << 20)
                .backpressureTimeoutMillis(2000L)
                .rotationTimeoutMillis(2000L)
                .flushAndWaitTimeoutMillis(10_000L)
                .metricsEnabled(true);
    }

    /**
     * 用默认段大小打开。
     *
     * @param dir 日志目录
     * @return WAL 实例
     */
    public static DefaultWal open(Path dir) {
        return open(dir, 4096L, b -> {
        });
    }

    /**
     * 打开 WAL。
     *
     * @param dir 日志目录
     * @param segmentSizeBytes 段大小
     * @param customizer 额外配置
     * @return WAL 实例
     */
    public static DefaultWal open(Path dir, long segmentSizeBytes, Consumer<WalBuilder> customizer) {
        WalBuilder b = builder(dir, segmentSizeBytes);
        customizer.accept(b);
        WalConfig config = b.buildConfig();
        return DefaultWal.open(config);
    }

    /**
     * 造一个 payload。
     *
     * @param len 字节数
     * @return 缓冲区（position=0，limit=len）
     */
    public static ByteBuffer payload(int len) {
        return payload(len, 0);
    }

    /**
     * 造一个带可辨识内容的 payload。
     *
     * @param len 字节数
     * @param seed 填充种子
     * @return 缓冲区
     */
    public static ByteBuffer payload(int len, int seed) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (seed + i);
        }
        return ByteBuffer.wrap(bytes);
    }

    /**
     * 造一个文本 payload。
     *
     * @param text 文本
     * @return 缓冲区
     */
    public static ByteBuffer text(String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 用游标读回某分区的全部记录。
     *
     * @param wal 实例
     * @param partitionId 分区 ID
     * @return 记录副本列表
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
     * 用游标读回某分区的全部 LSN。
     *
     * @param wal 实例
     * @param partitionId 分区 ID
     * @return LSN 列表（文件顺序）
     */
    public static List<Long> readAllLsns(Wal wal, int partitionId) {
        List<Long> out = new ArrayList<>();
        try (WalCursor cursor = wal.cursor(partitionId,
                CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
            WalRecord record;
            while ((record = cursor.next()) != null) {
                out.add(record.lsn());
            }
        }
        return out;
    }

    /**
     * 追加一条小记录（默认选项）。
     *
     * @param wal 实例
     * @param partitionId 分区 ID
     * @param payloadLen payload 字节数
     * @return LSN
     */
    public static long append(Wal wal, int partitionId, int payloadLen) {
        return wal.append(partitionId, payload(payloadLen), AppendOptions.DEFAULTS);
    }
}
