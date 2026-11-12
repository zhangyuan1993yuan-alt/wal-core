package com.walcore.retention;

import com.walcore.api.AppendOptions;
import com.walcore.api.CursorOptions;
import com.walcore.api.WalCursor;
import com.walcore.api.WalRecord;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.retention.RetentionManager;
import com.walcore.internal.segment.Segment;
import com.walcore.spi.PluginContext;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.RetentionPolicy;
import com.walcore.spi.SegmentInfo;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>保留围栏（不可删除边界）的 TOCTOU 回归测试</b>（T14 ② 交付前闭项）。
 *
 * <h2>被钉住的缺陷</h2>
 * 保留策略的一轮是"<b>决策一次、逐个执行</b>"：轮首先采样
 * {@code minPinnedLsn}，然后拿这个值去过滤整批待删段。问题在于"执行"要花时间——
 * 逐个 unmap + 删文件在 Windows 上可达十几毫秒。如果读者恰好在这段时间里出现
 * （游标被创建 / 订阅者启动 / 快照建立），它登记的围栏对<b>本轮</b>完全无效，
 * 于是"我刚要读、而且已经声明在读"的段被删掉，续读直接跳号。
 *
 * <p>用探针（4KB 段、50ms 保留间隔、300 条记录、游标读 60 条后停顿 1.2s）
 * 实测的失败形态：
 * <pre>
 *   t0        round: pinned=0  →  决定删 [109,121,...,277]
 *   t0+4ms    cursor: pin 1（读者出现了，声明"从 1 开始都要"）
 *   t0+10ms   round 仍在逐个删文件 → 181..288 被删
 *   t0+1.2s   cursor 续读：pos=180 的下一条读到 289（181..288 静默消失）
 * </pre>
 *
 * <h2>修复</h2>
 * 每个段删除前<b>重新采样</b>围栏，并把围栏作为参数传进
 * {@link com.walcore.internal.segment.SegmentManager#deleteSegment(int, long, long)}，
 * 在段序列临界区内、紧邻真正摘除之前再复核一次。
 *
 * <h2>本用例为什么是确定性的</h2>
 * 不去赌线程调度，而是让策略在 {@code evaluate()}（= 轮首采样之后、删除之前）
 * 里代表"新出现的读者"登记围栏。修复前必然失败（整轮沿用 pinned=0，把围栏之后的段删光），
 * 修复后必然通过。
 */
@DisplayName("保留围栏：批处理中途抬高的围栏必须立刻生效")
class RetentionFenceRaceTest {

    /** 段容量（小段，让 60 条记录就能跨多段）。 */
    private static final long SEGMENT_SIZE = 4096L;

    /** 记录负载长度：帧长 320B，4KB 段约 12 条/段。 */
    private static final int PAYLOAD = 256;

    /** 写入条数：约 8 个段。 */
    private static final int RECORDS = 96;

    @Test
    @DisplayName("evaluate 期间出现的读者：围栏之后的段一个都不能删，且数据可连续读完")
    void fenceRegisteredDuringEvaluateStopsLaterDeletions(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            long last = writeAndFlush(wal, RECORDS);
            List<Segment> segments = wal.segmentManager().segments(0);
            assertTrue(segments.size() >= 5, "60 条 × 320B 应产生多个段，实际 " + segments.size());

            // 围栏点：第 3 段的首 LSN（它之前有 2 个完整的段可删）
            long fence = segments.get(3).firstLsn();
            assertTrue(fence > segments.get(0).firstLsn(), "围栏必须落在中间，实际 " + fence);

            AtomicReference<RetentionManager> holder = new AtomicReference<>();
            RetentionPolicy pinningPolicy = new DeleteAllButLastPolicy(
                    () -> holder.get().pin(fence));
            RetentionManager manager = newManager(wal, pinningPolicy);
            holder.set(manager);

            // 轮首采样时没有任何读者（pinned=0）；策略在 evaluate 里"代表新读者"抬高围栏
            assertEquals(0L, manager.minPinnedLsn(0), "轮首采样时不应有任何围栏");

            RetentionManager.RunResult result = manager.runPartition(0, false);

            assertTrue(result.deleted() > 0, "围栏之前的段必须被删掉（否则保留策略没生效）: " + result);
            List<Segment> survivors = wal.segmentManager().segments(0);
            for (Segment s : survivors) {
                assertTrue(s.lastLsn() >= fence,
                        "围栏（LSN " + fence + "）之后的段被删了：" + s.tag()
                                + " 区间 [" + s.firstLsn() + "," + s.lastLsn() + "]；"
                                + "存活段=" + survivors.stream().map(Segment::tag).toList());
            }

            // 数据侧证据：从围栏点起必须能一条不少地读到末尾
            int expected = (int) (last - fence + 1L);
            try (WalCursor cursor = wal.cursor(0,
                    CursorOptions.builder().fromLsn(fence).useMappedWindows(false).build())) {
                int count = 0;
                long prev = fence - 1L;
                WalRecord record;
                while ((record = cursor.next()) != null) {
                    assertEquals(prev + 1L, record.lsn(), "围栏之后的记录出现跳号");
                    prev = record.lsn();
                    count++;
                }
                assertEquals(expected, count,
                        "围栏（LSN " + fence + "）之后的记录必须一条不少，实际 " + count
                                + " 条（期望 " + expected + " 条）");
            }
        }
    }

    @Test
    @DisplayName("正在被游标读的段：即使 force（忽略围栏）也不能删，本段内续读不跳号")
    void forceNeverDeletesSegmentWithLiveReader(@TempDir Path dir) throws Exception {
        try (DefaultWal wal = TestWal.open(dir, SEGMENT_SIZE, b -> b.retentionEnabled(false))) {
            writeAndFlush(wal, RECORDS);
            RetentionManager manager = newManager(wal, new DeleteAllButLastPolicy(null));

            try (WalCursor cursor = wal.cursor(0,
                    CursorOptions.builder().fromLsn(1L).useMappedWindows(false).build())) {
                // 只读 20 条：确保游标停在某个段的<b>中间</b>（不是段尾），
                // 这样"force 之后还能继续读"这件事才是可判定的
                for (int i = 0; i < 20; i++) {
                    assertNotNull(cursor.next(), "前 20 条必须可读");
                }
                List<Segment> held = wal.segmentManager().segments(0).stream()
                        .filter(s -> s.readerCount() > 0)
                        .toList();
                assertEquals(1, held.size(), "游标只应持有一个段，实际 " + held.size());
                Segment reading = held.get(0);
                assertTrue(reading.lastLsn() > 20L,
                        "本用例需要游标停在该段中间，实际段尾=" + reading.lastLsn());

                // force = 无视 minPinnedLsn 围栏；但"仍有读者"是独立于围栏的第二道闸门，
                // 必须仍然生效——否则正在读的段会在读者眼皮底下被 unmap
                manager.runPartition(0, true);

                List<Segment> after = wal.segmentManager().segments(0);
                assertTrue(after.contains(reading),
                        "游标正在读的段被 force 删掉了：" + reading.tag()
                                + "；存活段=" + after.stream().map(Segment::tag).toList());
                assertEquals(1, reading.readerCount(), "该段的读者计数必须仍为 1");

                // 本段内续读必须一条不少（force 允许跳过"未被任何读者持有"的段，
                // 但绝不允许把正在读的段读成残缺）
                long prev = 20L;
                while (prev < reading.lastLsn()) {
                    WalRecord record = cursor.next();
                    assertNotNull(record, "本段内还有记录（到 " + reading.lastLsn() + "）却读不到了");
                    assertEquals(prev + 1L, record.lsn(), "force 之后本段内续读出现跳号");
                    prev = record.lsn();
                }
            }
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 写入 {@code count} 条固定长度记录并刷盘。
     *
     * @param wal 实例
     * @param count 条数
     * @return 最后一条 LSN
     */
    private static long writeAndFlush(DefaultWal wal, int count) {
        long last = 0L;
        for (int i = 0; i < count; i++) {
            last = wal.append(0, TestWal.payload(PAYLOAD), AppendOptions.DEFAULTS);
        }
        assertTrue(wal.flushAndWait(0, last, 30_000L), "必须可持久确认");
        return last;
    }

    /**
     * 手工装配一个保留管理器（不启动后台线程，由测试驱动 {@code runPartition}）。
     *
     * @param wal 实例
     * @param policy 策略
     * @return 管理器
     */
    private static RetentionManager newManager(DefaultWal wal, RetentionPolicy policy) {
        return new RetentionManager(wal.segmentManager(), wal.registry(), policy,
                wal.config(), null, null);
    }

    /**
     * "删掉除最后一段之外全部段"的策略，可在 {@code evaluate} 期间执行一个动作
     * （用来模拟"决策完成之后、删除之前出现的读者"）。
     */
    private static final class DeleteAllButLastPolicy implements RetentionPolicy {

        private final Runnable onEvaluate;

        DeleteAllButLastPolicy(Runnable onEvaluate) {
            this.onEvaluate = onEvaluate;
        }

        @Override
        public String id() {
            return "test:retention-fence-race";
        }

        @Override
        public int apiVersion() {
            return com.walcore.api.WalVersion.API_VERSION;
        }

        @Override
        public void init(PluginContext context) {
            // 无状态
        }

        @Override
        public RetentionDecision evaluate(RetentionContext context) {
            if (onEvaluate != null) {
                onEvaluate.run();
            }
            List<SegmentInfo> all = context.segments();
            if (all.size() <= 1) {
                return RetentionDecision.keepAll();
            }
            // 刻意<b>不</b>看 context.minPinnedLsn()：模拟一个"判断得不好"的第三方策略，
            // 让框架侧的安全检查（围栏 + 读者计数）成为唯一防线。
            return RetentionDecision.delete(all.subList(0, all.size() - 1), "fence-race");
        }

        @Override
        public void onSegmentDeleted(SegmentInfo info) {
            // 无需记录
        }

        @Override
        public void close() {
            // 无外部资源
        }
    }
}
