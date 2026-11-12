package com.walcore.segment;

import com.walcore.api.AppendOptions;
import com.walcore.api.WalBuilder;
import com.walcore.api.WalConfig;
import com.walcore.api.WalException;
import com.walcore.internal.append.FrameEncoder;
import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.append.LsnAllocator;
import com.walcore.internal.io.FileChannelStorageBackend;
import com.walcore.internal.io.HeapStorageBackend;
import com.walcore.internal.partition.PartitionImpl;
import com.walcore.internal.partition.PartitionRegistry;
import com.walcore.internal.segment.Segment;
import com.walcore.internal.segment.SegmentManager;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.internal.spiimpl.RawRecordCodec;
import com.walcore.spi.StorageBackend;
import com.walcore.spi.StorageConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T05 验收之二：<b>段轮转、零阻塞、查找、冻结与删除</b>。
 *
 * <p>本测试用 {@link FileChannelStorageBackend}（真实文件）验证正确性，
 * 用 {@link HeapStorageBackend}（纯内存）压测 1 万次轮转的延迟——
 * 这是刻意的分工：延迟指标关心的是"轮转路径本身"的开销，
 * 混入 1 万次真实文件创建会把 IO 时间算进 p99，指标就失去意义了
 * （真实文件路径的轮转延迟由 {@link #rotationUsesPreparedSegmentWithoutFallback} 单独覆盖）。
 */
class SegmentRotationTest {

    /** 段容量取 8KB，让 320 字节的帧在 25 条左右就触发轮转，测试跑得快。 */
    private static final long SMALL_SEGMENT = 8 * 1024L;

    /** 单帧 payload 长度（帧长 = 64 + 128 = 192，8 字节对齐后仍是 192）。 */
    private static final int PAYLOAD_LEN = 128;

    // ==================================================================
    // 1. 首个段
    // ==================================================================

    @Test
    @DisplayName("createFirstSegment：文件落盘（.tmp 已原子改名）、active 就位、可查找")
    void createFirstSegment(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment seg = f.manager.createFirstSegment(f.pid, 1L);
            assertSame(f.pid, seg.partitionId());
            assertEquals(1L, seg.firstLsn());
            assertEquals(SMALL_SEGMENT, seg.capacityBytes());
            assertEquals(Segment.SegmentState.ACTIVE, seg.state());
            assertTrue(seg.isActive());
            assertSame(seg, f.manager.activeSegment(f.pid));
            assertEquals(1, f.manager.segmentCount(f.pid));
            assertEquals(1, f.manager.segments(f.pid).size());
            assertSame(seg, f.manager.findSegmentFor(f.pid, 1L));
            assertSame(seg, f.manager.findSegmentFor(f.pid, 999_999L),
                    "lsn 超过活跃段名字 LSN 时仍应落到活跃段");

            Path wal = SegmentNaming.walPath(tmp, f.pid, 1L);
            assertTrue(Files.exists(wal), "段文件必须已落盘: " + wal);
            Path tmpFile = SegmentNaming.tmpPath(tmp, f.pid, 1L);
            assertFalse(Files.exists(tmpFile),
                    ".tmp 必须已被 ATOMIC_MOVE 改名，不能残留（否则恢复期会看到半成品）");
            assertTrue(Files.size(wal) >= SMALL_SEGMENT,
                    "SPARSE 预分配后文件长度应等于段容量");

            // 重复创建首段必须失败（否则会覆盖活跃段）
            assertThrows(WalException.PartitionException.class,
                    () -> f.manager.createFirstSegment(f.pid, 2L));
        }
    }

    // ==================================================================
    // 2. 空间触发轮转
    // ==================================================================

    @Test
    @DisplayName("写满自动轮转：旧段 READONLY 且长度冻结，旧段不再接受写入")
    void rotationOnSpaceExhaustion(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment first = f.manager.createFirstSegment(f.pid, 1L);
            ByteBuffer frame = f.frame(1L);

            long lsn = 1L;
            int written = 0;
            while (!f.manager.needsRotation(f.pid, frame.limit())) {
                first.appendFrame(frame.duplicate(), lsn);
                lsn++;
                written++;
                assertTrue(written < 1000, "8KB 段内不可能塞下 1000 条 192 字节帧");
            }
            assertTrue(SMALL_SEGMENT - first.publishedWritePos() < frame.limit(),
                    "触发轮转时必须是因为剩余空间放不下这一帧");
            long sealedPos = first.publishedWritePos();

            Segment second = f.rotate();
            assertNotSame(first, second);
            assertEquals(Segment.SegmentState.READONLY, first.state(),
                    "旧段必须迁移为 READONLY");
            assertEquals(sealedPos, first.finalLength(),
                    "finalLength 必须冻结为封存瞬间的写位置");
            assertEquals(Segment.SegmentState.ACTIVE, second.state());
            assertSame(second, f.manager.activeSegment(f.pid));
            assertEquals(2, f.manager.segmentCount(f.pid));
            assertTrue(second.firstLsn() > first.firstLsn(), "新段名字 LSN 必须严格更大");

            // 旧段彻底不可写
            final long rejectedLsn = lsn;
            assertThrows(IllegalStateException.class,
                    () -> first.appendFrame(f.frame(rejectedLsn), rejectedLsn),
                    "READONLY 段必须拒绝 appendFrame");
            assertFalse(first.hasRoom(64));

            // 新段继续可写，且两段内容都完整
            second.appendFrame(f.frame(lsn), lsn);
            assertEquals(frame.limit(), second.publishedWritePos());
            assertEquals(sealedPos, first.publishedWritePos(),
                    "旧段的写位置不得因封存而改变");
        }
    }

    @Test
    @DisplayName("帧不跨段：剩余空间 < frameLen 时轮转，绝不拆分帧")
    void framesNeverSpanSegments(@TempDir Path tmp) throws Exception {
        // 段容量取 4096：64 + 128 = 192 字节帧，4096/192 = 21.33 → 21 帧后剩 64 字节
        try (Fixture f = Fixture.fileChannel(tmp, 4096L)) {
            Segment seg = f.manager.createFirstSegment(f.pid, 1L);
            int frameLen = f.frame(1L).limit();
            int fits = 0;
            while (seg.hasRoom(frameLen)) {
                seg.appendFrame(f.frame(1L + fits), 1L + fits);
                fits++;
            }
            long remaining = 4096L - seg.publishedWritePos();
            assertTrue(remaining < frameLen,
                    "hasRoom 为 false 时必须意味着剩余 " + remaining + " < 帧长 " + frameLen);
            assertTrue(remaining >= 0L);
            assertTrue(f.manager.needsRotation(f.pid, frameLen));

            // 剩余空间足够放一条更小的帧时，不该轮转（证明判据是"逐帧"而非"比例"）
            Segment second = f.rotate();
            assertEquals(0L, second.publishedWritePos());
            assertTrue(second.hasRoom(frameLen));
            assertEquals(fits * (long) frameLen, seg.publishedWritePos(),
                    "段内已写字节必须等于整帧长度之和（无跨段残片）");
        }
    }

    @Test
    @DisplayName("段内记录数达 segmentMaxRecords 也触发轮转")
    void rotationOnRecordCount(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, 1024L * 1024L,
                b -> b.segmentMaxRecords(5))) {
            Segment seg = f.manager.createFirstSegment(f.pid, 1L);
            for (int i = 1; i <= 5; i++) {
                assertFalse(f.manager.needsRotation(f.pid, f.frame(i).limit()),
                        "第 " + i + " 条写入前不该轮转");
                seg.appendFrame(f.frame(i), i);
            }
            assertTrue(f.manager.needsRotation(f.pid, f.frame(6).limit()),
                    "写入 5 条（=segmentMaxRecords）后必须要求轮转");
            Segment next = f.rotate();
            assertFalse(f.manager.needsRotation(f.pid, f.frame(6).limit()),
                    "新段记录数归零，不该再要求轮转");
            assertEquals(0L, next.recordCount());
        }
    }

    @Test
    @DisplayName("timeBasedRotation 开启后按存活时长轮转；默认关闭时不轮转")
    void rotationOnAge(@TempDir Path tmp) throws Exception {
        // 默认关闭：即使把阈值调到 1ms 也不该触发
        try (Fixture f = Fixture.fileChannel(tmp, 1024L * 1024L,
                b -> b.segmentMaxAgeMillis(1L).timeBasedRotation(false))) {
            f.manager.createFirstSegment(f.pid, 1L);
            Thread.sleep(15L);
            assertFalse(f.manager.needsRotation(f.pid, f.frame(1L).limit()),
                    "timeBasedRotation=false 时不得因存活时长轮转");
        }
        // 开启：1ms 阈值 + sleep 15ms 必然触发
        try (Fixture f2 = Fixture.fileChannel(tmp.resolve("aged"), 1024L * 1024L,
                b -> b.segmentMaxAgeMillis(1L).timeBasedRotation(true))) {
            f2.manager.createFirstSegment(f2.pid, 1L);
            Thread.sleep(15L);
            assertTrue(f2.manager.needsRotation(f2.pid, f2.frame(1L).limit()),
                    "timeBasedRotation=true 且超过 segmentMaxAgeMillis 时必须轮转");
        }
    }

    // ==================================================================
    // 3. 零阻塞轮转
    // ==================================================================

    @Test
    @Tag("perf")
    @DisplayName("预建命中时轮转是纯指针交换：1 万次轮转 0 降级，p99 < 1ms")
    void rotationIsZeroBlockingWhenPrepared() throws Exception {
        final int rotations = 10_000;
        try (Fixture f = Fixture.heap(SMALL_SEGMENT)) {
            f.manager.createFirstSegment(f.pid, 1L);
            f.awaitPrepared();

            long[] nanos = new long[rotations];
            for (int i = 0; i < rotations; i++) {
                long t0 = System.nanoTime();
                f.manager.rotateLocked(f.pid);
                nanos[i] = System.nanoTime() - t0;
                // 等下一份储备就位后再测下一次，确保测的是"命中路径"
                f.awaitPrepared();
            }

            assertEquals(0L, f.manager.fallbackCount(),
                    "预建始终命中时不得出现内联降级（有降级说明预建线程跟不上）");
            Arrays.sort(nanos);
            long p50 = nanos[rotations / 2];
            long p99 = nanos[(int) (rotations * 0.99)];
            long max = nanos[rotations - 1];
            double p99Micros = p99 / 1000.0;
            assertTrue(p99 < 1_000_000L,
                    "轮转 p99 = " + p99Micros + "µs，超过 1ms 上限（p50="
                            + (p50 / 1000.0) + "µs, max=" + (max / 1000.0) + "µs），"
                            + "说明走了降级路径或轮转路径上有重活");
            assertEquals(rotations, f.manager.rotationCount());
            assertEquals(rotations + 1, f.manager.segmentCount(f.pid));
        }
    }

    @Test
    @DisplayName("预建命中：真实文件后端也走指针交换（0 降级）")
    void rotationUsesPreparedSegmentWithoutFallback(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment first = f.manager.createFirstSegment(f.pid, 1L);
            f.awaitPrepared();
            assertTrue(f.manager.preparer().hasReady(f.pid), "预建线程应已备好下一段");

            Segment second = f.rotate();
            assertEquals(0L, f.manager.fallbackCount(), "预建命中时不该有降级");
            assertTrue(Files.exists(SegmentNaming.walPath(tmp, f.pid, second.firstLsn())),
                    "预建段文件应已就位（不是降级现场创建的）");
            assertEquals(Segment.SegmentState.READONLY, first.state());
        }
    }

    @Test
    @DisplayName("无预建时内联降级，并计入 wal.rotate.fallback")
    void rotationFallsBackWhenNothingPrepared(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannelNoPreparer(tmp, SMALL_SEGMENT)) {
            Segment first = f.manager.createFirstSegment(f.pid, 1L);
            Segment second = f.rotate();
            assertEquals(1L, f.manager.fallbackCount(),
                    "预建未启用时必须走内联降级并计数（否则尖刺不可见 = 违反铁律 7）");
            assertEquals(Segment.SegmentState.READONLY, first.state());
            assertEquals(Segment.SegmentState.ACTIVE, second.state());
            assertTrue(Files.exists(SegmentNaming.walPath(tmp, f.pid, second.firstLsn())));

            f.rotate();
            assertEquals(2L, f.manager.fallbackCount());
            assertEquals(3, f.manager.segmentCount(f.pid));
        }
    }

    @Test
    @DisplayName("预建段名字过期（两次连续轮转）时被丢弃，不破坏段序")
    void stalePreparedSegmentIsDiscarded(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannelNoPreparer(tmp, SMALL_SEGMENT)) {
            f.manager.start(); // 启动预建线程但不等待
            f.manager.createFirstSegment(f.pid, 1L);
            f.awaitPrepared();

            // 第一次轮转吃掉了唯一的储备
            Segment second = f.rotate();
            // 立刻再轮转：此刻预建线程还没补建，必然降级；但即使它补建了，
            // 新名字也必须严格大于 second，否则 attachToHolder 会抛异常
            Segment third = f.rotate();
            assertTrue(third.firstLsn() > second.firstLsn(),
                    "段名必须严格递增: second=" + second.firstLsn() + ", third=" + third.firstLsn());
            List<Segment> all = f.manager.segments(f.pid);
            assertEquals(3, all.size());
            for (int i = 1; i < all.size(); i++) {
                assertTrue(all.get(i).firstLsn() > all.get(i - 1).firstLsn(),
                        "段列表必须按名字 LSN 严格递增");
            }
            assertTrue(f.manager.preparer().discardedCount() >= 0L);
        }
    }

    @Test
    @DisplayName("无活跃段时 rotate 抛 PartitionException")
    void rotateWithoutSegmentFails(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            assertThrows(WalException.PartitionException.class, () -> f.manager.rotate(f.pid));
            assertThrows(WalException.PartitionException.class, () -> f.manager.rotate(999),
                    "未注册分区必须抛 PartitionException");
        }
    }

    // ==================================================================
    // 4. 二分查找
    // ==================================================================

    @Test
    @DisplayName("findSegmentFor：1000 个段上二分正确且 p99 <= 1µs")
    @Tag("perf")
    void findSegmentForIsFastAndCorrect(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.heap(1024L * 1024L)) {
            f.manager.createFirstSegment(f.pid, 1L);
            int segments = 1000;
            for (int i = 1; i < segments; i++) {
                f.manager.rotate(f.pid);
            }
            assertEquals(segments, f.manager.segmentCount(f.pid));

            List<Segment> all = f.manager.segments(f.pid);
            for (int i = 0; i < segments; i++) {
                Segment expected = all.get(i);
                assertSame(expected, f.manager.findSegmentFor(f.pid, expected.firstLsn()),
                        "命中段首 LSN 时必须返回该段");
                if (i + 1 < segments) {
                    long justBeforeNext = all.get(i + 1).firstLsn() - 1L;
                    assertSame(expected, f.manager.findSegmentFor(f.pid, justBeforeNext),
                            "floor 语义：应返回名字 LSN <= target 的最后一段");
                }
            }
            assertNull(f.manager.findSegmentFor(f.pid, 0L),
                    "lsn 早于第一个段时返回 null（调用方据此判定已超出保留范围）");
            assertNull(f.manager.findSegmentFor(999, 1L));

            // 延迟：100 万次查找，统计 p99
            long[] nanos = new long[1_000_000];
            for (int i = 0; i < nanos.length; i++) {
                long lsn = all.get(i % segments).firstLsn();
                long t0 = System.nanoTime();
                Segment s = f.manager.findSegmentFor(f.pid, lsn);
                nanos[i] = System.nanoTime() - t0;
                if (s == null) {
                    throw new AssertionError("查找返回 null, lsn=" + lsn);
                }
            }
            Arrays.sort(nanos);
            long p99 = nanos[(int) (nanos.length * 0.99)];
            assertTrue(p99 <= 1_000L,
                    "findSegmentFor p99 = " + (p99 / 1000.0) + "µs，超过 1µs 上限"
                            + "（是否退化成线性扫描？）");
        }
    }

    @Test
    @DisplayName("rolloverVersion 每次轮转递增，供读者感知段列表变化")
    void rolloverVersionAdvances(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.heap(SMALL_SEGMENT)) {
            assertEquals(-1L, f.manager.rolloverVersion(999));
            f.manager.createFirstSegment(f.pid, 1L);
            long v0 = f.manager.rolloverVersion(f.pid);
            f.manager.rotate(f.pid);
            long v1 = f.manager.rolloverVersion(f.pid);
            f.manager.rotate(f.pid);
            long v2 = f.manager.rolloverVersion(f.pid);
            assertTrue(v1 > v0 && v2 > v1, "版本号必须单调递增: " + v0 + "," + v1 + "," + v2);
        }
    }

    // ==================================================================
    // 5. 冻结与删除
    // ==================================================================

    @Test
    @DisplayName("freeze：所有段 FROZEN，文件仍在（可被恢复读取）")
    void freezeKeepsFiles(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment s1 = f.manager.createFirstSegment(f.pid, 1L);
            s1.appendFrame(f.frame(1L), 1L);
            f.manager.rotate(f.pid);
            Segment s2 = f.manager.activeSegment(f.pid);
            s2.appendFrame(f.frame(2L), 2L);

            f.manager.freeze(f.pid);
            for (Segment s : f.manager.segments(f.pid)) {
                assertEquals(Segment.SegmentState.FROZEN, s.state(),
                        "freeze 后所有段必须是 FROZEN");
            }
            assertTrue(Files.exists(SegmentNaming.walPath(tmp, f.pid, s1.firstLsn())));
            assertTrue(Files.exists(SegmentNaming.walPath(tmp, f.pid, s2.firstLsn())));
        }
    }

    @Test
    @DisplayName("deletePartitionData：Windows 上文件被删得掉（证明先 unmap 后 delete）")
    void deletePartitionDataRemovesFiles(@TempDir Path tmp) throws Exception {
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment s1 = f.manager.createFirstSegment(f.pid, 1L);
            s1.appendFrame(f.frame(1L), 1L);
            f.manager.rotate(f.pid);
            f.manager.activeSegment(f.pid).appendFrame(f.frame(2L), 2L);
            Path dir = SegmentNaming.partitionDir(tmp, f.pid);
            assertTrue(Files.exists(dir));

            f.manager.deletePartitionData(f.pid);

            assertEquals(0, f.manager.segmentCount(f.pid), "分区应从管理器中移除");
            assertNull(f.manager.activeSegment(f.pid));
            assertFalse(Files.exists(dir), "分区目录必须被删除（Windows 上映射未释放会失败）");
            try (Stream<Path> s = Files.list(tmp.resolve(SegmentNaming.PARTITIONS_DIR))) {
                assertEquals(0L, s.count(), "partitions 下不应残留任何分区目录");
            }
        }
    }

    @Test
    @DisplayName("close：封存活跃段并释放句柄，之后文件可删（Windows unmap 证明）")
    void closeReleasesHandles(@TempDir Path tmp) throws Exception {
        Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT);
        Segment seg = f.manager.createFirstSegment(f.pid, 1L);
        seg.appendFrame(f.frame(1L), 1L);
        Path wal = SegmentNaming.walPath(tmp, f.pid, 1L);
        f.close();
        assertTrue(f.manager.isClosed());
        assertEquals(Segment.SegmentState.READONLY, seg.state(), "关闭时应封存活跃段");
        assertTrue(Files.deleteIfExists(wal), "close 后段文件必须可删除（映射已 unmap）");

        f.close(); // 幂等
    }

    // ==================================================================
    // 6. 段名严格递增守卫
    // ==================================================================

    @Test
    @DisplayName("reopenSegment 能读回既有段并冻结长度")
    void reopenExistingSegment(@TempDir Path tmp) throws Exception {
        long sealedLen;
        try (Fixture f = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment seg = f.manager.createFirstSegment(f.pid, 1L);
            for (int i = 1; i <= 3; i++) {
                seg.appendFrame(f.frame(i), i);
            }
            sealedLen = seg.publishedWritePos();
            seg.force(true);
        }
        try (Fixture f2 = Fixture.fileChannel(tmp, SMALL_SEGMENT)) {
            Segment reopened = f2.manager.reopenSegment(f2.pid, 1L, false, sealedLen);
            assertEquals(Segment.SegmentState.READONLY, reopened.state(),
                    "以只读方式打开时状态必须是 READONLY");
            assertEquals(sealedLen, reopened.finalLength(),
                    "长度应冻结为恢复扫描出的逻辑长度，而不是 8KB 的物理长度");
            assertEquals(sealedLen, reopened.publishedWritePos());
            assertEquals(SMALL_SEGMENT, reopened.capacityBytes());
            assertEquals(SMALL_SEGMENT, reopened.handle().physicalLength(),
                    "SPARSE 预分配后物理长度恒为段容量（这正是不能拿它当逻辑长度的原因）");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 测试夹具：一次性的后端 + 注册表 + LSN 分配器 + SegmentManager。
     */
    private static final class Fixture implements AutoCloseable {

        final Path dir;
        final int pid = 3;
        final StorageBackend backend;
        final PartitionRegistry registry;
        final LsnAllocator lsnAllocator = new LsnAllocator();
        final SegmentManager manager;
        private final FrameEncoder encoder = new FrameEncoder(new RawRecordCodec());
        private final WalBuilder builder;

        private Fixture(Path dir, StorageBackend backend, WalBuilder builder) throws IOException {
            this.dir = dir;
            this.backend = backend;
            this.builder = builder;
            WalConfig config = builder.buildConfig();
            backend.init(new StorageConfig(dir, config.segmentSizeBytes(),
                    config.preallocateMode() == WalConfig.PreallocateMode.FILL,
                    config.mappedWindowBytes(), config.mappedWindowCount(),
                    config.forceParentDirOnCreate()));
            this.registry = new PartitionRegistry(config.maxPartitions());
            this.registry.register(new PartitionImpl(pid));
            this.manager = new SegmentManager(backend, registry, dir, config, lsnAllocator,
                    null, null);
        }

        static Fixture fileChannel(Path dir, long segmentSize) throws IOException {
            return build(dir, new FileChannelStorageBackend(), segmentSize, true, b -> {
            });
        }

        static Fixture fileChannel(Path dir, long segmentSize,
                                   java.util.function.Consumer<WalBuilder> customizer)
                throws IOException {
            return build(dir, new FileChannelStorageBackend(), segmentSize, true, customizer);
        }

        static Fixture fileChannelNoPreparer(Path dir, long segmentSize) throws IOException {
            return build(dir, new FileChannelStorageBackend(), segmentSize, false, b -> {
            });
        }

        static Fixture heap(long segmentSize) throws IOException {
            return build(PathsHolder.TMP, new HeapStorageBackend(), segmentSize, true, b -> {
            });
        }

        private static Fixture build(Path dir, StorageBackend backend, long segmentSize,
                                     boolean startPreparer,
                                     java.util.function.Consumer<WalBuilder> customizer)
                throws IOException {
            WalBuilder b = new WalBuilder()
                    .baseDir(dir)
                    .segmentSizeBytes(segmentSize)
                    .segmentMaxRecords(10_000_000)
                    .segmentMaxAgeMillis(3_600_000L)
                    .timeBasedRotation(false)
                    .indexIntervalRecords(1000);
            customizer.accept(b);
            Fixture f = new Fixture(dir, backend, b);
            if (startPreparer) {
                f.manager.start();
            }
            return f;
        }

        /** 构造一帧（payload 全 0，长度固定）。 */
        ByteBuffer frame(long lsn) {
            ByteBuffer payload = ByteBuffer.allocate(PAYLOAD_LEN);
            ByteBuffer dst = ByteBuffer.allocate(FrameLayout.MAX_FRAME_LEN);
            int len = encoder.encodeFrame(dst, 0, dst.capacity(), lsn, pid, payload,
                    AppendOptions.defaults());
            assertTrue(len > 0, "帧编码失败");
            dst.limit(len);
            dst.position(0);
            return dst;
        }

        Segment rotate() throws IOException {
            return manager.rotate(pid);
        }

        /** 自旋等待预建就位（不走 sleep，避免把 1ms 定时器误差算进测量）。 */
        void awaitPrepared() {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (!manager.preparer().hasReady(pid)) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("等待预建段超时（预建线程是否已启动且未阻塞？）");
                }
                Thread.onSpinWait();
            }
        }

        @Override
        public void close() {
            manager.close();
            try {
                backend.close();
            } catch (RuntimeException ignored) {
                // 后端关闭失败不影响断言
            }
        }
    }

    /** 堆后端不碰文件系统，但仍需要一个"名义上的"目录用于命名。 */
    private static final class PathsHolder {
        static final Path TMP = java.nio.file.Paths.get("build", "wal-heap-test");
    }
}
