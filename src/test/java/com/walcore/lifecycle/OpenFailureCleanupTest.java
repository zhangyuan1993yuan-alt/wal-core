package com.walcore.lifecycle;

import com.walcore.api.WalException;
import com.walcore.internal.DefaultWal;
import com.walcore.internal.segment.SegmentNaming;
import com.walcore.support.TestWal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>F5 回归：open 中途失败必须把已启动的组件全部关掉。</b>
 *
 * <h2>被回归的缺陷</h2>
 * {@code DefaultWal.open} 的失败清理链只关 coordinator / segmentManager /
 * windowCache / backend / pluginManager。但开办顺序里 {@code RetentionManager}
 * （线程名前缀 {@code wal-retention}）与 {@code AppendPipeline}（持 staging 区）
 * 在最后的 {@code writeMeta} <b>之前</b>就已创建/启动——writeMeta 失败时它们
 * 不会被关闭，留下：
 * <ul>
 *   <li>泄漏的 wal-retention 线程（还持有段引用，目录删不掉）；</li>
 *   <li>未 unmap 的 staging 映射（Windows 上目录被占）。</li>
 * </ul>
 *
 * <h2>注入方式</h2>
 * 把 {@code wal.meta.tmp} 预先建成一个<b>目录</b>：{@code MetaFile.write}
 * 先写临时文件再原子替换，对目录路径打开输出流必然抛 IOException——
 * 开办流程其余部分全部正常，只在最后一步失败，正好覆盖"最晚失败点"。
 */
class OpenFailureCleanupTest {

    @Test
    @DisplayName("F5：writeMeta 失败 → 无残留 wal-retention 线程，LOCK 已释放")
    void openFailureCleansUpRetentionAndPipeline(@TempDir Path dir) throws Exception {
        List<String> retentionThreadsBefore = retentionThreads();
        // 预置临时文件路径为目录：writeMeta 必失败（对目录 open 写通道抛 IOException）
        Path tmp = dir.resolve(SegmentNaming.META_FILE + SegmentNaming.TMP_EXT);
        Files.createDirectories(tmp);
        assertTrue(Files.isDirectory(tmp), "注入失败的前置条件");

        assertThrows(WalException.class,
                () -> TestWal.open(dir, 64L * 1024, b -> b.retentionEnabled(true)),
                "writeMeta 失败必须以异常冒泡（开办失败的语义不变）");

        // ---- 核心：不得留下 wal-retention 线程 ----
        List<String> retentionThreadsAfter = retentionThreads();
        List<String> leaked = retentionThreadsAfter.stream()
                .filter(name -> !retentionThreadsBefore.contains(name))
                .collect(Collectors.toList());
        // 容错：并发跑的其他测试可能恰在此时启停同名线程；只断言"没有新出现的活跃线程"
        // 且总数没有净增长。
        assertTrue(leaked.isEmpty() || retentionThreadsAfter.size() <= retentionThreadsBefore.size(),
                "open 失败后不得留下 wal-retention 线程。开启前=" + retentionThreadsBefore
                        + "，开启后=" + retentionThreadsAfter + "，新增=" + leaked);
        assertFalse(retentionThreadsAfter.size() > retentionThreadsBefore.size() + 1,
                "wal-retention 线程数不得净增长: before=" + retentionThreadsBefore.size()
                        + ", after=" + retentionThreadsAfter.size());

        // ---- LOCK 必须已释放：删掉注入物后能重新 open（同一 JVM 内 FileLock 不可重入）----
        // Windows 上目录被映射占用时 delete 会失败——F5 修的就是这个：
        // 段与 staging 已被失败清理链关闭，目录可以删掉。
        Files.delete(tmp);
        try (DefaultWal wal = TestWal.open(dir, 64L * 1024, b -> {
        })) {
            long lsn = TestWal.append(wal, 0, 32);
            assertTrue(lsn > 0L, "失败清理后必须能重新开办并写入");
        }
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 当前 JVM 里名字以 wal-retention 开头的存活线程。
     *
     * @return 线程名列表
     */
    private static List<String> retentionThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .filter(name -> name.startsWith("wal-retention"))
                .collect(Collectors.toList());
    }
}
