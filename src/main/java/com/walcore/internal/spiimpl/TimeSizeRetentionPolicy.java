package com.walcore.internal.spiimpl;

import com.walcore.api.WalConfig;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.Priority;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.RetentionPolicy;
import com.walcore.spi.SegmentInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 内置保留策略：<b>时间与大小双维度，取更严格的那一侧</b>（{@code builtin:retention-time-size}）。
 *
 * <h2>"取更严格"到底怎么算</h2>
 * 两个维度各自独立算出一批"该删"的段，然后取<b>并集</b>：
 * <ul>
 *   <li><b>时间</b>：{@code createdMillis < now - retentionMaxAgeMillis}；</li>
 *   <li><b>大小</b>：把所有段按创建时刻从旧到新累计，只要"当前总占用 &gt; 上限"就继续删最旧的，
 *       直到降到上限以下。</li>
 * </ul>
 * 并集而不是交集，是因为两者都是"上限"语义：任一超限都必须处理，否则另一维度的宽松
 * 会让整个保留策略失效（典型事故：7 天没到但磁盘已经写满）。
 *
 * <h2>三条硬保护（策略层自己先排除，管理器还会兜底再查一次）</h2>
 * <ol>
 *   <li><b>永远保留最后一个段</b>——否则"日志目录里一个段都没有"，下次开办要重建，
 *       且正在写的记录无处落地；</li>
 *   <li><b>绝不越过 {@code minPinnedLsn}</b>——有活跃 tailer/cursor/快照时必须留住它们需要的数据；</li>
 *   <li><b>只删连续的前缀</b>——保留策略的语义是"删最老的"，出现"中间挖洞"意味着
 *       段序与时间序矛盾，那属于更严重的状态错误，本策略不去猜。</li>
 * </ol>
 */
@Priority(0)
public final class TimeSizeRetentionPolicy implements RetentionPolicy {

    /** 内置 id（配置契约的一部分，不得修改）。 */
    public static final String ID = "builtin:retention-time-size";

    private volatile long maxAgeMillis = 7L * 24 * 3600 * 1000;
    private volatile long maxTotalBytes = 500L * 1024 * 1024 * 1024;
    private volatile WalConfig config;

    /**
     * 无参构造（满足 SPI 的实例化约定）。
     */
    public TimeSizeRetentionPolicy() {
        // 默认值写在字段初始化器里
    }

    /**
     * 直接指定阈值的构造（测试与手工装配用）。
     *
     * @param maxAgeMillis 保留时长（毫秒），{@code <= 0} 表示不按时间删
     * @param maxTotalBytes 总大小上限（字节），{@code <= 0} 表示不按大小删
     */
    public TimeSizeRetentionPolicy(long maxAgeMillis, long maxTotalBytes) {
        this.maxAgeMillis = maxAgeMillis;
        this.maxTotalBytes = maxTotalBytes;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return com.walcore.api.WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        if (context != null && context.config() != null) {
            WalConfig cfg = context.config();
            this.config = cfg;
            this.maxAgeMillis = cfg.retentionMaxAgeMillis();
            this.maxTotalBytes = cfg.retentionMaxTotalBytes();
        }
    }

    @Override
    public RetentionDecision evaluate(RetentionContext context) {
        if (context == null) {
            return RetentionDecision.keepAll();
        }
        List<SegmentInfo> segments = context.segments();
        if (segments.size() <= 1) {
            // 只剩一个段：无论超时还是超量都不删（保护 ①）
            return RetentionDecision.keepAll();
        }

        // 候选池：排除最后一个段（最活跃的）、排除越过 minPinnedLsn 的段
        List<SegmentInfo> deletable = new ArrayList<>(segments.size());
        SegmentInfo lastSegment = segments.get(segments.size() - 1);
        for (SegmentInfo info : segments) {
            if (info == lastSegment) {
                continue;
            }
            if (isPinned(info, context.minPinnedLsn())) {
                continue;
            }
            deletable.add(info);
        }
        if (deletable.isEmpty()) {
            return RetentionDecision.keepAll();
        }

        Set<SegmentInfo> selected = new LinkedHashSet<>();

        // 维度一：时间
        if (maxAgeMillis > 0L) {
            long cutoff = context.nowMillis() - maxAgeMillis;
            for (SegmentInfo info : deletable) {
                if (info.createdMillis() > 0L && info.createdMillis() < cutoff) {
                    selected.add(info);
                }
            }
        }

        // 维度二：大小（从旧到新累计）
        if (maxTotalBytes > 0L && context.totalBytes() > maxTotalBytes) {
            List<SegmentInfo> oldestFirst = new ArrayList<>(deletable);
            oldestFirst.sort(Comparator.comparingLong(SegmentInfo::createdMillis));
            long running = context.totalBytes();
            for (SegmentInfo info : oldestFirst) {
                if (running <= maxTotalBytes) {
                    break;
                }
                selected.add(info);
                running -= info.sizeBytes();
            }
        }

        if (selected.isEmpty()) {
            return RetentionDecision.keepAll();
        }

        // 只保留"连续前缀"（保护 ③）：从最旧的待删段起，要求它确实是 segments 里的连续一段。
        // 出现"中间挖洞"说明时间序与段序矛盾，本策略不去猜，直接放弃本轮。
        List<SegmentInfo> ordered = new ArrayList<>(selected);
        ordered.sort(Comparator.comparingLong(SegmentInfo::firstLsn));
        int startIdx = indexOfFirstLsn(segments, ordered.get(0).firstLsn());
        if (startIdx < 0) {
            return RetentionDecision.keepAll();
        }
        int limit = segments.size() - 1;   // 最后一个段永不删除
        List<SegmentInfo> prefix = new ArrayList<>(ordered.size());
        for (int i = 0; i < ordered.size(); i++) {
            int idx = startIdx + i;
            if (idx >= limit) {
                break;
            }
            SegmentInfo candidate = segments.get(idx);
            if (candidate.firstLsn() != ordered.get(i).firstLsn()) {
                break;
            }
            prefix.add(candidate);
        }
        if (prefix.isEmpty()) {
            return RetentionDecision.keepAll();
        }

        return RetentionDecision.delete(prefix, buildReason(prefix.size(), context));
    }

    /**
     * 找到指定 {@code firstLsn} 在段列表中的下标。
     *
     * @param segments 段列表（升序）
     * @param firstLsn 目标
     * @return 下标；不存在返回 -1
     */
    private static int indexOfFirstLsn(List<SegmentInfo> segments, long firstLsn) {
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).firstLsn() == firstLsn) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 该段是否被"不可删除边界"保护。
     *
     * @param info 段
     * @param minPinnedLsn 边界 LSN，{@code <= 0} 表示无保护
     * @return 受保护返回 true
     */
    private static boolean isPinned(SegmentInfo info, long minPinnedLsn) {
        if (minPinnedLsn <= 0L) {
            return false;
        }
        long segmentLast = info.lastLsn();
        // 空段（lastLsn == 0）不承载任何记录，删掉不影响任何读者
        return segmentLast >= minPinnedLsn;
    }

    /**
     * 构造稳定的原因字符串（会进指标标签，不宜含时间戳等易变内容）。
     *
     * @param count 待删段数
     * @param context 上下文
     * @return 原因
     */
    private String buildReason(int count, RetentionContext context) {
        StringBuilder sb = new StringBuilder(48);
        sb.append("time-or-size(").append(count).append(" segments");
        if (maxTotalBytes > 0L && context.totalBytes() > maxTotalBytes) {
            sb.append(", over-bytes");
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * 当前生效的最大保留时长（诊断/测试用）。
     *
     * @return 毫秒
     */
    public long maxAgeMillis() {
        return maxAgeMillis;
    }

    /**
     * 当前生效的总大小上限（诊断/测试用）。
     *
     * @return 字节
     */
    public long maxTotalBytes() {
        return maxTotalBytes;
    }

    @Override
    public void close() {
        // 无外部资源
    }

    @Override
    public String toString() {
        return "TimeSizeRetentionPolicy{" + ID
                + ", maxAgeMs=" + maxAgeMillis
                + ", maxTotalBytes=" + maxTotalBytes
                + (config == null ? "" : ", fromConfig") + '}';
    }
}
