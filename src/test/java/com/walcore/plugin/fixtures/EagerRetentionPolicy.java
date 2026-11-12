package com.walcore.plugin.fixtures;

import com.walcore.spi.PluginContext;
import com.walcore.spi.Priority;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.RetentionPolicy;
import com.walcore.spi.SegmentInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试夹具：一个"永远想删掉除最后一个之外全部段"的激进保留策略。
 *
 * <p>用途：验证 <b>{@code wal.spi.retention} 配置生效</b>（T14 ⑥）。
 * 它刻意与内置策略行为完全不同（内置策略在阈值未超时什么都不删），
 * 因此"策略有没有被换成我指定的那个"是可观测的，而不是靠日志猜。
 */
@Priority(50)
public final class EagerRetentionPolicy implements RetentionPolicy {

    /** 插件 id（配置里用它强制指定）。 */
    public static final String ID = "test:retention-eager";

    private final AtomicInteger evaluateCount = new AtomicInteger();
    private final AtomicInteger deletedCallbacks = new AtomicInteger();
    private volatile PluginContext context;

    /**
     * 无参构造（SPI 约定）。
     */
    public EagerRetentionPolicy() {
        // 无状态
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
    public void init(PluginContext pluginContext) {
        this.context = pluginContext;
    }

    @Override
    public RetentionDecision evaluate(RetentionContext retentionContext) {
        evaluateCount.incrementAndGet();
        List<SegmentInfo> segments = retentionContext.segments();
        if (segments.size() <= 1) {
            return RetentionDecision.keepAll();
        }
        List<SegmentInfo> toDelete = new ArrayList<>(segments.size() - 1);
        for (int i = 0; i < segments.size() - 1; i++) {
            SegmentInfo info = segments.get(i);
            if (retentionContext.minPinnedLsn() > 0L
                    && info.lastLsn() >= retentionContext.minPinnedLsn()) {
                continue;
            }
            toDelete.add(info);
        }
        return RetentionDecision.delete(toDelete, "eager");
    }

    @Override
    public void onSegmentDeleted(SegmentInfo info) {
        deletedCallbacks.incrementAndGet();
    }

    @Override
    public void close() {
        // 无外部资源
    }

    /**
     * {@link #evaluate} 被调用次数（测试断言"确实用的是我"）。
     *
     * @return 次数
     */
    public int evaluateCount() {
        return evaluateCount.get();
    }

    /**
     * {@link #onSegmentDeleted} 被调用次数。
     *
     * @return 次数
     */
    public int deletedCallbacks() {
        return deletedCallbacks.get();
    }

    /**
     * 注入的上下文（可为 null）。
     *
     * @return 上下文
     */
    public PluginContext context() {
        return context;
    }
}
