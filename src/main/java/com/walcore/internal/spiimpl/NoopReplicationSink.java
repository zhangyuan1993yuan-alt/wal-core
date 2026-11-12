package com.walcore.internal.spiimpl;

import com.walcore.api.WalConfig;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.ReplicationSink;
import com.walcore.spi.WalRecordView;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内置默认复制实现：<b>什么都不做</b>（{@code builtin:replication-noop}）。
 *
 * <h2>为什么默认必须存在一个"空"实现</h2>
 * 没有它，{@code wal.spi.replication} 这个扩展点在"没装插件"时就只能返回 {@code null}，
 * 于是所有调用点都要写 {@code if (sink != null)}，而 {@code replicatedLsn()} 之类
 * 的水位语义也要到处特判。给一个"永远返回 0、永远接收记录"的空实现，
 * 调用点就统一了，同时 {@code wal.stats().activePlugins()} 能如实显示
 * {@code replication -> builtin:replication-noop}，避免"看起来配了复制其实没配"。
 *
 * <p>它仍然记录 {@link #lastRecordLsn}，因此"复制链路是否真的被调用过"是可观测的
 * （诊断用，不作为正确性依赖）。
 */
public final class NoopReplicationSink implements ReplicationSink {

    /** 内置 id（配置契约的一部分，不得修改）。 */
    public static final String ID = "builtin:replication-noop";

    private final Map<Integer, Long> lastRecordLsn = new ConcurrentHashMap<>();
    private volatile WalConfig config;

    /**
     * 无参构造（满足 SPI 的实例化约定）。
     */
    public NoopReplicationSink() {
        // 字段默认值即正确
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
        this.config = context == null ? null : context.config();
    }

    @Override
    public Mode mode() {
        return Mode.ASYNC;
    }

    @Override
    public void onPartitionAdded(int partitionId, long firstLsn) {
        // 空实现：不复制
    }

    @Override
    public void onPartitionRemoved(int partitionId, long lastLsn) {
        lastRecordLsn.remove(partitionId);
    }

    @Override
    public void onRecord(WalRecordView record) {
        if (record != null) {
            lastRecordLsn.put(record.partitionId(), record.lsn());
        }
    }

    @Override
    public long replicatedLsn(int partitionId) {
        Long lsn = lastRecordLsn.get(partitionId);
        return lsn == null ? 0L : lsn;
    }

    @Override
    public void close() {
        lastRecordLsn.clear();
    }

    /**
     * 最近一次收到的配置（诊断用）。
     *
     * @return 配置，未 init 时为 null
     */
    public WalConfig config() {
        return config;
    }

    @Override
    public String toString() {
        return "NoopReplicationSink{" + ID + '}';
    }
}
