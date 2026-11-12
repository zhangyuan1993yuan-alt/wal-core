package com.walcore.api;

/**
 * 健康度。经 {@link WalStats#health()} 暴露，是运维判断"要不要摘流量"的唯一依据。
 */
public enum Health {

    /** 一切正常。 */
    HEALTHY,

    /**
     * 降级：{@code force()} 失败（如 ENOSPC）、fsync 卡死、组提交线程异常重启。
     *
     * <p>此时<b>所有 {@code flushAndWait} 都不再推进 {@code durableLsn}</b>
     * （宁可不确认，也不假确认），后台每 5s 重试，成功则回到 {@link #HEALTHY}。
     */
    DEGRADED,

    /**
     * 失败：组提交线程连续异常重启超过上限（3 次），或段文件丢失等不可自愈故障。
     *
     * <p>此时只保留只读能力，必须人工介入。
     */
    FAILED
}
