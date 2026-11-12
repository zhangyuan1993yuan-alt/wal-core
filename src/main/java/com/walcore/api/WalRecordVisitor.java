package com.walcore.api;

/**
 * 批量读回调，用于 {@link WalCursor#nextBatch(int, WalRecordVisitor)}。
 *
 * <p><b>为什么需要它</b>：逐条 {@code next()} 在每秒百万条的读场景下会产生大量
 * {@link WalRecord} 对象。批量接口把这批记录的"外壳"复用同一个实例，回调里只能读取
 * 字段、<b>不得持有</b>该引用——下一次回调时它的内容会被覆盖（ARCH §8.1）。
 * 需要长期持有请调用 {@link WalRecord#copy()}。
 *
 * <pre>{@code
 * long[] sink = new long[1000];
 * int n = cursor.nextBatch(1000, r -> { sink[count++] = r.lsn(); return true; });
 * }</pre>
 */
@FunctionalInterface
public interface WalRecordVisitor {

    /**
     * 接收一条记录。
     *
     * @param record 记录视图（<b>复用的实例</b>，回调返回后内容即可被下一批覆盖）
     * @return {@code true} 继续读；{@code false} 提前结束本批（{@code nextBatch} 立即返回）
     */
    boolean accept(WalRecord record);
}
