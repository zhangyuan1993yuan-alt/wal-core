package com.walcore.spi;

/**
 * 索引查找结果：{@code <= 查询 LSN} 的最近索引项。
 *
 * <p>{@link #NONE} 表示"目标 LSN 小于段内首个索引项"，这不是错误：
 * 上层应从段首开始顺序扫描（最多扫 {@code indexIntervalRecords} 条就必然遇到下一个索引项）。
 */
public final class IndexHit {

    /** 未命中（目标 LSN 早于段内首个索引项）。 */
    public static final IndexHit NONE = new IndexHit(-1L, -1L, false);

    /** 命中的索引项 LSN（{@code <=} 查询值）。 */
    public final long lsn;

    /** 该索引项对应的帧起始偏移。 */
    public final long fileOffset;

    private final boolean present;

    private IndexHit(long lsn, long fileOffset, boolean present) {
        this.lsn = lsn;
        this.fileOffset = fileOffset;
        this.present = present;
    }

    /**
     * 构造一个命中结果。
     *
     * @param lsn 命中的索引项 LSN
     * @param fileOffset 帧起始偏移
     * @return 结果
     */
    public static IndexHit of(long lsn, long fileOffset) {
        return new IndexHit(lsn, fileOffset, true);
    }

    /**
     * 是否命中。
     *
     * @return 命中返回 true
     */
    public boolean isPresent() {
        return present;
    }

    /**
     * 命中的索引项 LSN。
     *
     * @return LSN
     */
    public long lsn() {
        return lsn;
    }

    /**
     * 命中的帧起始偏移。
     *
     * @return 偏移
     */
    public long fileOffset() {
        return fileOffset;
    }

    @Override
    public String toString() {
        return present ? "IndexHit{lsn=" + lsn + ", fileOffset=" + fileOffset + '}' : "IndexHit{NONE}";
    }
}
