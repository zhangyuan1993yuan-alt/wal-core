package com.walcore.spi;

/**
 * 一条稀疏索引项：{@code lsn -> fileOffset}。
 *
 * <p>与磁盘上的 {@code .idx} 条目一一对应，每条严格 16 字节：
 * <pre>
 *   offset 0: lsn        int64 LE
 *   offset 8: fileOffset int64 LE
 * </pre>
 * 满足 PRD WAL-P0-07 ② "索引内存占用 ≤ 每条目 32 字节 × 记录数/1000"（实际 16 字节）。
 */
public final class IndexEntry {

    /** 该记录 LSN。 */
    public final long lsn;

    /** 该帧在 {@code .wal} 中的起始字节偏移。 */
    public final long fileOffset;

    /**
     * 构造索引项。
     *
     * @param lsn 记录 LSN
     * @param fileOffset 帧起始偏移
     */
    public IndexEntry(long lsn, long fileOffset) {
        this.lsn = lsn;
        this.fileOffset = fileOffset;
    }

    /**
     * 该记录 LSN（访问器形态，便于在不破坏 public final 风格的前提下使用）。
     *
     * @return LSN
     */
    public long lsn() {
        return lsn;
    }

    /**
     * 帧起始偏移。
     *
     * @return 偏移
     */
    public long fileOffset() {
        return fileOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexEntry other)) {
            return false;
        }
        return lsn == other.lsn && fileOffset == other.fileOffset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(lsn) * 31 + Long.hashCode(fileOffset);
    }

    @Override
    public String toString() {
        return "IndexEntry{lsn=" + lsn + ", fileOffset=" + fileOffset + '}';
    }
}
