package com.walcore.spi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * ② 存储介质 SPI 的<b>段句柄</b>：一个已打开（或已创建）段文件的读写视图。
 *
 * <h2>契约（三后端共用同一套，{@code SegmentHandleContractTest} 参数化验证）</h2>
 * <ul>
 *   <li>{@link #append(ByteBuffer)} 只在 {@link #appendPosition()} 处追加；追加完成后必须
 *       以 <b>release 语义</b>发布新的 {@code appendPosition}，使并发读者 acquire 到它之后
 *       能安全读到 {@code [0, appendPosition)} 里的完整帧。</li>
 *   <li>{@link #read(long, ByteBuffer, int)} 把 {@code len} 字节写入 {@code dst} 的
 *       <b>绝对区间</b> {@code [dst.position(), dst.position()+len)}，并把
 *       {@code dst.limit()} 设为 {@code dst.position()+len}；<b>不改变</b>
 *       {@code dst.position()}。返回实际读到的字节数（文件末尾不足时小于 {@code len}）。</li>
 *   <li>{@link #force(boolean)} 必须落到真正的 {@code fdatasync} / {@code fsync}。
 *       metaData=true 时同时刷元数据（仅创建与关闭时使用）。</li>
 *   <li>{@link #truncate(long)} 只在恢复期使用，且 <b>必须先 unmap</b>（Windows 上映射中的
 *       文件无法被 truncate）。</li>
 *   <li>{@link #close()} 必须幂等，且**必须 unmap**（同上）。</li>
 * </ul>
 */
public interface SegmentHandle extends AutoCloseable {

    /**
     * 段文件路径。
     *
     * @return 路径
     */
    Path path();

    /**
     * 该段首个记录所在的 LSN（命名里的 {@code firstLsn}）。
     *
     * @return LSN
     */
    long firstLsn();

    /**
     * 段容量（字节），等于创建时的 {@code segmentSizeBytes}。
     *
     * @return 字节数
     */
    long capacityBytes();

    /**
     * 已发布的可追加位置（字节）。
     *
     * <p>读取方用 acquire 语义读该值，之后只读 {@code [0, appendPosition())}，
     * 因此永远不会看到"半条记录"。
     *
     * @return 字节偏移
     */
    long appendPosition();

    /**
     * 追加一条完整帧。
     *
     * <p>{@code frame} 的 {@code [position, limit)} 就是整帧字节（{@code frameLen} 已包含在内）。
     * 实现必须在返回前把 {@code appendPosition} 前进 {@code frame.remaining()} 字节。
     * 多次追加后必须满足 {@code appendPosition == Σ frameLen}。
     *
     * @param frame 整帧
     * @throws IOException IO 失败
     */
    void append(ByteBuffer frame) throws IOException;

    /**
     * <b>打开既有段</b>时把"已写入边界"推进到 {@code position}（恢复期专用，ARCH 契约修订 T10）。
     *
     * <h2>为什么必须有这个方法</h2>
     * 可写句柄的初值恒定是 {@code 0}（新建段就该从 0 开始写），只读句柄的初值是文件长度。
     * 但恢复期需要"<b>以可写句柄打开既有段</b>"——这样 {@link #force(boolean)} 才能落在
     * 一个有写权限的 fd 上（Windows 上对只读 fd 调 {@code FlushFileBuffers} 会
     * {@code AccessDeniedException}）。此时句柄自报的 {@code appendPosition} 是 {@code 0}，
     * 与段内真实的逻辑长度（由扫描得出）不一致，于是：
     * <ul>
     *   <li>{@code Segment.publishedWritePos()} 显示 8176（上层发布过）；</li>
     *   <li>而 {@link #read(long, ByteBuffer, int)} 内部用 {@code appendPosition()} 当上界，
     *       算出的 {@code available = 0 - 0 = 0}，<b>任何读取都返回 0 字节</b>。</li>
     * </ul>
     * 这就是"水位看得见、数据读不出来"。所以逻辑长度必须显式推回句柄。
     *
     * <h2>调用约束</h2>
     * <ul>
     *   <li>只允许在"打开既有段"路径调用一次，<b>绝不可</b>出现在 append 热路径上；</li>
     *   <li>{@code position} 必须落在 {@code [appendPosition(), capacityBytes()]} 内：
     *       只允许把水位往前推，不允许后退（后退请用 {@link #truncate(long)}，它带真实 IO）；</li>
     *   <li>实现必须<b>以 release 语义</b>发布，使并发读者 acquire 后能读到
     *       {@code [0, position)} 内的完整字节。</li>
     * </ul>
     *
     * @param position 恢复扫描得出的逻辑长度（字节）
     * @throws IOException IO 失败
     * @throws UnsupportedOperationException 实现不支持（此时该后端无法用于崩溃恢复）
     */
    default void restoreAppendPosition(long position) throws IOException {
        throw new UnsupportedOperationException(
                getClass().getName() + " 未实现 restoreAppendPosition(long)，"
                        + "无法在崩溃恢复后以可写句柄打开既有段");
    }

    /**
     * 从 {@code fileOffset} 读取 {@code len} 字节到 {@code dst}。
     *
     * <p>写入位置见接口注释：{@code dst} 的绝对区间
     * {@code [dst.position(), dst.position()+len)}。
     *
     * @param fileOffset 文件内偏移
     * @param dst 目标缓冲区
     * @param len 期望字节数
     * @return 实际读取字节数
     * @throws IOException IO 失败
     */
    int read(long fileOffset, ByteBuffer dst, int len) throws IOException;

    /**
     * 刷盘。
     *
     * @param metaData 是否同时刷元数据
     * @throws IOException IO 失败
     */
    void force(boolean metaData) throws IOException;

    /**
     * 截断到指定长度（恢复期使用）。
     *
     * <p>实现约定：截断后 {@code appendPosition} 被重设为 {@code newLength}。
     *
     * @param newLength 新长度（字节）
     * @throws IOException IO 失败
     */
    void truncate(long newLength) throws IOException;

    /**
     * 文件的实际物理长度（可能与 {@code appendPosition} 不同，恢复期比对用）。
     *
     * @return 字节数
     * @throws IOException IO 失败
     */
    long physicalLength() throws IOException;

    /**
     * 是否通过 mmap 方式映射。
     *
     * @return mmap 返回 true
     */
    boolean isMapped();

    /**
     * 关闭句柄并释放映射。必须幂等。
     *
     * @throws IOException IO 失败
     */
    @Override
    void close() throws IOException;
}
