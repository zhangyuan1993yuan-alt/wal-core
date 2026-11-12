package com.walcore.spi;

import java.io.IOException;
import java.nio.file.Path;

/**
 * ② 存储介质 SPI：把"创建/打开/删除段"与"读写/force/truncate"抽象出来，
 * 让上层（{@code SegmentManager}）完全不感知 mmap / FileChannel / 堆内的差异。
 *
 * <h2>为什么需要三种后端</h2>
 * <table border="1">
 *   <caption>三后端定位</caption>
 *   <tr><th>后端</th><th>插件 id</th><th>定位</th></tr>
 *   <tr><td>{@code MappedStorageBackend}</td><td>{@code builtin:storage-mapped}</td>
 *       <td>Linux x86_64 默认。mmap 直写，零系统调用，p50 ~0.5µs</td></tr>
 *   <tr><td>{@code FileChannelStorageBackend}</td><td>{@code builtin:storage-fc}</td>
 *       <td>Windows 默认兜底。可移植性最好，每次 append 一次 write 系统调用</td></tr>
 *   <tr><td>{@code HeapStorageBackend}</td><td>{@code builtin:storage-heap}</td>
 *       <td>纯内存，无文件系统副作用，用于测试与嵌入式小规模场景</td></tr>
 * </table>
 *
 * <p>三者必须通过<b>同一套</b>契约测试——这是 Windows 上"正确性有保证"的唯一手段
 * （性能不承诺，见 ARCH §16.4）。
 */
public interface StorageBackend extends WalPlugin {

    /**
     * 是否支持"零拷贝追加"（mmap 直写）。
     *
     * <p>返回 true 时 {@code AppendPipeline} 可以跳过 staging buffer 直接写段；
     * 返回 false 时必须经 {@code StagingBuffer} 攒批后一次 {@code write}。
     *
     * @return 支持返回 true
     */
    boolean supportsZeroCopyAppend();

    /**
     * 初始化后端（创建根目录、校验文件系统能力）。
     *
     * <p>与 {@link WalPlugin#init(PluginContext)} 分开：前者注入框架能力，
     * 本方法注入存储参数（目录、段大小、窗口配置）。
     *
     * @param config 存储配置
     * @throws IOException 目录无法创建等
     */
    void init(StorageConfig config) throws IOException;

    /**
     * 创建一个新段文件，容量固定为 {@code segmentSizeBytes}。
     *
     * <p>实现约定：文件创建后立即 {@code setLength(segmentSizeBytes)}（SPARSE 预分配），
     * 因此 mmap 长度恒定、永不 remap。创建后应对父目录 {@code force(true)}
     * （{@code forceParentDirOnCreate=true} 且平台支持时）。
     *
     * @param partitionId 分区 ID
     * @param firstLsn 段内首条记录的 LSN（用于命名）
     * @param segmentSizeBytes 段容量
     * @return 可写句柄
     * @throws IOException IO 失败
     */
    SegmentHandle create(int partitionId, long firstLsn, long segmentSizeBytes) throws IOException;

    /**
     * 打开既有段文件。
     *
     * @param path 段路径
     * @param firstLsn 段内首条记录的 LSN（从文件名解析）
     * @param writable true 仅用于活跃段；false 用于只读段
     * @return 句柄
     * @throws IOException IO 失败
     */
    SegmentHandle open(Path path, long firstLsn, boolean writable) throws IOException;

    /**
     * 删除段文件（调用方保证所有 handle 已 close）。
     *
     * @param path 段路径
     * @throws IOException IO 失败
     */
    void delete(Path path) throws IOException;

    /**
     * 判断路径是否属于本后端可识别的段文件（按扩展名与命名规则）。
     *
     * @param path 路径
     * @return 可识别返回 true
     */
    boolean accepts(Path path);
}
