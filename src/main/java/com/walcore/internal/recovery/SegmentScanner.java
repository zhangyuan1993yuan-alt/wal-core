package com.walcore.internal.recovery;

import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.spi.SegmentHandle;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 段顺序扫描器（ARCH §9.1 的 {@code SegmentScanner 从 offset=0 顺序扫描}）。
 *
 * <h2>为什么不用 {@code FrameDecoder}</h2>
 * 解码器要做 codec 分发、payload 还原、前向兼容跳过——这些是<b>读业务数据</b>需要的，
 * 恢复期只需要"帧边界 + CRC + 三个头字段"。这里刻意只依赖 {@link FrameLayout} 的偏移
 * 常量做绝对下标读取，因此：
 * <ul>
 *   <li>不分配 {@code DecodedFrame}，不碰 codec；</li>
 *   <li>插件坏了（比如 codec {@code init} 失败）也不影响恢复——恢复必须比业务更"皮实"；</li>
 *   <li>扫描速率只受 IO 与 CRC 限制。</li>
 * </ul>
 *
 * <h2>三个可选行为</h2>
 * <ul>
 *   <li><b>校验 CRC</b>（{@code verifyCrc}）：关掉后只做帧边界校验，用于
 *       {@code cleanShutdown=true} 的快速重启；</li>
 *   <li><b>重同步</b>（{@code resyncWindowBytes > 0}）：命中损坏后在该窗口内逐字节
 *       寻找下一个"magic+len+CRC 都合法"的位置，对应 {@code SKIP_RECORD} 策略；</li>
 *   <li><b>帧访问者</b>：每帧回调一次，返回 {@code false} 可提前结束扫描。</li>
 * </ul>
 */
public final class SegmentScanner {

    /** 默认读取缓冲（帧头 + 常见帧一次读完）。 */
    private static final int READ_CHUNK = 64 * 1024;

    private final TornWriteDetector detector = new TornWriteDetector();

    /**
     * 一帧的头信息（不含 payload，恢复期不需要）。
     */
    public static final class FrameInfo {
        /** 帧在文件中的起始偏移。 */
        public long offset;
        /** 帧长。 */
        public int frameLen;
        /** 全局 LSN。 */
        public long lsn;
        /** 分区 ID。 */
        public int partitionId;
        /** 记录类型（无符号）。 */
        public int recordType;
        /** 编码器 id。 */
        public int codecId;
        /** 写入时刻（epoch 纳秒）。 */
        public long timestampNanos;
        /** 标志位。 */
        public int flags;
        /** 逻辑长度。 */
        public int payloadLen;
        /** 存储长度。 */
        public int storedLen;
    }

    /**
     * 帧访问者。
     */
    @FunctionalInterface
    public interface FrameVisitor {

        /**
         * 访问一帧。
         *
         * @param info 帧信息（复用实例，回调返回后可能被下次覆盖）
         * @return {@code false} 提前结束扫描
         */
        boolean onFrame(FrameInfo info);
    }

    /**
     * 损坏回调（{@code SKIP_RECORD} 重同步时每次跳过都会回调）。
     */
    @FunctionalInterface
    public interface CorruptionSink {

        /**
         * 报告一处跳过。
         *
         * @param atOffset 发现损坏的偏移
         * @param skippedBytes 为跳过而丢弃的字节数
         * @param rule 命中的规则标识
         */
        void onSkip(long atOffset, long skippedBytes, String rule);
    }

    /**
     * 扫描结果。
     */
    public static final class ScanResult {
        /** 最后一条完整记录之后的偏移（恢复后的 {@code appendPosition}）。 */
        public long lastGoodOffset;
        /** 最后一条完整记录的 LSN，无记录为 0。 */
        public long lastLsn;
        /** 完整帧数。 */
        public long frameCount;
        /** 是否因为损坏/不完整而提前停止。 */
        public boolean stopped;
        /**
         * 是否因为"数据正常结束"而停止（预分配未使用尾部）。
         *
         * <p>与 {@link #stopped} 互斥：{@code dataEnd=true} 表示没有损坏，
         * 只是后面全零。
         */
        public boolean dataEnd;
        /** 停止位置（{@code stopped} 时有效）。 */
        public long stopOffset;
        /** 停止原因（命中的规则标识）。 */
        public String stopRule;
        /** 停止原因的类型。 */
        public TornWriteDetector.Kind stopKind;
        /** 重同步跳过的总字节数。 */
        public long skippedBytes;
        /** 重同步次数。 */
        public long resyncCount;

        /**
         * 是否发现了损坏（含被重同步跳过的）。
         *
         * @return 有损坏返回 true
         */
        public boolean hasCorruption() {
            return stopped || resyncCount > 0;
        }

        @Override
        public String toString() {
            return "ScanResult{lastGoodOffset=" + lastGoodOffset
                    + ", lastLsn=" + lastLsn
                    + ", frames=" + frameCount
                    + (stopped ? ", stopped@" + stopOffset + " (" + stopRule + ')' : "")
                    + (resyncCount > 0 ? ", resync=" + resyncCount + " skipped=" + skippedBytes : "")
                    + '}';
        }
    }

    /**
     * 扫描一个段的 {@code [fromOffset, fileLength)} 区间。
     *
     * @param handle 段句柄（调用方负责 open/close）
     * @param fromOffset 起始偏移
     * @param fileLength 文件当前长度
     * @param verifyCrc 是否逐帧校验 CRC32C
     * @param resyncWindowBytes 重同步窗口（0 表示不重同步，遇到损坏即停止）
     * @param visitor 帧访问者，可为 null
     * @param corruptionSink 跳过回调，可为 null
     * @return 扫描结果
     * @throws IOException 读取失败
     */
    public ScanResult scan(SegmentHandle handle, long fromOffset, long fileLength,
                           boolean verifyCrc, int resyncWindowBytes,
                           FrameVisitor visitor, CorruptionSink corruptionSink) throws IOException {
        ScanResult result = new ScanResult();
        result.lastGoodOffset = fromOffset;
        ByteBuffer buffer = ByteBuffer.allocate(READ_CHUNK);
        ByteBuffer header = ByteBuffer.allocate(FrameLayout.HEADER_LEN);
        FrameInfo info = new FrameInfo();

        long offset = fromOffset;
        while (offset < fileLength) {
            long remaining = fileLength - offset;
            if (remaining < FrameLayout.HEADER_LEN) {
                if (isAllZero(handle, offset, remaining)) {
                    // 预分配尾部的最后几十字节：数据正常结束
                    result.dataEnd = true;
                    result.stopOffset = offset;
                    result.stopRule = TornWriteDetector.SUMMARY_ZERO_TAIL;
                    result.stopKind = TornWriteDetector.Kind.TORN;
                    break;
                }
                result.stopped = true;
                result.stopOffset = offset;
                result.stopKind = TornWriteDetector.Kind.TORN;
                result.stopRule = "rule1.shortHeader";
                break;
            }
            readFully(handle, offset, header, FrameLayout.HEADER_LEN);
            TornWriteDetector.Verdict verdict = detector.inspect(header, offset, fileLength);
            if (!verdict.isOk()) {
                if (TornWriteDetector.SUMMARY_ZERO_TAIL.equals(verdict.rule)) {
                    // 预分配未使用尾部：数据到此正常结束，不算损坏、不重同步
                    result.dataEnd = true;
                    result.stopOffset = offset;
                    result.stopRule = verdict.rule;
                    result.stopKind = TornWriteDetector.Kind.TORN;
                    break;
                }
                long resyncAt = resyncWindowBytes > 0
                        ? findResync(handle, offset + 1L, fileLength, resyncWindowBytes, verifyCrc)
                        : -1L;
                if (resyncAt < 0L) {
                    result.stopped = true;
                    result.stopOffset = offset;
                    result.stopKind = verdict.kind;
                    result.stopRule = verdict.rule;
                    break;
                }
                long skipped = resyncAt - offset;
                result.skippedBytes += skipped;
                result.resyncCount++;
                if (corruptionSink != null) {
                    corruptionSink.onSkip(offset, skipped, verdict.rule);
                }
                offset = resyncAt;
                continue;
            }

            int frameLen = verdict.frameLen;
            if (buffer.capacity() < frameLen) {
                buffer = ByteBuffer.allocate(frameLen);
            }
            readFully(handle, offset, buffer, frameLen);
            if (verifyCrc) {
                TornWriteDetector.Verdict crc = detector.verifyCrc(buffer, frameLen);
                if (!crc.isOk()) {
                    long resyncAt = resyncWindowBytes > 0
                            ? findResync(handle, offset + 1L, fileLength, resyncWindowBytes, true)
                            : -1L;
                    if (resyncAt < 0L) {
                        result.stopped = true;
                        result.stopOffset = offset;
                        result.stopKind = TornWriteDetector.Kind.CORRUPT;
                        result.stopRule = crc.rule;
                        break;
                    }
                    long skipped = resyncAt - offset;
                    result.skippedBytes += skipped;
                    result.resyncCount++;
                    if (corruptionSink != null) {
                        corruptionSink.onSkip(offset, skipped, crc.rule);
                    }
                    offset = resyncAt;
                    continue;
                }
            }

            fillInfo(info, buffer, offset, frameLen);
            result.frameCount++;
            result.lastLsn = info.lsn;
            offset += frameLen;
            result.lastGoodOffset = offset;
            if (visitor != null && !visitor.onFrame(info)) {
                break;
            }
        }
        if (offset >= fileLength) {
            result.lastGoodOffset = fileLength;
        }
        return result;
    }

    /**
     * 在 {@code [from, min(from+window, fileLength))} 内逐字节寻找下一个可解析的帧起点。
     *
     * @return 找到的位置；找不到返回 -1
     * @throws IOException 读取失败
     */
    private long findResync(SegmentHandle handle, long from, long fileLength, int window,
                            boolean verifyCrc) throws IOException {
        long end = Math.min(from + window, fileLength - FrameLayout.HEADER_LEN);
        ByteBuffer header = ByteBuffer.allocate(FrameLayout.HEADER_LEN);
        ByteBuffer frame = ByteBuffer.allocate(READ_CHUNK);
        for (long candidate = from; candidate <= end; candidate++) {
            readFully(handle, candidate, header, FrameLayout.HEADER_LEN);
            TornWriteDetector.Verdict verdict = detector.inspect(header, candidate, fileLength);
            if (!verdict.isOk()) {
                continue;
            }
            if (!verifyCrc) {
                return candidate;
            }
            int frameLen = verdict.frameLen;
            if (frame.capacity() < frameLen) {
                frame = ByteBuffer.allocate(frameLen);
            }
            readFully(handle, candidate, frame, frameLen);
            if (detector.verifyCrc(frame, frameLen).isOk()) {
                return candidate;
            }
        }
        return -1L;
    }

    /**
     * 判断 {@code [offset, offset+len)} 是否全为 0（用于区分"预分配尾部"与"真撕裂"）。
     */
    private static boolean isAllZero(SegmentHandle handle, long offset, long len)
            throws IOException {
        if (len <= 0L) {
            return true;
        }
        ByteBuffer probe = ByteBuffer.allocate((int) Math.min(len, FrameLayout.HEADER_LEN));
        probe.clear();
        probe.limit(probe.capacity());
        int read = handle.read(offset, probe, probe.capacity());
        if (read <= 0) {
            return true;
        }
        for (int i = 0; i < read; i++) {
            if (probe.get(i) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 把 {@code len} 字节读到 {@code dst} 的下标 0 起（不依赖 dst 的 position）。
     */
    private static void readFully(SegmentHandle handle, long offset, ByteBuffer dst, int len)
            throws IOException {
        dst.clear();
        dst.limit(len);
        int read = handle.read(offset, dst, len);
        if (read < len) {
            throw new IOException("读取不足: offset=" + offset + ", 需要 " + len
                    + " 字节，实际 " + read + " 字节（段 " + handle.path() + "）");
        }
        // 保证绝对下标读取与 CRC 计算的 limit 前置条件成立
        dst.limit(dst.capacity());
    }

    /**
     * 从帧缓冲区头里取出恢复所需字段（绝对下标读取，零分配）。
     */
    private static void fillInfo(FrameInfo info, ByteBuffer frame, long offset, int frameLen) {
        info.offset = offset;
        info.frameLen = frameLen;
        info.lsn = frame.getLong(FrameLayout.OFFSET_LSN);
        info.timestampNanos = frame.getLong(FrameLayout.OFFSET_TIMESTAMP_NANOS);
        info.partitionId = frame.getInt(FrameLayout.OFFSET_PARTITION_ID);
        info.recordType = MemoryUtils.getUnsignedByte(frame, FrameLayout.OFFSET_RECORD_TYPE);
        info.flags = MemoryUtils.getUnsignedByte(frame, FrameLayout.OFFSET_FLAGS);
        info.codecId = frame.getInt(FrameLayout.OFFSET_CODEC_ID);
        info.payloadLen = frame.getInt(FrameLayout.OFFSET_PAYLOAD_LEN);
        info.storedLen = frame.getInt(FrameLayout.OFFSET_STORED_LEN);
    }
}
