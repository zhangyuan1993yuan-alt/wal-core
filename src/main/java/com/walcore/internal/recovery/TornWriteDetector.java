package com.walcore.internal.recovery;

import com.walcore.internal.append.FrameLayout;
import com.walcore.internal.io.MemoryUtils;
import com.walcore.internal.util.Crc32cUtil;

import java.nio.ByteBuffer;

/**
 * 撕裂写（torn write）判定器 —— ARCH §9.2 的五条规则。
 *
 * <p>按顺序执行，<b>任一命中即判定"从该 offset 起不可用"</b>：
 *
 * <table border="1">
 *   <caption>五条规则</caption>
 *   <tr><th>#</th><th>规则</th><th>捕获的物理形态</th></tr>
 *   <tr><td>①</td><td>{@code fileLength - offset < 64}</td><td>帧头都没写完（断电最典型形态）</td></tr>
 *   <tr><td>②</td><td>{@code magic != WAL1}</td><td>帧头被零填充 / 垃圾覆盖</td></tr>
 *   <tr><td>③</td><td>{@code frameLen < 72 || > 16777288 || &7 != 0}</td><td>帧头半写导致长度是垃圾值</td></tr>
 *   <tr><td>④</td><td>{@code offset + frameLen > fileLength}</td><td>帧头完整但 payload 没写完</td></tr>
 *   <tr><td>⑤</td><td>{@code crc32c != 帧内 crc}</td><td>数据位翻转（介质损坏 / 坏块）</td></tr>
 * </table>
 *
 * <h2>为什么"截断到最近一条完整记录"是安全的</h2>
 * {@code durableLsn} 的语义保证"已 ACK 的记录必已完整落盘"。撕裂写只会发生在
 * <b>尚未 ACK</b> 的尾部记录上——截断它们等价于"那笔请求从未被确认"，
 * 与上层事务语义一致。这也是为什么规则 ⑤ 在<b>非最后段</b>命中时必须报错而不是截断：
 * 中间段损坏意味着历史已确认数据丢失，静默截断会造成 LSN 空洞。
 */
public final class TornWriteDetector {

    /**
     * "预分配未使用尾部"的规则标识。
     *
     * <p>调用方（{@code SegmentScanner} / {@code RecoveryManager}）据此把
     * "数据正常结束" 与 "真的坏了" 区分开：命中本标识时既不截断文件、
     * 也不算损坏、也不触发重同步。
     */
    public static final String SUMMARY_ZERO_TAIL = "rule2.zeroTail";

    /** 判定结果种类。 */
    public enum Kind {
        /** 帧完整且 CRC 正确。 */
        OK,
        /** 不完整（尾部被截断/未写完）—— 可以安全截断。 */
        TORN,
        /** 内容损坏（magic/长度/CRC 不对）—— 需要按策略处理，非最后段必须报错。 */
        CORRUPT
    }

    /**
     * 判定结论（不可变）。
     */
    public static final class Verdict {

        /** 结论种类。 */
        public final Kind kind;
        /** 命中的规则标识，如 {@code "rule3.frameLen"}；OK 时为 {@code null}。 */
        public final String rule;
        /** 规则通过时的帧长（{@code kind == OK} 时有效），否则为 -1。 */
        public final int frameLen;
        /** 帧内记录的 CRC（{@code rule5} 时用于拼错误消息），否则为 0。 */
        public final int expectedCrc;
        /** 实际计算的 CRC，否则为 0。 */
        public final int actualCrc;
        /** 人类可读说明。 */
        public final String detail;

        private Verdict(Kind kind, String rule, int frameLen, int expectedCrc, int actualCrc,
                        String detail) {
            this.kind = kind;
            this.rule = rule;
            this.frameLen = frameLen;
            this.expectedCrc = expectedCrc;
            this.actualCrc = actualCrc;
            this.detail = detail;
        }

        static Verdict ok(int frameLen) {
            return new Verdict(Kind.OK, null, frameLen, 0, 0, null);
        }

        static Verdict torn(String rule, String detail) {
            return new Verdict(Kind.TORN, rule, -1, 0, 0, detail);
        }

        static Verdict corrupt(String rule, int expectedCrc, int actualCrc, String detail) {
            return new Verdict(Kind.CORRUPT, rule, -1, expectedCrc, actualCrc, detail);
        }

        /**
         * 是否通过。
         *
         * @return 通过返回 true
         */
        public boolean isOk() {
            return kind == Kind.OK;
        }

        @Override
        public String toString() {
            return "Verdict{" + kind + (rule == null ? "" : ", " + rule)
                    + (detail == null ? "" : ", " + detail) + '}';
        }
    }

    /**
     * 按规则 ①~④ 检查帧头（不校验 CRC）。
     *
     * @param header 帧头缓冲区，<b>从下标 0 开始</b>至少含 {@code HEADER_LEN} 字节
     * @param offset 该帧在文件中的起始偏移
     * @param fileLength 文件当前长度
     * @return 判定结论；{@link Kind#OK} 时 {@link Verdict#frameLen} 有效
     */
    public Verdict inspect(ByteBuffer header, long offset, long fileLength) {
        // ① 帧头都不完整
        if (fileLength - offset < FrameLayout.HEADER_LEN) {
            return Verdict.torn("rule1.shortHeader",
                    "剩余字节 " + (fileLength - offset) + " < 帧头 " + FrameLayout.HEADER_LEN);
        }
        if (header.limit() < FrameLayout.HEADER_LEN) {
            return Verdict.torn("rule1.shortHeader",
                    "读到的帧头只有 " + header.limit() + " 字节");
        }
        // ② magic 被破坏
        int magic = header.getInt(FrameLayout.OFFSET_MAGIC);
        if (magic != FrameLayout.MAGIC) {
            if (magic == 0) {
                // 全零帧头 —— 这是 SPARSE/FILL 预分配留下的<b>未使用尾部</b>，
                // 不是损坏。区分这一点至关重要：若当成损坏，每次重启都会把
                // "预分配量" 记成 truncatedBytes、把 clean 判成 false，
                // 进而做出"把预分配尾部截断"这种破坏 SPARSE 语义的动作
                // （之后 mmap 写在文件末尾之外会 SIGBUS）。
                return Verdict.torn(SUMMARY_ZERO_TAIL, "全零帧头（预分配未使用区域）");
            }
            return Verdict.corrupt("rule2.magic", 0, 0,
                    String.format("magic=0x%08X，期望 0x%08X", magic, FrameLayout.MAGIC));
        }
        // ③ frameLen 是垃圾值
        int frameLen = header.getInt(FrameLayout.OFFSET_FRAME_LEN);
        if (!FrameLayout.isValidFrameLen(frameLen)) {
            return Verdict.corrupt("rule3.frameLen", 0, 0,
                    "frameLen=" + frameLen + " 不在 [" + FrameLayout.MIN_FRAME_LEN + ", "
                            + FrameLayout.MAX_FRAME_LEN + "] 或不是 " + FrameLayout.ALIGNMENT + " 的倍数");
        }
        int headerLen = MemoryUtils.getUnsignedByte(header, FrameLayout.OFFSET_HEADER_LEN);
        if (!FrameLayout.isReadableHeaderLen(headerLen) || headerLen > frameLen) {
            return Verdict.corrupt("rule3.headerLen", 0, 0,
                    "headerLen=" + headerLen + " 非法（frameLen=" + frameLen + "）");
        }
        // ④ payload 没写完
        if (offset + frameLen > fileLength) {
            return Verdict.torn("rule4.frameBeyondEof",
                    "offset=" + offset + " + frameLen=" + frameLen + " > fileLength=" + fileLength);
        }
        return Verdict.ok(frameLen);
    }

    /**
     * 规则 ⑤：比对帧内 CRC 与实际计算值。
     *
     * @param frame 含完整帧的缓冲区（帧从下标 0 开始），{@code limit() >= frameLen}
     * @param frameLen 帧长
     * @return CRC 一致返回 true
     */
    public boolean crcMatches(ByteBuffer frame, int frameLen) {
        int expected = frame.getInt(FrameLayout.OFFSET_CRC32C);
        int actual = Crc32cUtil.computeFrame(frame, 0, frameLen);
        return expected == actual;
    }

    /**
     * 规则 ⑤ 的详细版本（失败时给出期望/实际 CRC，供错误消息使用）。
     *
     * @param frame 含完整帧的缓冲区（帧从下标 0 开始）
     * @param frameLen 帧长
     * @return 判定结论；CRC 一致时返回 {@link Kind#OK}
     */
    public Verdict verifyCrc(ByteBuffer frame, int frameLen) {
        int expected = frame.getInt(FrameLayout.OFFSET_CRC32C);
        int actual = Crc32cUtil.computeFrame(frame, 0, frameLen);
        if (expected == actual) {
            return Verdict.ok(frameLen);
        }
        return Verdict.corrupt("rule5.crc", expected, actual,
                String.format("crc32c 期望 0x%08X，实际 0x%08X", expected, actual));
    }
}
