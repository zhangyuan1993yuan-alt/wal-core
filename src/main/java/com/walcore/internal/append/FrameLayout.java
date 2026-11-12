package com.walcore.internal.append;

/**
 * 记录帧格式的<b>唯一真源</b>（single source of truth）。
 *
 * <p>本类的每一个偏移常量与大小常量均逐项对应 ARCH-wal.md §4.1 的字节布局表。
 * 编码器（{@code FrameEncoder}）、解码器（{@code FrameDecoder}）、恢复扫描器
 * （{@code SegmentScanner}）、撕裂写检测（{@code TornWriteDetector}）都必须从这里取偏移，
 * <b>禁止</b>在任何别处硬编码数字字面量。改这里一处，全链路一致；改别处一处，恢复测试会以
 * "帧偏移差 4 字节"这种极难定位的方式失败。
 *
 * <h2>帧布局（64B 固定帧头 + 变长 payload + 8B 对齐填充）</h2>
 * <pre>
 * offset  field             type        len  note
 *      0  magic             int32 LE     4   0x57414C31 ("WAL1")
 *      4  version           uint8        1   当前 1
 *      5  headerLen         uint8        1   当前 64；前向兼容关键
 *      6  recordType        uint8        1   1=DATA ... 128~255 用户自定义
 *      7  flags             uint8        1   bit0..bit5 已定义，bit6~7 保留（读者必须忽略）
 *      8  frameLen          int32 LE     4   整帧字节数，恒为 8 的倍数
 *     12  payloadLen        int32 LE     4   逻辑 payload 长度
 *     16  storedLen         int32 LE     4   实际存储 payload 字节数（不含填充）
 *     20  crc32c            int32 LE     4   CRC 覆盖 [0,20) ∪ [24, frameLen)
 *     24  lsn               int64 LE     8
 *     32  timestampNanos    int64 LE     8
 *     40  partitionId       int32 LE     4
 *     44  codecId           int32 LE     4
 *     48  attributes        int64 LE     8
 *     56  reserved0         int32 LE     4   必须写 0，读者忽略
 *     60  reserved1         int32 LE     4   必须写 0，读者忽略
 *     64  payload           bytes[storedLen] 变长
 * 64+storedLen padding        bytes[0..7]  补 0 使 frameLen 为 8 的倍数
 * </pre>
 *
 * <p><b>为什么帧头是 64B 而不是紧凑的 32B</b>：① 16MB payload 必须 8 字节对齐访问
 * {@code lsn}/{@code timestamp}；② 64B = 一个 cache line，帧头读写正好一次缓存行加载；
 * ③ 留 8B 保留字段，未来加字段不必改 {@code headerLen}。
 */
public final class FrameLayout {

    private FrameLayout() {
        // 工具类禁止实例化
    }

    // ------------------------------------------------------------------
    // 1. 魔数与版本
    // ------------------------------------------------------------------

    /** 帧魔数：ASCII {@code "WAL1"}，用于快速定位帧边界与重同步。 */
    public static final int MAGIC = 0x57414C31;

    /** 当前帧格式主版本。 */
    public static final byte FORMAT_VERSION = 0x01;

    // ------------------------------------------------------------------
    // 2. 帧头字段偏移（全部为绝对偏移，单位字节）
    // ------------------------------------------------------------------

    /** 魔数偏移。 */
    public static final int OFFSET_MAGIC = 0;
    /** 格式版本偏移。 */
    public static final int OFFSET_VERSION = 4;
    /** 帧头长度偏移（uint8）。 */
    public static final int OFFSET_HEADER_LEN = 5;
    /** 记录类型偏移（uint8）。 */
    public static final int OFFSET_RECORD_TYPE = 6;
    /** 标志位偏移（uint8）。 */
    public static final int OFFSET_FLAGS = 7;
    /** 整帧长度偏移（int32 LE）。 */
    public static final int OFFSET_FRAME_LEN = 8;
    /** 逻辑 payload 长度偏移（int32 LE）。 */
    public static final int OFFSET_PAYLOAD_LEN = 12;
    /** 实际存储 payload 长度偏移（int32 LE）。 */
    public static final int OFFSET_STORED_LEN = 16;
    /** CRC32C 偏移（int32 LE）。 */
    public static final int OFFSET_CRC32C = 20;
    /** 全局 LSN 偏移（int64 LE）。 */
    public static final int OFFSET_LSN = 24;
    /** 写入时刻（epoch 纳秒）偏移（int64 LE）。 */
    public static final int OFFSET_TIMESTAMP_NANOS = 32;
    /** 分区 ID 偏移（int32 LE）。 */
    public static final int OFFSET_PARTITION_ID = 40;
    /** 编码器 ID 偏移（int32 LE）。 */
    public static final int OFFSET_CODEC_ID = 44;
    /** 复用属性字段偏移（int64 LE）。 */
    public static final int OFFSET_ATTRIBUTES = 48;
    /** 保留字段 0 偏移。 */
    public static final int OFFSET_RESERVED0 = 56;
    /** 保留字段 1 偏移。 */
    public static final int OFFSET_RESERVED1 = 60;

    // ------------------------------------------------------------------
    // 3. 大小常量
    // ------------------------------------------------------------------

    /** 帧头固定长度（字节）。 */
    public static final int HEADER_LEN = 64;

    /** CRC 覆盖区间的第一段 {@code [0, CRC_SKIP_START)}。 */
    public static final int CRC_SKIP_START = OFFSET_CRC32C;          // 20
    /** CRC 覆盖区间的第二段起点 {@code [CRC_SKIP_END, frameLen)}。 */
    public static final int CRC_SKIP_END = OFFSET_LSN;               // 24

    /** 帧长对齐粒度：{@code frameLen} 恒为 8 的倍数。 */
    public static final int ALIGNMENT = 8;

    /** 最小 payload 长度（字节）。 */
    public static final int MIN_PAYLOAD_LEN = 8;

    /** 单条 payload 上限：16MB。 */
    public static final int MAX_PAYLOAD_LEN = 16 * 1024 * 1024;      // 16_777_216

    /**
     * 单条 payload 上限的别名（TASKS-wal.md T01 ③ 使用的名字为 {@code MAX_PAYLOAD_BYTES}）。
     * 与 {@link #MAX_PAYLOAD_LEN} 同值，保持单一真源。
     */
    public static final int MAX_PAYLOAD_BYTES = MAX_PAYLOAD_LEN;

    /** 单帧上限 = 16MB + 64 + 8 = {@code 16_777_288}。 */
    public static final int MAX_FRAME_LEN = MAX_PAYLOAD_LEN + HEADER_LEN + ALIGNMENT;

    /** 最小帧长 = 64 + 8（最小 payload）+ 0（已对齐）= {@code 72}。 */
    public static final int MIN_FRAME_LEN = HEADER_LEN + MIN_PAYLOAD_LEN;

    /**
     * 压测基准帧长 {@code 328}：对应 256B payload 的"帧头 + payload + 满 8B 填充"口径。
     *
     * <p><b>与 {@link #frameLenFor(int)} 的差异（ARCH §4.1 的算术不一致，已记录）</b>：
     * {@code 64 + 256 = 320} 本身已是 8 的倍数，按"对齐后填充 0 字节"的规则实际帧长是
     * <b>320</b>；ARCH 把它写成 328（多算了一个满填充），同理 {@link #MAX_FRAME_LEN}
     * 也按 {@code 16MB + 64 + 8} 多算了 8 字节。本类保留这两个常量原值以对齐验收口径
     * （TASKS-wal.md T01 ③ 明确要求 {@code MAX_FRAME_LEN = 16_777_288}），
     * 但把 {@link #MAX_FRAME_LEN} 当作"帧长合法性上限"而不是"必然出现的帧长"：
     * 实际最大帧是 {@code frameLenFor(16MB) = 16_777_280 < 16_777_288}，仍然通过校验。
     * 压测容量估算按 328B/条是保守的（高估 8B，不影响正确性）。
     */
    public static final int TYPICAL_FRAME_LEN = HEADER_LEN + 256 + ALIGNMENT;

    // ------------------------------------------------------------------
    // 4. 记录类型（recordType）
    // ------------------------------------------------------------------

    /** 记录类型常量。{@code 128~255} 保留给用户自定义。 */
    public static final class RecordTypes {
        private RecordTypes() {
        }

        /** 业务数据记录。 */
        public static final byte DATA = 1;
        /** 检查点标记。 */
        public static final byte CHECKPOINT = 2;
        /** 快照标记。 */
        public static final byte SNAPSHOT_MARK = 3;
        /** 控制记录（轮转/元数据变更等）。 */
        public static final byte CONTROL = 4;
        /** 索引锚点记录。 */
        public static final byte IDX_ANCHOR = 5;
        /** 墓碑记录（分区删除）。 */
        public static final byte TOMBSTONE = 6;
        /** 用户自定义类型下界。 */
        public static final int USER_MIN = 128;
    }

    // ------------------------------------------------------------------
    // 5. 标志位（flags）
    // ------------------------------------------------------------------

    /** flags 位定义。bit6~7 保留，读者必须忽略（不得因未定义的位拒绝读帧）。 */
    public static final class Flags {
        private Flags() {
        }

        /** payload 经过 codec 压缩。 */
        public static final byte COMPRESSED = 0x01;
        /** payload 加密（保留，当前恒不置位）。 */
        public static final byte ENCRYPTED = 0x02;
        /** {@code attributes} 字段存放幂等键摘要。 */
        public static final byte IDEMPOTENT_KEY = 0x04;
        /** 事务提交点。 */
        public static final byte TXN_COMMIT = 0x08;
        /** 分区内首条记录。 */
        public static final byte PARTITION_FIRST = 0x10;
        /** 分区内末条记录。 */
        public static final byte PARTITION_LAST = 0x20;
        /** 所有已定义位的掩码。 */
        public static final byte DEFINED_MASK = COMPRESSED | ENCRYPTED | IDEMPOTENT_KEY
                | TXN_COMMIT | PARTITION_FIRST | PARTITION_LAST;

        /**
         * 判断某个 flags 字节在当前版本是否只含有已定义位。
         * <b>注意</b>：为 true 并不表示帧合法，为 false 也<b>不得</b>拒绝该帧——仅供诊断用。
         *
         * @param flags 帧头中的 flags 字节
         * @return 是否不含未知位
         */
        public static boolean hasOnlyDefinedBits(byte flags) {
            return (flags & ~DEFINED_MASK) == 0;
        }
    }

    // ------------------------------------------------------------------
    // 6. 派生计算
    // ------------------------------------------------------------------

    /**
     * 计算给定存储长度所需的尾部填充字节数，使整帧长度为 8 的倍数。
     *
     * @param storedLen 实际存储 payload 字节数，必须 {@code >= 0}
     * @return 填充字节数，取值 0..7
     */
    public static int paddingFor(int storedLen) {
        int rest = (HEADER_LEN + storedLen) & (ALIGNMENT - 1);
        return rest == 0 ? 0 : ALIGNMENT - rest;
    }

    /**
     * 给定存储长度计算整帧长度。
     *
     * @param storedLen 实际存储 payload 字节数，必须 {@code >= 0}
     * @return {@code HEADER_LEN + storedLen + padding}
     */
    public static int frameLenFor(int storedLen) {
        return HEADER_LEN + storedLen + paddingFor(storedLen);
    }

    /**
     * 判断一个 {@code frameLen} 是否落在合法区间且对齐正确。
     *
     * @param frameLen 待校验的整帧长度
     * @return 合法返回 true
     */
    public static boolean isValidFrameLen(int frameLen) {
        return frameLen >= MIN_FRAME_LEN
                && frameLen <= MAX_FRAME_LEN
                && (frameLen & (ALIGNMENT - 1)) == 0;
    }

    /**
     * 判断一个 {@code payloadLen} 是否在允许区间。
     *
     * @param payloadLen 逻辑 payload 长度
     * @return 合法返回 true
     */
    public static boolean isValidPayloadLen(int payloadLen) {
        return payloadLen >= 0 && payloadLen <= MAX_PAYLOAD_LEN;
    }

    /**
     * 判断一个 {@code partitionId} 是否在设计允许范围内。
     *
     * @param partitionId 分区 ID
     * @param maxPartitions 分区上限
     * @return 合法返回 true
     */
    public static boolean isValidPartitionId(int partitionId, int maxPartitions) {
        return partitionId >= 0 && partitionId < maxPartitions;
    }

    /**
     * 判断一个 {@code headerLen} 是否为可被本版本理解的帧头长度。
     *
     * <p>前向兼容规则：{@code headerLen >= HEADER_LEN} 即可读；比 64 大的部分由解码器
     * 以 {@code headerLen} 为步长跳过（<b>不得</b>硬编码 64）。
     *
     * @param headerLen 帧头长度
     * @return 可读返回 true
     */
    public static boolean isReadableHeaderLen(int headerLen) {
        return headerLen >= HEADER_LEN && headerLen < 256;
    }
}
