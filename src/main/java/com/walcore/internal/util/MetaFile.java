package com.walcore.internal.util;

import com.walcore.internal.segment.SegmentNaming;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code wal.meta} 的读写：<b>手写 KV 文本 + 尾部 CRC 行 + 临时文件原子替换</b>。
 *
 * <h2>文件格式（每行一条，最后一行是 CRC）</h2>
 * <pre>
 * formatVersion=1
 * epoch=1
 * nextLsn=131073
 * cleanShutdown=false
 * createdAtMillis=1731245000000
 * partitionCount=16
 * partitionIds=0,1,2,...,15
 * partitionStates=0:ACTIVE,1:ACTIVE,...
 * nextSegmentId=131072
 * crc32c=1234567890
 * </pre>
 * 为什么用手写文本而不是 JSON/Binary：零依赖（约束 1）；人类可直接 {@code cat} 排查；
 * 行数极少（分区数 + 10 行），解析成本可忽略。
 *
 * <h2>为什么必须原子替换</h2>
 * 崩溃可能发生在"写到一半"的时刻。若直接覆盖 {@code wal.meta}，重启后可能读到半截文件，
 * 进而误判 {@code cleanShutdown} / {@code nextLsn}，这是最危险的错误之一。
 * 因此一律"写 {@code .tmp} → force → ATOMIC_MOVE"。
 *
 * <h2>读到半截内容怎么办</h2>
 * CRC 校验失败时 {@link Meta#crcValid()} 返回 false，调用方（{@code RecoveryManager}）
 * 应"扫描 partitions 目录重建 meta"（ARCH §9.1 分支 C2），而不是直接报错退出。
 */
public final class MetaFile {

    /** {@code formatVersion} 键。 */
    public static final String KEY_FORMAT_VERSION = "formatVersion";
    /** {@code epoch} 键（每次 clean 关闭后 +1，用于人眼区分实例代际）。 */
    public static final String KEY_EPOCH = "epoch";
    /** {@code nextLsn} 键。 */
    public static final String KEY_NEXT_LSN = "nextLsn";
    /** {@code cleanShutdown} 键。 */
    public static final String KEY_CLEAN_SHUTDOWN = "cleanShutdown";
    /** {@code createdAtMillis} 键。 */
    public static final String KEY_CREATED_AT = "createdAtMillis";
    /** {@code partitionCount} 键。 */
    public static final String KEY_PARTITION_COUNT = "partitionCount";
    /** {@code partitionIds} 键。 */
    public static final String KEY_PARTITION_IDS = "partitionIds";
    /** {@code partitionStates} 键。 */
    public static final String KEY_PARTITION_STATES = "partitionStates";
    /** {@code nextSegmentId} 键。 */
    public static final String KEY_NEXT_SEGMENT_ID = "nextSegmentId";
    /** {@code purgedPartitions} 键（{@code id:lastLsn:purgedAtMillis} 逗号分隔）。 */
    public static final String KEY_PURGED_PARTITIONS = "purgedPartitions";
    /** CRC 行的键名。 */
    public static final String CRC_KEY = "crc32c";

    /** CRC 行前缀。 */
    public static final String CRC_PREFIX = CRC_KEY + "=";

    private MetaFile() {
    }

    /**
     * {@code wal.meta} 的解析结果。
     */
    public static final class Meta {

        private final Map<String, String> values;
        private final boolean present;
        private final boolean crcValid;

        Meta(Map<String, String> values, boolean present, boolean crcValid) {
            this.values = Collections.unmodifiableMap(values);
            this.present = present;
            this.crcValid = crcValid;
        }

        /**
         * 文件是否存在。
         *
         * @return 存在返回 true
         */
        public boolean present() {
            return present;
        }

        /**
         * CRC 是否校验通过。
         *
         * @return 通过返回 true
         */
        public boolean crcValid() {
            return crcValid;
        }

        /**
         * 是否可以信任其内容（存在且 CRC 有效）。
         *
         * @return 可信任返回 true
         */
        public boolean trusted() {
            return present && crcValid;
        }

        /**
         * 取字符串值。
         *
         * @param key 键
         * @return 值，不存在返回 null
         */
        public String get(String key) {
            return values.get(key);
        }

        /**
         * 取长整数值。
         *
         * @param key 键
         * @param defaultValue 缺省值
         * @return 值
         */
        public long getLong(String key, long defaultValue) {
            String v = values.get(key);
            if (v == null || v.isEmpty()) {
                return defaultValue;
            }
            try {
                return Long.parseLong(v.trim());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }

        /**
         * 取布尔值。
         *
         * @param key 键
         * @param defaultValue 缺省值
         * @return 值
         */
        public boolean getBoolean(String key, boolean defaultValue) {
            String v = values.get(key);
            if (v == null || v.isEmpty()) {
                return defaultValue;
            }
            return Boolean.parseBoolean(v.trim());
        }

        /**
         * 全部键值（不可变）。
         *
         * @return 映射
         */
        public Map<String, String> values() {
            return values;
        }

        @Override
        public String toString() {
            return "Meta{present=" + present + ", crcValid=" + crcValid + ", values=" + values + '}';
        }
    }

    /**
     * 读取 {@code wal.meta}。文件不存在时返回 {@code present=false} 的空结果（不抛异常）。
     *
     * @param baseDir 日志根目录
     * @return 解析结果
     * @throws IOException 读取失败（权限等）
     */
    public static Meta read(Path baseDir) throws IOException {
        Path file = baseDir.resolve(SegmentNaming.META_FILE);
        if (!Files.exists(file)) {
            return new Meta(new LinkedHashMap<>(), false, false);
        }
        byte[] bytes = Files.readAllBytes(file);
        String text = new String(bytes, StandardCharsets.UTF_8);
        List<String> lines = text.lines().toList();

        int crcLineIndex = -1;
        for (int i = lines.size() - 1; i >= 0; i--) {
            if (lines.get(i).startsWith(CRC_PREFIX)) {
                crcLineIndex = i;
                break;
            }
        }
        Map<String, String> values = new LinkedHashMap<>();
        StringBuilder body = new StringBuilder(text.length());
        boolean crcValid = false;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (i == crcLineIndex) {
                int expected;
                try {
                    expected = Integer.parseInt(line.substring(CRC_PREFIX.length()).trim());
                } catch (NumberFormatException e) {
                    expected = 0;
                }
                int actual = Crc32cUtil.computeRange(
                        body.toString().getBytes(StandardCharsets.UTF_8));
                crcValid = expected == actual;
                continue;
            }
            body.append(line).append('\n');
            int eq = line.indexOf('=');
            if (eq > 0) {
                values.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
        if (crcLineIndex < 0) {
            crcValid = false;
        }
        return new Meta(values, true, crcValid);
    }

    /**
     * 原子写入 {@code wal.meta}。
     *
     * @param baseDir 日志根目录
     * @param values 要写入的键值（按迭代顺序输出）
     * @throws IOException 写入失败
     */
    public static void write(Path baseDir, Map<String, String> values) throws IOException {
        Preconditions.checkNotNull(baseDir, "baseDir");
        Preconditions.checkNotNull(values, "values");
        Files.createDirectories(baseDir);
        StringBuilder body = new StringBuilder(256);
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (e.getKey() == null || isCrcKey(e.getKey())) {
                // 防止调用方误传 crc 行导致文件里出现两行 crc32c=，
                // 让 read() 的"最后一行是 CRC"假定失效
                continue;
            }
            body.append(e.getKey()).append('=').append(e.getValue() == null ? "" : e.getValue())
                    .append('\n');
        }
        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.UTF_8);
        int crc = Crc32cUtil.computeRange(bodyBytes);
        String full = body + CRC_PREFIX + crc + "\n";

        Path file = baseDir.resolve(SegmentNaming.META_FILE);
        Path tmp = baseDir.resolve(SegmentNaming.META_FILE + SegmentNaming.TMP_EXT);
        byte[] fullBytes = full.getBytes(StandardCharsets.UTF_8);
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(fullBytes);
            long pos = 0L;
            while (buf.hasRemaining()) {
                pos += ch.write(buf, pos);
            }
            ch.force(true);
        }
        try {
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
        forceParentDir(baseDir);
    }

    /**
     * 判断某个键是否等价于 CRC 行（容忍 {@code "crc32c"} 与 {@code "crc32c="} 两种写法）。
     *
     * @param key 键
     * @return 是 CRC 键返回 true
     */
    private static boolean isCrcKey(String key) {
        String k = key.endsWith("=") ? key.substring(0, key.length() - 1) : key;
        return CRC_KEY.equals(k);
    }

    /**
     * 组装一份常用的 meta 键值表。
     *
     * @param formatVersion 格式版本
     * @param epoch 实例代际
     * @param nextLsn 下一个可用 LSN
     * @param cleanShutdown 是否干净关闭
     * @param partitionIds 分区 ID 列表（逗号分隔）
     * @return 键值表
     */
    public static Map<String, String> newEntries(int formatVersion, long epoch, long nextLsn,
                                                 boolean cleanShutdown, String partitionIds) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(KEY_FORMAT_VERSION, Integer.toString(formatVersion));
        m.put(KEY_EPOCH, Long.toString(epoch));
        m.put(KEY_NEXT_LSN, Long.toString(nextLsn));
        m.put(KEY_CLEAN_SHUTDOWN, Boolean.toString(cleanShutdown));
        m.put(KEY_CREATED_AT, Long.toString(System.currentTimeMillis()));
        m.put(KEY_PARTITION_IDS, partitionIds == null ? "" : partitionIds);
        return m;
    }

    /**
     * 对父目录 {@code force(true)}（Linux 需要，Windows 降级为 no-op）。
     *
     * @param dir 目录
     */
    private static void forceParentDir(Path dir) {
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // 平台不支持目录 fsync，明确降级
        }
    }
}
