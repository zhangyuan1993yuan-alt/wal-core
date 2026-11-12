package com.walcore.internal.snapshot;

import com.walcore.api.WalException;
import com.walcore.api.WalVersion;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;
import com.walcore.spi.Priority;
import com.walcore.spi.SnapshotMeta;
import com.walcore.spi.SnapshotStore;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 默认快照存储：<b>自描述单文件 + SHA-256 校验 + 原子落盘</b>（{@code builtin:snapshot-file}）。
 *
 * <h2>文件格式（64 字节定长头 + 内容）</h2>
 * <pre>
 * [0,8)    magic "WALSNAP1"
 * [8,16)   uptoLsn        —— 快照覆盖到的最后一条 LSN（含）
 * [16,24)  createdAtMillis
 * [24,32)  stateBytes
 * [32,64)  SHA-256（32 字节原始摘要）
 * [64,..)  状态内容（由上层业务序列化）
 * </pre>
 *
 * <h2>为什么把元数据放进文件头而不是单独的 {@code .meta}</h2>
 * 两个文件就有"元数据在、内容丢了"或"内容在、元数据丢了"的中间态，
 * 需要一套额外的配对/修复逻辑。单文件自描述让"快照要么完整可见、要么完全不可见"，
 * 配合 {@code .tmp → ATOMIC_MOVE}，中断后目录里只会留下可被清理的 {@code .tmp}。
 *
 * <h2>写入为什么要写两遍头</h2>
 * 摘要只有读完内容才知道。所以顺序是：写全零头 → 流式写内容并边算摘要 → 回头把真头写回
 * 偏移 0 → {@code ATOMIC_MOVE}。整个过程只读一遍输入流，不需要把状态全塞进内存。
 */
@Priority(0)
public final class FileSnapshotStore implements SnapshotStore {

    /** 内置 id（配置契约的一部分，不得修改）。 */
    public static final String ID = "builtin:snapshot-file";

    /** 文件 magic。 */
    private static final byte[] MAGIC = {'W', 'A', 'L', 'S', 'N', 'A', 'P', '1'};

    /** 头部长度。 */
    private static final int HEADER_BYTES = 64;

    /** 摘要长度（SHA-256）。 */
    private static final int DIGEST_BYTES = 32;

    /** 快照目录名。 */
    private static final String SNAPSHOT_DIR = "snapshots";

    /** 扩展名。 */
    private static final String SNAP_EXT = ".snap";

    /** 临时扩展名。 */
    private static final String TMP_EXT = ".tmp";

    private final Path rootDir;
    private volatile PluginContext context;

    /**
     * 用日志根目录构造（快照落在 {@code <baseDir>/snapshots}）。
     *
     * @param baseDir 日志根目录
     */
    public FileSnapshotStore(Path baseDir) {
        this.rootDir = baseDir.resolve(SNAPSHOT_DIR);
        try {
            Files.createDirectories(this.rootDir);
        } catch (IOException e) {
            throw new WalException.IoException("创建快照目录失败: " + this.rootDir, e);
        }
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public int apiVersion() {
        return WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext pluginContext) throws PluginException {
        this.context = pluginContext;
    }

    // ==================================================================
    // 写
    // ==================================================================

    @Override
    public SnapshotMeta write(int partitionId, long uptoLsn, InputStream state, long stateBytes) {
        if (partitionId < 0) {
            throw new WalException.PartitionException("partitionId 不得为负: " + partitionId);
        }
        if (uptoLsn < 0L) {
            throw new IllegalArgumentException("uptoLsn 不得为负: " + uptoLsn);
        }
        if (state == null) {
            throw new IllegalArgumentException("state 不得为 null");
        }
        long created = System.currentTimeMillis();
        Path target = pathOf(partitionId, uptoLsn);
        Path tmp = target.resolveSibling(target.getFileName() + TMP_EXT);
        byte[] digest;
        long written;
        try {
            Files.deleteIfExists(tmp);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                // 1) 先占位写全零头，摘要算完再回填
                ch.write(ByteBuffer.allocate(HEADER_BYTES), 0L);
                MessageDigest md = sha256();
                written = copyAndDigest(state, ch, md);
                digest = md.digest();
                // 2) 回填真头
                ch.write(ByteBuffer.wrap(buildHeader(uptoLsn, created, written, digest)), 0L);
                ch.force(true);
            }
            // 3) 原子替换
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            forceDirectory();
        } catch (IOException e) {
            deleteQuietly(tmp);
            throw new WalException.IoException(
                    "写快照失败（partition=" + partitionId + ", uptoLsn=" + uptoLsn + "）", e);
        }
        return new SnapshotMeta(partitionId, uptoLsn, created, written, hex(digest), target);
    }

    /**
     * 流式拷贝并同时更新摘要。
     *
     * @param in 输入流
     * @param ch 目标通道
     * @param md 摘要器
     * @return 写入字节数
     * @throws IOException IO 失败
     */
    private static long copyAndDigest(InputStream in, FileChannel ch, MessageDigest md)
            throws IOException {
        byte[] buf = new byte[64 * 1024];
        long total = 0L;
        int n;
        long pos = HEADER_BYTES;
        while ((n = in.read(buf)) > 0) {
            md.update(buf, 0, n);
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, n);
            while (bb.hasRemaining()) {
                pos += ch.write(bb, pos);
            }
            total += n;
        }
        return total;
    }

    /**
     * 构造 64 字节头。
     *
     * @param uptoLsn 覆盖 LSN
     * @param created 创建时刻
     * @param stateBytes 内容字节数
     * @param digest 摘要（32 字节）
     * @return 头部字节
     */
    private static byte[] buildHeader(long uptoLsn, long created, long stateBytes, byte[] digest) {
        ByteBuffer bb = ByteBuffer.allocate(HEADER_BYTES);
        bb.put(MAGIC);
        bb.putLong(uptoLsn);
        bb.putLong(created);
        bb.putLong(stateBytes);
        bb.put(digest, 0, Math.min(DIGEST_BYTES, digest.length));
        return bb.array();
    }

    // ==================================================================
    // 读
    // ==================================================================

    @Override
    public Optional<SnapshotMeta> latest(int partitionId) {
        List<SnapshotMeta> all = list(partitionId);
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(all.size() - 1));
    }

    @Override
    public List<SnapshotMeta> list(int partitionId) {
        if (!Files.isDirectory(rootDir)) {
            return List.of();
        }
        List<SnapshotMeta> out = new ArrayList<>();
        String prefix = "snapshot-p" + String.format(Locale.ROOT, "%04d", partitionId) + "-";
        try (Stream<Path> files = Files.list(rootDir)) {
            for (Path p : files.toList()) {
                String name = p.getFileName().toString();
                if (!name.startsWith(prefix) || !name.endsWith(SNAP_EXT)) {
                    continue;
                }
                SnapshotMeta meta = readHeader(p);
                if (meta != null && meta.partitionId() == partitionId) {
                    out.add(meta);
                }
            }
        } catch (IOException e) {
            throw new WalException.IoException("列出快照失败（partition=" + partitionId + "）", e);
        }
        out.sort(Comparator.comparingLong(SnapshotMeta::uptoLsn));
        return List.copyOf(out);
    }

    /**
     * 解析文件头（不做内容校验，便于 {@link #list} 快速扫描）。
     *
     * @param file 快照文件
     * @return 元数据；头部损坏返回 null
     */
    private static SnapshotMeta readHeader(Path file) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer hdr = ByteBuffer.allocate(HEADER_BYTES);
            while (hdr.hasRemaining()) {
                if (ch.read(hdr) < 0) {
                    return null;
                }
            }
            hdr.flip();
            byte[] magic = new byte[MAGIC.length];
            hdr.get(magic);
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) {
                    return null;
                }
            }
            long uptoLsn = hdr.getLong();
            long created = hdr.getLong();
            long stateBytes = hdr.getLong();
            byte[] digest = new byte[DIGEST_BYTES];
            hdr.get(digest);
            return new SnapshotMeta(partitionIdOf(file.getFileName().toString()), uptoLsn,
                    created, stateBytes, hex(digest), file);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public InputStream open(SnapshotMeta meta) {
        if (meta == null) {
            throw new IllegalArgumentException("meta 不得为 null");
        }
        Path file = meta.path();
        if (file == null || !Files.isRegularFile(file)) {
            throw new WalException.SegmentMissingException("快照文件不存在: " + file);
        }
        SnapshotMeta actual = readHeader(file);
        if (actual == null) {
            throw new WalException.CorruptedException("快照文件头损坏: " + file);
        }
        String expected = meta.checksumHex();
        String real = actual.checksumHex();
        if (expected != null && !expected.equalsIgnoreCase(real)) {
            throw new WalException.CorruptedException(
                    "快照 SHA-256 校验失败，拒绝加载: " + file
                            + "（期望 " + expected + "，实际 " + real + "）");
        }
        // 先完整校验一遍再交付内容：宁可多读一遍，也不要在业务反序列化到一半时才发现摘要不对
        verifyContent(file, real);
        try {
            long skip = HEADER_BYTES;
            InputStream raw = Files.newInputStream(file);
            long skipped = 0L;
            while (skipped < skip) {
                long s = raw.skip(skip - skipped);
                if (s <= 0L) {
                    raw.close();
                    throw new WalException.CorruptedException("快照内容不足（头部之后为空）: " + file);
                }
                skipped += s;
            }
            return new FilterInputStream(raw) {
                // 仅仅是为了让调用方拿到的流"从内容第一个字节开始"
            };
        } catch (IOException e) {
            throw new WalException.IoException("打开快照失败: " + file, e);
        }
    }

    /**
     * 校验文件内容摘要。
     *
     * @param file 快照文件
     * @param expectedHex 期望的摘要 hex
     */
    private static void verifyContent(Path file, String expectedHex) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            MessageDigest md = sha256();
            ch.position(HEADER_BYTES);
            ByteBuffer buf = ByteBuffer.allocate(64 * 1024);
            while (ch.read(buf) > 0) {
                buf.flip();
                md.update(buf);
                buf.clear();
            }
            String real = hex(md.digest());
            if (!real.equalsIgnoreCase(expectedHex)) {
                throw new WalException.CorruptedException(
                        "快照内容 SHA-256 不符，拒绝加载: " + file
                                + "（头中记录 " + expectedHex + "，实际 " + real + "）");
            }
        } catch (IOException e) {
            throw new WalException.IoException("校验快照失败: " + file, e);
        }
    }

    /**
     * 以字节数组形式读取快照内容（便于上层反序列化；同样做摘要校验）。
     *
     * @param meta 元数据
     * @return 内容字节
     */
    public byte[] readAll(SnapshotMeta meta) {
        try (InputStream in = open(meta)) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new WalException.IoException("读取快照内容失败: " + meta.path(), e);
        }
    }

    /**
     * 用字节数组写一份快照（便捷方法）。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 覆盖 LSN
     * @param content 内容
     * @return 元数据
     */
    public SnapshotMeta write(int partitionId, long uptoLsn, byte[] content) {
        return write(partitionId, uptoLsn, new ByteArrayInputStream(content), content.length);
    }

    // ==================================================================
    // 删除 / pin
    // ==================================================================

    @Override
    public void deleteBefore(int partitionId, long uptoLsn) {
        for (SnapshotMeta meta : list(partitionId)) {
            if (meta.uptoLsn() < uptoLsn) {
                deleteQuietly(meta.path());
            }
        }
    }

    @Override
    public long pinnedLsn(int partitionId) {
        return latest(partitionId).map(SnapshotMeta::uptoLsn).orElse(0L);
    }

    @Override
    public void close() {
        // 无外部资源（每次操作自带通道）
    }

    // ==================================================================
    // 辅助
    // ==================================================================

    /**
     * 快照文件路径。
     *
     * @param partitionId 分区 ID
     * @param uptoLsn 覆盖 LSN
     * @return 路径
     */
    public Path pathOf(int partitionId, long uptoLsn) {
        return rootDir.resolve("snapshot-p" + String.format(Locale.ROOT, "%04d", partitionId)
                + "-" + String.format(Locale.ROOT, "%020d", uptoLsn) + SNAP_EXT);
    }

    /**
     * 快照根目录。
     *
     * @return 目录
     */
    public Path rootDir() {
        return rootDir;
    }

    /**
     * 从文件名解析分区 ID（解析失败返回 -1）。
     *
     * @param fileName 文件名
     * @return 分区 ID
     */
    private static int partitionIdOf(String fileName) {
        int p = fileName.indexOf("-p");
        if (p < 0) {
            return -1;
        }
        int end = fileName.indexOf('-', p + 2);
        if (end < 0) {
            return -1;
        }
        try {
            return Integer.parseInt(fileName.substring(p + 2, end));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * 取 SHA-256 摘要器。
     *
     * @return 摘要器
     */
    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 是 JDK 强制实现的算法，缺失说明运行环境被裁剪过
            throw new IllegalStateException("当前 JDK 不支持 SHA-256", e);
        }
    }

    /**
     * 字节转小写 hex。
     *
     * @param bytes 字节
     * @return hex
     */
    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    /**
     * 尽力删除文件。
     *
     * @param path 路径
     */
    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Windows 上句柄未释放会失败，交由下次清理
        }
    }

    /**
     * 对目录做 {@code force}（Linux 上保证目录项落盘；Windows 上是 no-op）。
     */
    private void forceDirectory() {
        try (FileChannel ch = FileChannel.open(rootDir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignored) {
            // 目录 force 在部分平台不支持，不影响正确性（只是掉电后可能丢目录项）
        }
    }

    /**
     * 当前上下文（诊断用）。
     *
     * @return 上下文
     */
    public PluginContext context() {
        return context;
    }

    @Override
    public String toString() {
        return "FileSnapshotStore{" + ID + ", dir=" + rootDir + '}';
    }

}
