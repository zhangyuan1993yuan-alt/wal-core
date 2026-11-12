# wal-core —— 单机百万级 TPS 的嵌入式 WAL 库

一个**零第三方运行时依赖**、纯 JDK 17 实现的预写日志（Write-Ahead Log），面向撮合引擎 / 消息中间件 /
账本 / 状态机复制这类"必须知道数据到底落盘了没有"的场景。

| | |
|---|---|
| 运行时依赖 | **0**（`mvn dependency:tree -Dscope=compile` 输出为空） |
| 唯一测试依赖 | `org.junit.jupiter:junit-jupiter:5.10.5` |
| JDK | 17+（用 `VarHandle`、`CRC32C`、`ByteBuffer.put(int,ByteBuffer,int,int)`） |
| 平台 | Linux x86_64（默认 mmap 后端）/ Windows / macOS（FileChannel 后端） |
| 规模 | ~100 个主代码类，0 个 `TODO` |

**它做什么**：把"一条记录"按 64 字节定长帧头 + 变长 payload 写进分段文件，分配全局单调 LSN，
按策略 `fdatasync`，崩溃后能精确截断到最后一个完整帧并重放。
**它不做什么**：不做事务、不做业务去重、不做 SQL、不做集群（集群靠复制 SPI 外接）。

---

## 0. 目录

- [1. 5 分钟上手](#1-5-分钟上手)
- [2. 核心概念](#2-核心概念)
- [3. 架构一图流](#3-架构一图流)
- [4. 性能口径（**先读这一节再谈数字**）](#4-性能口径先读这一节再谈数字)
- [5. 配置速查](#5-配置速查)
- [6. SPI 扩展指南](#6-spi-扩展指南)
- [7. 故障排查表（15 条）](#7-故障排查表15-条)
- [8. 示例与测试](#8-示例与测试)
- [9. 已知限制与非目标](#9-已知限制与非目标)

---

## 1. 5 分钟上手

`pom.xml` 加依赖（或者直接把 `src/main/java` 拷进你的项目——它没有外部依赖）：

```xml
<dependency>
  <groupId>com.walcore</groupId>
  <artifactId>wal-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

然后这 10 行就是全部：

```java
Path dir = Path.of("./demo-wal");                       // ① 数据目录，独占（LOCK 文件保护）

Wal wal = new WalBuilder()
        .baseDir(dir)
        .partitions(4)                                   // ② 4 个分区，各一把写锁
        .fsyncPolicy(WalConfig.FsyncPolicy.BATCH)        // ③ 攒批后 fsync
        .build();

long lsn = wal.append(0,                                 // ④ 纯内存追加，返回全局唯一 LSN
        ByteBuffer.wrap("order-created".getBytes(StandardCharsets.UTF_8)),
        AppendOptions.DEFAULTS);

boolean durable = wal.flushAndWait(0, lsn, 1_000L);      // ⑤ true 之后断电也丢不了

try (WalCursor cursor = wal.cursor(0,                    // ⑥ 顺序读，从 LSN 1 开始
        CursorOptions.builder().fromLsn(1L).build())) {
    WalRecord record;
    while ((record = cursor.next()) != null) {
        System.out.println(record.lsn() + " -> " + record.payloadAsUtf8());
    }
}

wal.close();                                             // ⑦ 两阶段优雅关闭
```

预期输出：

```
1 -> order-created
```

### 这段代码的三个"必须知道"

1. **`append` 不等于落盘**。它返回时数据在 **page cache**（进程 `kill -9` 不丢，**断电会丢**）。
   要"断电也不丢"就必须 `flushAndWait` 拿到 `true`（见 [§4 性能口径](#4-性能口径先读这一节再谈数字)）。
2. **`Wal` 必须 close**（用 try-with-resources 最省心）。不 close 会留着段文件的 mmap，
   在 Windows 上连临时目录都删不掉。
3. **`record.payload()` 是只读视图**，底层可能是段文件的 mmap 片段。
   要拿字节用 `payloadBytes()`，要字符串用 `payloadAsUtf8()`，**不要** `payload().array()`
   （会抛 `UnsupportedOperationException`）。要长期持有整条记录请用 `record.copy()`。

> 上面这段代码有一份**逐字等价的可执行副本**：`src/test/java/com/walcore/examples/QuickStartSmokeTest.java`。
> 它随 `mvn test` 一起跑——所以 README 里的示例永远不会"签名改了但文档没同步"。

---

## 2. 核心概念

| 概念 | 一句话 | 关键不变量 |
|---|---|---|
| **LSN** | 全局唯一的 64 位逻辑序号，从 **1** 起 | 严格递增、**绝不复用**；与墙上时钟**解耦**，时钟回拨不影响顺序。写失败的 append 可能留下**空洞**（失败记录的 LSN 视为从未存在，重启恢复从 `max(lastLsn)+1` 续号）——宁可空洞，绝不把同一个 LSN 发给两条不同的记录 |
| **分区（partition）** | 独立的写锁 + 独立的分段文件序列 | 单分区内"文件顺序 == LSN 顺序"；跨分区只共享 LSN 计数器 |
| **段（segment）** | 一个 `.wal` 文件，写满即封存（默认 256MB） | 帧**不跨段**；封存段只读、可被保留策略删除 |
| **帧（frame）** | 64B 定长头 + 变长 payload + 8B 对齐 | `frameLen == 64 + storedLen + padding`；CRC32C 覆盖 `[0,20) ∪ [24,frameLen)` |
| **稀疏索引** | 每 1000 条记一项（16B），落在 `.idx` | 内存二分定位 + 段首兜底扫描，索引丢了能重建 |
| **组提交** | 写入线程只写内存，后台批量 `fdatasync` | 写入线程**永远不** `fsync`（`BATCH` 模式下） |
| **水位** | `publishedLsn`（已可见）/ `durableLsn`（已落盘） | `durableLsn ≤ publishedLsn` 是硬约束；宁可超时也绝不假确认 |

---

## 3. 架构一图流

```
                      写入线程（业务线程，N 个）
 ┌──────────────────────────────────────────────────────────────────────┐
 │  wal.append(pid, payload, opts)                                      │
 │    ① 状态/背压检查（2 个 volatile load，失败即抛或阻塞）              │
 │    ② partition.lock()  ← 每分区一把非公平锁                          │
 │    ③ LSN 分配（全局 AtomicLong，必须在本锁内、紧邻写帧）              │
 │    ④ 编码 64B 帧头 + memcpy payload + CRC32C（SSE4.2 intrinsic）      │
 │    ⑤ 写入段（mmap 后端 = 纯 memcpy，零系统调用）                      │
 │    ⑥ publishedWritePos = storeRelease(pos)；unlock()                 │
 │    返回 LSN —— 数据此刻在 page cache                                  │
 └───────────────┬──────────────────────────────────────────────────────┘
                 │ markDirty(pid) + addBytes(n)
                 ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  DirtyTracker（位掩码分片，drainMine 无锁取走）                       │
 └───────────────┬──────────────────────────────────────────────────────┘
                 ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  GroupCommitCoordinator（groupCommitThreads=2）                      │
 │   三重攒批触发：200µs / 4096 条 / 1MB  ← 谁先到算谁                   │
 │   对每个脏分区 force(false)  →  publish durableLsn  →  unpark 等待者   │
 └───────────────┬──────────────────────────────────────────────────────┘
                 ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  SegmentManager                                                      │
 │   活跃段（每分区 1 个，mmap 写）                                      │
 │   NextSegmentPreparer（预建 + 预映射下一段，轮转零阻塞）              │
 │   只读段 ← MappedWindowCache（8 个窗口 LRU 复用，句柄数有上界）       │
 └───────────────┬──────────────────────────────────────────────────────┘
                 ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  StorageBackend（SPI，三实现共用一套契约测试）                        │
 │   builtin:storage-mapped   mmap（Linux 默认）                        │
 │   builtin:storage-fc       FileChannel + staging（Windows 默认）      │
 │   builtin:storage-heap     纯内存（测试/演示）                        │
 └──────────────────────────────────────────────────────────────────────┘

  读取侧                                   运维侧
 ┌────────────────────┐          ┌──────────────────────────────────────┐
 │ WalCursor 顺序读    │          │ RetentionManager  → RetentionPolicy  │
 │  seekTo / next      │          │ CheckpointManager → SnapshotStore    │
 │  nextBatch(visitor) │          │ MetricsRegistry   → MeterSink        │
 │ WalTailer 实时订阅  │          │   （计数器 LongAdder + 固定桶直方图） │
 └────────────────────┘          └──────────────────────────────────────┘

  崩溃恢复（启动时，cleanShutdown == false）
  ┌──────────────────────────────────────────────────────────────────────┐
  │ 加独占 LOCK → 读 wal.meta → 定位"有效尾段"（最后一个有帧的段）        │
  │   → 两遍扫描（先定位完整帧边界，再逐帧 CRC）                          │
  │   → 尾部撕裂写**物理截断**（幂等，重放多少次都一样）                  │
  │   → 校验/重建 .idx → nextLsn = max(lastLsn)+1 → 按 LSN 全局升序重放   │
  └──────────────────────────────────────────────────────────────────────┘
```

### 为什么是"多分区 × 分段"而不是"一个大文件"

| 方案 | 并发 | 归档 | 结论 |
|---|---|---|---|
| 单一大文件 | 全部写入争 1 把锁，做不到百万 TPS | 无法独立删除段 | ❌ |
| **多分区 × 分段** | 每分区独立锁，跨分区只共享 LSN | 段可独立删除/上传 | ✅ |
| 每分区单文件不分段 | 锁 OK，但单文件无限增长 | 无法归档 | ❌ |

---

## 4. 性能口径（**先读这一节再谈数字**）

WAL 的"性能"必须带口径说。同一个库，两个人可以报出差 100 倍的数字**而且都没错**——
差别只在问的是"追加"还是"持久确认"。

### 4.1 两个口径，互不替代

| | **口径 A：`append()`** | **口径 B：`flushAndWait()`** |
|---|---|---|
| 代码 | `long lsn = wal.append(pid, payload, opts);` | `boolean ok = wal.flushAndWait(pid, lsn, timeoutMs);` |
| 语义 | 已分配 LSN、已写入段映射区 | LSN ≤ lsn 的记录已经过 `fdatasync` 落盘 |
| 数据在哪 | **page cache**（内核内存） | **存储设备** |
| `kill -9` | 不丢 | 不丢 |
| **断电** | **会丢** | 不丢 |
| 能不能对外承诺"成功" | **不能** | **能**（这是唯一能回 ACK 的口径） |
| 量级（单机 NVMe） | p50 ≈ 0.5µs，百万级 TPS | p50 ≈ 200~250µs，十万级 TPS |

### 4.2 "100 万 TPS" 与 "100µs 持久确认" **在单机 NVMe 上不可同时达成**

这不是实现问题，是组提交的物理下限，算一遍就清楚了：

| 事实 | 数字 |
|---|---|
| NVMe 企业盘单次 `fdatasync` 延迟 | **p50 ≈ 80µs**，p99 ≈ 300µs |
| 想做到持久确认 p99 ≤ 100µs，攒批窗口最多 | **≈ 20µs** |
| 20µs 窗口内、100 万 TPS 下能攒多少条 | `20µs × 1M/s` = **20 条/批** |
| 20 条/批 → 每秒需要多少次 `fdatasync` | **50,000 次/s**（单分区） |
| 企业 NVMe 的 4K flush IOPS 上限 | 100k ~ 200k；消费级 SSD 仅 ~10k |
| 结果 | 20 条/批时 fsync 已吃掉一半以上 IOPS 预算，且"每次 fsync 80µs" > "20µs 窗口" → 队列必然堆积 → p99 爆掉 |

反过来：要 **100 万 TPS**，每批必须攒到 **≈3200 条（≈1MB）**，
此时持久确认延迟 = 攒批时间 + fsync ≈ **200~250µs p50 / ~1.4ms p99**（16 分区、2 组提交线程）。

### 4.3 所以正确的表述是

> **单机 NVMe：`append` 可达百万 TPS 量级（内存口径）；
> 同一台机器上 `flushAndWait` 的持久确认是 ~200µs p50 / ~1.4ms p99。
> 若要持久确认 p99 ≤ 100µs，必须把攒批窗口压到 20µs 且分区数 ≤ 4，
> 此时单分区吞吐降到 ~15 万 TPS。两个指标二选一，取决于业务要吞吐还是要延迟。**

### 4.4 三种典型配置

| 你要什么 | 配置 | 预期 |
|---|---|---|
| **最大吞吐**（日志、埋点、可容忍断电丢尾部） | 默认（`BATCH` / 200µs / 4096 条 / 1MB，16 分区） | `append` 百万 TPS；持久确认 ~250µs p50 |
| **低延迟持久确认**（撮合回 ACK） | `batchIntervalMicros(20)` + `partitions(4)` + `groupCommitThreads(4)` | 持久确认 p99 ≈ 100µs；吞吐 ~15 万 TPS |
| **最强持久性**（账本、审计） | `fsyncPolicy(ALWAYS)` | 每条 append 都 fsync；TPS ≈ 设备 fdatasync IOPS |

### 4.5 性能数字怎么自己测

`docs/PERF-wal.md` 是完整的基准文档（环境表格、命令、口径定义、预期基线、未达标项）。
三个基准都标了 `@Tag("perf")`，默认**不跑**，需显式开启：

```bash
mvn -o test -Dtest='ThroughputIT'    -Dtest.excludedGroups= -Dgroups=perf
mvn -o test -Dtest='LatencyBenchIT'  -Dtest.excludedGroups= -Dgroups=perf
mvn -o test -Dtest='RecoveryKill9IT' -Dtest.excludedGroups= -Dgroups=perf
```

> ⚠ **不要在 Windows 开发机上取数**。Windows 默认走 FileChannel 后端（单条 ~1~2µs）、
> `force()` 走 `FlushFileBuffers`、时钟中断粒度 15.6ms——µs 级分位和 fsync 吞吐都测不准。
> 在 Windows 上跑这三个 IT 的唯一合法用途是**验证基准代码本身没写错**。

---

## 5. 配置速查

`WalConfig` 的常用项（全部有 `WalBuilder` 链式 setter，全部有 Javadoc）：

| 项 | 默认 | 说明 |
|---|---|---|
| `partitionCount` | 16 | 分区数。每个分区独立写锁与文件序列 |
| `segmentSizeBytes` | 256MB | 段大小。**调大能显著降低恢复时间**（段数变少） |
| `fsyncPolicy` | `BATCH` | `ALWAYS` / `BATCH` / `PERIODIC` |
| `batchIntervalMicros` | 200 | 攒批时间上限（吞吐 vs 延迟的主旋钮） |
| `batchMaxRecords` | 4096 | 攒批条数上限 |
| `batchMaxBytes` | 1MB | 攒批字节上限 |
| `groupCommitThreads` | 2 | 组提交线程数。分区多时调大 |
| `indexIntervalRecords` | 1000 | 每多少条记一个索引项 |
| `maxPayloadBytes` | 16MB | 单条 payload 上限 |
| `corruptionPolicy` | `TAIL_TRUNCATE` | `TAIL_TRUNCATE` / `FAIL_FAST` / `SKIP_RECORD` |
| `backpressurePolicy` | 阻塞 + 超时 | staging 满 / `dirtyBytes` 超限时的行为 |
| `metricsEnabled` | true | 关掉后热路径连 `nanoTime` 都不读 |
| `retentionMaxAgeMillis` / `retentionMaxTotalBytes` | 关闭 | 时间 / 大小双维度，取**更严格**者 |
| `maxDirtyBytes` | 32MB | 未刷盘字节上限，触顶触发背压 |
| `storageBackendId` | 平台自动 | 可强制 `builtin:storage-mapped` / `-fc` / `-heap` |

> ⚠ **"已定义、暂未接线"的配置项（1.0.0 现状）**：以下项在 `WalConfig`/`WalBuilder`
> 里已定义、有 Javadoc 与默认值，但**没有任何运行时消费者**——设置它们不会改变行为：
> `minFreeBytes`（磁盘空间预检）、`offHeapBudgetBytes`（堆外预算强制）、
> `forceTimeoutMillis`（fsync watchdog）、`rotationTimeoutMillis`、`purgeTimeoutMillis`、
> `metricsIntervalMillis`（另有 `cursorCheckpointIntervalMillis`、`tailerBufferSize`、
> `tailerOverflowPolicy` 同属此类）。补齐接线属于后续版本范围；故障排查表 #1/#2/#5 的
> 措辞已按实际行为对齐，不要据此表旧印象配置这些项。

---

## 6. SPI 扩展指南

### 6.1 五类扩展点

| # | 接口 | 决定什么 | 内置实现 | 示例实现 |
|---|---|---|---|---|
| ① | `com.walcore.spi.RecordCodec` | payload 怎么编码（压缩/加密/自定义序列化） | `builtin:raw`（恒等） | `CountingCodec`、`Rot13Codec` |
| ② | `com.walcore.spi.StorageBackend` | 字节怎么落到设备（mmap / FileChannel / 自定义） | `builtin:storage-mapped` / `-fc` / `-heap` | —（改这个要过完整契约测试） |
| ③ | `com.walcore.spi.IndexStore` | LSN → 文件偏移怎么索引 | `builtin:index-sparse`（稀疏索引） | `CountBasedIndexStore` |
| ④ | `com.walcore.spi.RetentionPolicy` | **哪些段可以删**（只判断，不删除） | `builtin:retention-time-size` | `EagerRetentionPolicy` + §6.3 完整示例 |
| ⑤ | `com.walcore.spi.ReplicationSink` | 记录怎么往外部投递（Kafka / Raft / 文件） | `builtin:replication-noop` | `PrintlnReplicationSink` |
| 附 | `com.walcore.spi.SnapshotStore` | 快照怎么存 | `builtin:snapshot-file`（SHA-256 + 原子替换） | — |

> ⚠ **ReplicationSink 当前版本不投递记录**：插件会被 `ServiceLoader` 发现、参与仲裁并可通过
> `replicationSink(id)` 显式选定，但框架内**尚无调用 `onRecord` 的复制循环**——选定的 sink
> 不会被喂任何数据，`replicatedLsn()` 水位回报因此也无人消费。接口契约（含
> `WalRecordView` 的生命周期约束）已定稿，接入属于后续版本范围；当前要做复制请用
> `WalTailer` 自行外接。

### 6.2 注册与选择

**注册**（`src/main/resources/META-INF/services/<接口全名>`）：

```
# 文件：META-INF/services/com.walcore.spi.RecordCodec
com.example.wal.Rot13Codec
com.example.wal.CountingCodec
```

一个文件可以列多个实现，一行一个，`#` 开头是注释。

**选择**（两种方式，显式配置优先级最高）：

```java
// 方式 1：代码里显式指定——强制用这个 id，忽略优先级
new WalBuilder().baseDir(dir).codec("com.example:rot13").build();

// 方式 2：只靠 @Priority，不指定就用优先级最高的
@Priority(60)   // 内置=0，厂商推荐=10~50，用户覆盖=60~100
public final class Rot13Codec implements RecordCodec { ... }
```

仲裁规则（`PluginManager`）：

1. 配置里显式指定了 id → **只考虑该 id**，找不到按 `onPluginLoadFailure` 处理；
2. 否则按 `@Priority` **降序**，同分按 `id()` **字典序升序**（保证同一 classpath 下结果可复现，
   不受 `ServiceLoader` 迭代顺序影响）；
3. `apiVersion() != WalVersion.API_VERSION` 的候选被剔除（记入 `pluginDiagnostics().incompatiblePlugins()`）；
4. `init()` 抛异常的被剔除（记入 `failedPlugins()`），**不污染**已成功的实现；
5. 同 id 同实现类（parent-first 造成的重复可见）静默跳过；同 id 不同类才算真冲突。

**排错**：`wal.stats().activePlugins()` 看实际生效的实现，`wal.pluginManager().diagnostics()`
看被剔除的原因。

### 6.3 完整示例：自定义 `RetentionPolicy`

需求：**"每个段最多留 5 分钟，且分区 0 的段总量不超过 100MB；如果段的 LSN 范围里还有
未持久化的记录，绝对不能删"**。

```java
package com.example.wal;

import com.walcore.spi.Priority;
import com.walcore.spi.RetentionContext;
import com.walcore.spi.RetentionDecision;
import com.walcore.spi.RetentionPolicy;
import com.walcore.spi.SegmentInfo;
import com.walcore.spi.PluginContext;
import com.walcore.spi.PluginException;

import java.util.ArrayList;
import java.util.List;

/**
 * 5 分钟 + 100MB 双维度保留策略（取更严格者）。
 *
 * <p>注意三个"必须"：
 * <ul>
 *   <li>必须有无参构造器（ServiceLoader 靠它实例化）；</li>
 *   <li>{@link #evaluate} 必须只读，且不得阻塞秒级以上；</li>
 *   <li>必须主动排除 {@code lastLsn >= context.minPinnedLsn()} 的段——管理器会兜底再查一次，
 *       但策略自己排除能让 {@code reason} 更准确、日志更好读。</li>
 * </ul>
 */
@Priority(60)
public final class FiveMinuteRetentionPolicy implements RetentionPolicy {

    /** 单分区总大小上限：100MB。 */
    private static final long MAX_BYTES_PER_PARTITION = 100L * 1024 * 1024;
    /** 段最长存活：5 分钟。 */
    private static final long MAX_AGE_MILLIS = 5L * 60L * 1000L;

    private volatile boolean closed;

    @Override
    public String id() {
        return "com.example:retention-five-minute";
    }

    @Override
    public int apiVersion() {
        return com.walcore.api.WalVersion.API_VERSION;
    }

    @Override
    public void init(PluginContext context) throws PluginException {
        // 这里可以做自检（例如配置项是否合理）。抛 PluginException 即视为不可用，
        // 框架会剔除本实现并回落到内置策略（onPluginLoadFailure=FALLBACK_DEFAULT 时）。
        if (MAX_BYTES_PER_PARTITION <= 0L) {
            throw new PluginException("MAX_BYTES_PER_PARTITION 必须为正数");
        }
        context.warn("RETENTION_INIT", "启用 5 分钟保留策略", null);
    }

    @Override
    public RetentionDecision evaluate(RetentionContext context) {
        List<SegmentInfo> segments = context.segments();
        if (segments.isEmpty()) {
            return RetentionDecision.keepAll();
        }
        long now = context.nowMillis();

        // ① 先算"被 pin 住"的边界段下标：从最旧开始，只要还在保护范围内就跳过。
        //    段按 firstLsn 升序，故一旦某段被 pin，它后面的段也一定被 pin。
        int pinnedFrom = segments.size();
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).lastLsn() >= context.minPinnedLsn()) {
                pinnedFrom = i;
                break;
            }
        }
        if (pinnedFrom == 0) {
            return RetentionDecision.keepAll();      // 最旧的段都被 pin 了，没什么可删
        }

        // ② 时间维度：找出第一个"还新鲜"的段，它之前（不含）的都是超龄候选。
        int tooOldEnd = 0;
        while (tooOldEnd < pinnedFrom
                && now - segments.get(tooOldEnd).createdMillis() > MAX_AGE_MILLIS) {
            tooOldEnd++;
        }

        // ③ 大小维度：从最旧开始累加，超出 100MB 之前的部分也是候选。
        long accumulated = 0L;
        int tooBigEnd = 0;
        while (tooBigEnd < pinnedFrom && accumulated <= MAX_BYTES_PER_PARTITION) {
            accumulated += segments.get(tooBigEnd).sizeBytes();
            tooBigEnd++;
        }

        // ④ 取更严格者（并集，而不是交集）：两个维度谁要求删得多就按谁删。
        int deleteEnd = Math.max(tooOldEnd, tooBigEnd);
        if (deleteEnd <= 0) {
            return RetentionDecision.keepAll();
        }
        // ⑤ 恒不删最后一个段：它可能是唯一能续写的位置。
        deleteEnd = Math.min(deleteEnd, segments.size() - 1);
        if (deleteEnd <= 0) {
            return RetentionDecision.keepAll();
        }

        List<SegmentInfo> victims = new ArrayList<>(segments.subList(0, deleteEnd));
        return RetentionDecision.delete(victims,
                "超过 5 分钟或单分区 100MB（时间维度=" + tooOldEnd
                        + " 段，大小维度=" + tooBigEnd
                        + " 段，pinFrom=" + pinnedFrom + "）");
    }

    @Override
    public void onSegmentDeleted(SegmentInfo info) {
        // 删除之后回调：用于更新自己的统计、打点、或写审计日志。
        // 注意这里已经在删除之后，文件已经不在了，不要试图再读它。
    }

    @Override
    public void close() {
        // 必须幂等：热切换失败的路径上可能会关两次。
        this.closed = true;
    }
}
```

注册：

```
# src/main/resources/META-INF/services/com.walcore.spi.RetentionPolicy
com.example.wal.FiveMinuteRetentionPolicy
```

启用：

```java
new WalBuilder()
        .baseDir(dir)
        .retentionEnabled(true)
        .retentionPolicy("com.example:retention-five-minute")   // 省略则按 @Priority 自动选
        .retentionIntervalMillis(30_000L)                       // 30s 评估一次
        .build();
```

### 6.4 ⚠ 插件只允许 `provided` 依赖

```xml
<!-- 插件自己的 pom.xml：必须是 provided，绝不能是 compile -->
<dependency>
  <groupId>com.walcore</groupId>
  <artifactId>wal-core</artifactId>
  <version>1.0.0</version>
  <scope>provided</scope>
</dependency>
```

**为什么这条是硬的**：如果用 shade / assembly 把 `com.walcore.*` 打进插件 JAR，插件类加载器里就有
**第二份** `RecordCodec` 接口。宿主的 `instanceof` 会对插件实例永远是 `false`，
`ServiceLoader` 找到了却"用不了"——而且**不报错**，是最难查的一类问题。

框架会主动拦：`PluginClassLoader` 检测到插件 JAR 内含 `com/walcore/` 的类时**拒绝加载**并给出明确原因。
`PluginClassLoader` 对 `com.walcore.*` 与 `java.*` 强制 **parent-first**，
其余走插件自己的 URLClassLoader。

### 6.5 插件写法的四条约束

1. **无参构造器**：`ServiceLoader` 靠它实例化（可以 `private static final class`，但不能是内部非静态类）。
2. **`apiVersion()` 必须等于 `WalVersion.API_VERSION`**，否则被剔除。
3. **`init()` 抛异常 = 不可用**：要抛 `PluginException` 并在消息里写清缺失的依赖，便于诊断。
4. **`close()` 必须幂等**：热切换失败的路径上可能关两次。

---

## 7. 故障排查表（15 条）

按"现象 → 原因 → 处置"排；对应 `docs/ARCH-wal.md` §14 的失败模式表。

| # | 现象 | 原因 | 处置 |
|---:|---|---|---|
| 1 | `append`/`appendBatch` 抛 `WalException.IoException`（底层写或预分配返回 `ENOSPC`）；`stats().health()==DEGRADED` | 磁盘满 | 清空间。框架**绝不静默丢记录**、**不推进 `durableLsn`**（不假确认）：组提交按退避间隔重试 `force`，任何一次成功即自动回 `HEALTHY`。注意：专用的 `OverloadedException(WAL_DISK_FULL)` 与 `minFreeBytes` 空间预检**当前版本未接线**（异常工厂与配置项已定义、有 Javadoc，但无运行时消费者），磁盘满统一表现为 IO 异常 + 健康度降级 |
| 2 | `flushAndWait` 一律超时，健康度 `DEGRADED` | `force()` 持续失败或设备挂起 | 框架**不推进 `durableLsn`**（宁可不确认也不假确认）；组提交退避重试，成功一次即恢复 `HEALTHY`，确认后可重试同一 LSN。注意：独立的 fsync watchdog（`forceTimeoutMillis`，本表旧版措辞称"默认 10s"）**当前版本未实现**——卡死的 `force` 只能靠 `flushAndWait` 自身的超时暴露，不会主动打断 |
| 3 | 打开时报 `IOException: Too many open files` | 分区过多 + 只读窗口句柄超限 | 常态句柄数 ≈ `2×分区数 + 8`（默认 16 分区 = 40 个）。调小 `partitionCount` 或 `mappedWindowCount` |
| 4 | 进程 OOM（堆内） | 业务侧持有大量 `WalRecord` 未 `copy()` 也未丢弃 | append 路径本身零分配；`nextBatch` 的 `WalRecord` 是**复用**的，要长期持有必须 `record.copy()` |
| 5 | 抛 `OffHeapExhaustedException` | ——（**当前版本不会抛出该异常**） | `offHeapBudgetBytes`（默认 512MB）与 `OffHeapExhaustedException` 已定义但**无运行时消费者**，堆外占用目前由只读窗口的硬上界约束（`mappedWindowCount × mappedWindowBytes`，默认 8×32MB=256MB），用 `stats().offHeapBytes()` 观测。活跃段 mmap 计入**虚拟地址空间**，不计入任何预算。需要预算强制时须自行监控 |
| 6 | 重启后尾部少了若干条记录，`RecoveryReport.truncatedBytes() > 0` | 上次是 `kill -9` 或断电 | **正常行为**。这些记录**从未被 ACK**（`durableLsn` 没推进）。要让它们不丢就必须在回 ACK 前 `flushAndWait` |
| 7 | 断电后重启，最后的记录消失 | 那些记录只在 page cache，没 fsync | 见 #6。这是"口径 A 不承诺持久"的直接后果 |
| 8 | 段文件被外部删除或磁盘换过；段序列 `firstLsn` 出现缺口 | 人为/介质事故 | **当前版本启动时不校验段序列连续性**（`SegmentMissingException` 实际只在快照文件缺失时抛出）：缺口表现为该区间的数据**静默不可读**——游标从下一个现存段继续，不报错、也不会给出缺失区间 `[a,b]`。需要严格性请自备巡检（比对相邻段名 LSN 是否衔接）。处置：从备节点/归档恢复缺失段，或明确接受丢失后清理目录 |
| 9 | 读或恢复时抛 `CorruptedException` / 收到 `CorruptionEvent` | 段文件被外部改写，CRC 不符 | 按 `corruptionPolicy` 处理：`TAIL_TRUNCATE`（默认，仅限尾段）/ `FAIL_FAST`（不动文件）/ `SKIP_RECORD`（4KB 窗口内重同步）。`stats().corruptionCount()` 可告警 |
| 10 | 第二个进程打开同一目录抛 `AlreadyOpenException` | `LOCK` 文件被占用（`FileLock` 独占） | 一个目录只能一个实例。要做多副本请用 `WalTailer` 外接订阅，不要双写同一目录 |
| 11 | 容器里被 OOMKilled，但 JVM 没报 OOM | cgroup 限制与 `Runtime.maxMemory()` 不一致 | 显式设 `-Xmx` 与 `-XX:MaxDirectMemorySize`，建议 `-Xmx512m -XX:MaxDirectMemorySize=768m` |
| 12 | 启动日志有 `WAL_PLUGIN`，`pluginDiagnostics().failedPlugins()` 里有你的插件 | 插件缺依赖类（`NoClassDefFoundError`）或 `init()` 抛异常 | 按提示补齐依赖；确认 `wal-core` 是 `provided` 作用域而不是被打进 JAR（见 §6.4） |
| 13 | `addPartition(5)` 抛 `PartitionException: 分区 5 已 PURGED，禁止复用` | 该分区被 `PURGE` 过（`p0005.state=PURGED` 是 tombstone），默认禁止 ID 复用以免历史数据错认 | 换一个 ID；确实要复用时显式 `PartitionOptions.allowingIdReuse(true)`。配置里声明的分区**会**被重建（"配置即意图"） |
| 14 | 日志里偶发 `timestampNanos` 倒退，但 LSN 顺序正常 | 时钟回拨（NTP 校时） | **不影响正确性**：LSN 与时钟完全解耦（这是选"单一计数器"而非"时间戳拼接"的关键好处）。平台只记 WARN |
| 15 | `flushAndWait` 偶发变慢，但写入不断 | 长时间 GC 停顿把组提交线程饿住（`park` 实测 > 5× 期望） | 写入不依赖组提交线程（数据已在 page cache），所以只是"确认变慢"。查 GC 日志、加 `-XX:MaxGCPauseMillis`、必要时调大 `groupCommitThreads` |

---

## 8. 示例与测试

### 8.1 示例插件（可直接照抄改成自己的）

| 文件 | 演示什么 |
|---|---|
| `src/test/java/com/walcore/examples/spi/CountingCodec.java` | 最小可用的 `RecordCodec`：统计编码字节数，`codecId=128` |
| `src/test/java/com/walcore/examples/spi/Rot13Codec.java` | "真的会改字节"的 `RecordCodec`：演示 `encode`/`decode` 的非对称实现，`codecId=129` |
| `src/test/java/com/walcore/examples/spi/CountBasedIndexStore.java` | 委托内置稀疏索引并统计调用次数，演示 `IndexStore` 装饰器写法 |
| `src/test/java/com/walcore/examples/spi/PrintlnReplicationSink.java` | `ReplicationSink`（`mode()==ASYNC`）演示外部投递与 `replicatedLsn()` 水位回报 |
| `src/test/java/com/walcore/examples/spi/ExamplePluginsTest.java` | 7 个用例：这些插件**真能被 ServiceLoader 发现并生效**（不是只有代码没有验证） |
| `src/test/java/com/walcore/examples/QuickStartSmokeTest.java` | README §1 那段代码的可执行副本 |

### 8.2 跑测试

```bash
mvn -o test       # 全量：功能测试全跑，@Tag("perf") 的性能用例默认排除
mvn -o package    # 产出 target/wal-core-1.0.0.jar（含 6 个 META-INF/services）
```

- **三后端共用一套契约测试**：`SegmentHandleContractTest` + `MultiBackendEndToEndTest`，
  Windows 开发机上也强制把 mmap 后端跑一遍，保证没有"仅 Linux 可运行"的分支。
- **崩溃恢复 41 个用例**：27 种尾部损坏形态 + 9 个策略用例 + 5 个索引重建用例。
- **性能用例**统一 `@Tag("perf")`，见 [§4.5](#45-性能数字怎么自己测)。

---

## 9. 已知限制与非目标

| # | 限制 | 说明 / 缓解 |
|---:|---|---|
| 1 | 性能只在 Linux x86_64 + NVMe 上有意义 | Windows/macOS 走 FileChannel 后端，功能完整但单条 append 慢 2 个数量级。目标是**正确性**而非性能 |
| 2 | 万段级 RTO 未优化 | 恢复期按段串行扫描。`10,000` 段时可能超过 30s。缓解：**调大 `segmentSizeBytes`**（段数 = 总量/段大小） |
| 3 | 无在线复制 | 只交付 `NoopReplicationSink` + 示例，且**当前版本不投递记录**——框架尚无调用 `onRecord` 的复制循环（见 §6.1 的警告块）。生产复制请用 `WalTailer` 自行外接（订阅接口已可用） |
| 4 | 无框架内幂等去重 | 有意为之：框架只把幂等键摘要写进帧头（`AppendOptions.idempotent()` → `record.attributes()` + `hasIdempotentKey()`），**判定交给上层**。1 万 key 的 `HashSet` 会破坏 append 零分配，且"什么算同一条业务记录"是业务语义 |
| 5 | 无在线全量校验 `verify()` | 等价能力是"停机 + 重开"：会逐帧校验 CRC 并产出 `RecoveryReport`。为不给出语义模糊的在线接口，本版本不暴露 |
| 6 | 分位估计是固定桶 | 桶边界 `{1,2,5,10,50,100,500,1000,5000,10000,50000}µs`。尾巴全落同一宽桶时误差可达桶宽 1/2（固有分辨率，非缺陷）。需要精确分位请用 `LatencyBenchIT` 的精确采样路径 |
| 7 | 单进程单实例 | 一个目录一个实例（`LOCK` 保护）。多实例共享目录请用复制，不要双写 |
| 8 | `appendBatch` 固定 `AppendOptions.DEFAULTS` | 批量接口暂只支持 DATA 类型、无幂等键。需要混合类型请逐条 `append` |

**明确的非目标**：SQL / 事务隔离 / 二级索引 / 多机一致性 / 在线 schema 变更 / 数据压缩存储引擎。

---

## 10. 文档索引

| 文档 | 内容 |
|---|---|
| `docs/PRD-wal.md` | 产品需求：10 条 P0 + 6 条 P1，每条带验收标准 |
| `docs/ARCH-wal.md` | 架构设计：帧格式、并发模型、失败模式（§14）、性能预算（§15）、完整文件清单（§17） |
| `docs/TASKS-wal.md` | 20 个实现任务与验收点、8 条铁律、8 个易踩坑 |
| `docs/PERF-wal.md` | 性能基准：环境表格、命令、双口径定义、预算基线、未达标项 |
| `docs/class-diagram.mermaid` / `sequence-diagram.mermaid` | 类图与写入时序图 |

---

版本：`1.0.0`　|　API 版本：`WalVersion.API_VERSION`　|　许可证：见仓库根。
