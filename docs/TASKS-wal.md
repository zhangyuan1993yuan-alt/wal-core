# TASKS：WAL 框架实现任务清单（给工程师寇豆码的执行清单）

| 项目 | 内容 |
|---|---|
| 上游文档 | `docs/ARCH-wal.md`（架构师高见远）+ `docs/PRD-wal.md` |
| 任务总数 | **20 个**，分 **5 个实现批次** |
| 新增文件总量 | **131 个**（93 主代码 + 6 main `META-INF/services` + 22 测试 + 3 测试 services + 7 构建/文档） |
| 技术栈 | Java 17（`maven.compiler.release=17`）、Maven 3.9.9、JUnit 5.10.5（仅 test） |
| 工作目录 | `J:/WiWork/Project/WorkBuddy/wal` |

---

## 0. 开工前必读的 8 条铁律

1. **核心库零第三方运行时依赖**。`mvn dependency:tree -Dscope=compile` 必须输出空。任何"要不要引个库"的念头都视为违规。测试依赖只允许 `org.junit.jupiter:junit-jupiter:5.10.5`（本地仓库已有，含 5.10.0/5.10.2/5.10.5/5.11.4/5.12.2，**统一用 5.10.5**）。
2. **`append()` 路径上禁止**：`new` 对象、`synchronized`、`String` 拼接、日志调用、`fsync`、任何形式的队列。
3. **帧格式常量只有一处真源**：`FrameLayout`。测试直接断言它的值，任何"顺手改一下"都会让 T02/T10 崩。
4. **CRC 覆盖区间必须精确**：`[0,20) ∪ [24, frameLen)`，跳过 crc 字段自身。写错一次，恢复测试会以奇怪的方式失败。
5. **`lsn` 分配必须在分区写锁内**。出了锁再分配，同分区文件顺序就可能与 LSN 顺序不一致，P0-01 ① 直接失败且极难 debug。
6. **Windows 必须能跑通全部测试**。开发机是 Windows，Linux 上的 mmap 优化不能成为"仅 Linux 可编译"的代码。三后端共用一套契约测试。
7. **不要写方法体到本清单**。本清单只给"要做什么 + 验收点"，接口签名以 `ARCH-wal.md` 为准，逐字照抄（很多签名细节是有意为之，比如 `decode` 的 5 个参数）。
8. **每完成一个任务立即 `mvn -q test`**。这个项目里 80% 的 bug 会以"帧偏移差 4 字节"或"CRC 算错 1 字节"的形式出现，晚发现成本翻倍。

---

## 1. 建议的实现批次（并行/串行安排）

### 批次总览

| 批次 | 主题 | 任务 | 并行度 | 建议工期占比 |
|---|---|---|---|---|
| **B1** | 地基 | **T01** | 串行（唯一总前置） | 10% |
| **B2** | 三条独立支线 | **T02** ∥ **T03** ∥ **T04** | **3 路可并行**（互不依赖） | 20% |
| **B3** | 存储与索引 | **T05** ∥ **T06**、**T07** | **3 路可并行**（T05 稍晚于 T03；T06 依赖 T05；T07 只依赖 T02+T04，可最早开工） | 25% |
| **B4** | 主链路集成 | **T08** · **T09** · **T10** · **T11** | T08/T09/T10 三路可并行；**T11 完全独立**（只依赖 T01），可提前到 B2 就开始 | 25% |
| **B5** | 增补与交付 | **T12**~**T17** 可并行；**T18** → **T19** → **T20** 严格串行 | 6 路并行 + 3 步串行收尾 | 20% |

### 关键路径（决定总工期）

```
T01 → T03 → T05 → T08 → T12 → T18 → T19 → T20
                              ↑
              T02 → T07 ──────┘
              T05 → T06 → T10 ┘
```

- **关键路径长度 = 8 个任务**，其中 `T03`(L)、`T05`(L)、`T08`(L)、`T18`(L) 四个 L 级任务是工期风险点。
- **可提前启动的"偷跑"任务**：`T11`（SPI 插件框架）只依赖 T01，且与所有业务代码解耦，**建议第 2 天就并行开工**，不要等到 B4。
- **可推迟的"非阻塞"任务**：`T15`（快照/checkpoint）、`T17`（指标）、`T19`（性能基准）不影响 P0 十条需求的功能完整性，若工期紧张可放到最后一轮。

### 并行冲突预警

| 风险 | 说明 | 规避 |
|---|---|---|
| `DefaultWal.java` 被 T08/T12/T16 三个任务同时修改 | 单文件多方修改，合并冲突高 | T08 一次性把 `DefaultWal` 的**全部方法骨架**（含 `close`/`addPartition`/`stats` 的空实现或抛 `UnsupportedOperationException`）写完；T12/T16 只补具体方法体，不改结构 |
| `Segment.java` 被 T05/T06/T07 同时碰 | `Segment` 同时是存储和索引的宿主 | `Segment` 在 T05 一次定型：只暴露 `appendFrame/reserve/publishPosition/force/sealToReadOnly/hasRoom` + `index()` 访问器；T06 只在 `Segment` 之外实现 `SparseIndex`，T07 只读 `Segment` 的 `handle()` |
| `pom.xml` 被 T18 修改 | surefire 配置 | T01 就一次性配好 surefire 3.2.5，T18 不再动 pom |
| `WalConfig.java` 被几乎所有任务需要新参数 | 配置项散落 | T01 按 ARCH §17 的完整配置项**一次性写全**（含尚未使用的字段），后续任务只读不写 |

---

## 2. 任务明细表

> 复杂度：**S** = 半天内 / **M** = 1~2 天 / **L** = 3~5 天
> 涉及文件路径省略统一前缀 `src/main/java/com/walcore/`，测试省略 `src/test/java/com/walcore/`

| # | 任务名 | 涉及文件 | 依赖 | 复杂度 | 验收点 |
|---|---|---|---|---|---|
| **T01** | **项目地基**：Maven 构建、帧格式常量、异常族、配置体系、平台探测、线程工厂 | `../../../../pom.xml`, `README.md`, `.gitignore`, `internal/append/FrameLayout.java`, `api/WalException.java`, `api/WalConfig.java`, `api/WalBuilder.java`, `internal/util/Preconditions.java`, `internal/util/Platform.java`, `internal/util/NamedThreadFactory.java`；测试：`append/FrameLayoutTest.java` | — | **M** | ① `mvn -q compile` 通过且无 warning（`-Xlint:all` 下仅允许 serial 警告）；② **`mvn dependency:tree -Dscope=compile` 输出为空**（硬性）；③ `FrameLayout` 的 14 个常量与 ARCH §4.1 逐项相等，`FrameLayoutTest` 断言通过（含 `MAX_FRAME_LEN=16_777_288`）；④ `WalConfig` 全部默认值与 ARCH §3.5/§7.2/§6.1 一致（`segmentSizeBytes=268435456`、`batchIntervalMicros=200`、`batchMaxRecords=4096`、`batchMaxBytes=1048576`、`groupCommitThreads=2`、`indexIntervalRecords=1000`、`maxPayloadBytes=16777216`、`maxPartitions=4096`、`partitionCount=16`）；⑤ `WalException` 15 个嵌套子类各有稳定 `errorCode()`（如 `WAL_CLOSED`/`WAL_OVERLOADED`/`WAL_CORRUPTED`/`WAL_DISK_FULL`/`WAL_TIMEOUT`/`WAL_PLUGIN`/`WAL_SEGMENT_MISSING`）；⑥ `Platform.isLinuxX86_64()` 在 Windows 开发机返回 false |
| **T02** | **帧格式编解码 + CRC32C + 前向兼容** | `spi/RecordCodec.java`, `internal/spiimpl/RawRecordCodec.java`, `internal/append/FrameEncoder.java`, `internal/read/FrameDecoder.java`, `internal/util/Crc32cUtil.java`, `internal/io/MemoryUtils.java`, `api/WalRecord.java`, `api/AppendOptions.java`；测试：`append/FrameEncoderDecoderTest.java` | T01 | **M** | ① 128B / 256B / 1KB / 4KB / 16MB 五种 payload 编解码往返**字节级完全一致**；② `frameLen` 恒为 8 的倍数、`frameLen == 64 + storedLen + padding`；③ CRC 覆盖 `[0,20) ∪ [24,frameLen)`——**逐一改写 0..frameLen-1 中除 [20,24) 外任意一字节，解码必失败**（抽样 100 个位置）；④ `headerLen=96` 的伪造帧被正确跳过（模拟未来版本）；⑤ `recordType=200` 且无处理器 → 解码器**跳过整帧、不抛异常、不报损坏**；⑥ `flags` bit6/bit7 置位时解码不失败；⑦ `MemoryUtils` 的 `int64` 读写只用 `VarHandle`/`ByteBuffer` 原生方法，**无 `sun.misc.Unsafe.putLong` 直调**（unmap 除外） |
| **T03** | **IO 抽象层与三种后端**（mmap / FileChannel / Heap） | `spi/StorageBackend.java`, `spi/SegmentHandle.java`, `spi/StorageConfig.java`, `internal/io/StorageBackendFactory.java`, `internal/io/MappedStorageBackend.java`, `internal/io/MappedSegmentHandle.java`, `internal/io/FileChannelStorageBackend.java`, `internal/io/FileChannelSegmentHandle.java`, `internal/io/HeapStorageBackend.java`, `internal/io/HeapSegmentHandle.java`, `internal/io/MappedWindowCache.java`；测试：`io/SegmentHandleContractTest.java`（**三后端参数化跑同一套**）、`io/MappedWindowCacheTest.java` | T01 | **L** | ① 三后端通过**同一份** `SegmentHandleContractTest`：`create/open/append/read/force/truncate/physicalLength/appendPosition/isMapped/close` 全绿；② 三后端行为一致：连续 append 后 `appendPosition == Σ frameLen`，`read` 逐字节等于写入内容；③ `Platform.isLinuxX86_64()==true` 时默认选 `Mapped`，否则默认 `FileChannel`（工厂单测用 mock Platform 覆盖两分支）；④ `MappedSegmentHandle.force()` 实现走 `FileChannel.force(false)`，**不是** `MappedByteBuffer.force()`（代码审查确认）；⑤ `MappedWindowCache` 打开的文件句柄数**永不超 `windowCount=8`**（测试用计数器断言 + LRU 逐出正确）；⑥ `MappedStorageBackend` 在 Windows 上 close 后文件可被删除（`MemoryUtils.unmap` 生效，`Files.delete` 不抛 `AccessDeniedException`）；⑦ `HeapStorageBackend` 无文件系统副作用，可完全在内存运行 |
| **T04** | **LSN 分配器 + 分区运行时 + 分区注册表** | `internal/append/LsnAllocator.java`, `internal/partition/PartitionImpl.java`, `internal/partition/PartitionRegistry.java`, `api/PartitionInfo.java`, `api/PartitionOptions.java`, `api/RemoveMode.java`；测试：`append/LsnAllocatorTest.java`、`partition/PartitionRegistryTest.java` | T01 | **M** | ① 16 线程 × 100 万次 `allocate()` → 得到 1600 万个值，**排序后严格连续、首尾差 = 1600万-1、无重复**；② `LsnAllocator` 初值可注入（`new LsnAllocator(firstLsn)`），`peek()` 不消耗；③ `PartitionRegistry.get(pid)` 读路径**零锁零 volatile 读放大**（审查 `volatile PartitionImpl[] slots` + 单次 `loadAcquire`）；④ `register/unregister` 用数组 COW，并发读期间不抛 `ArrayIndexOutOfBounds`；⑤ `PartitionImpl` 的 `publishedLsn`/`durableLsn` 用 `VarHandle` release/acquire，10 万次跨线程读写压测无丢失更新；⑥ `PartitionImpl.state` 迁移 `ACTIVE → FROZEN → PURGED` 单向且不可逆 |
| **T05** | **段管理与零阻塞轮转 + 预建线程** | `internal/segment/SegmentManager.java`, `internal/segment/Segment.java`, `internal/segment/SegmentNaming.java`, `internal/segment/NextSegmentPreparer.java`, `internal/util/MetaFile.java`；测试：`segment/SegmentNamingTest.java`、`segment/SegmentRotationTest.java` | T02, T03, T04 | **L** | ① `SegmentNaming`：`wal-p0003-00000000000000131072.wal` 的字典序与数值序一致（对 100 个随机 firstLsn 断言）；非法文件名（负号/位数不对/分区越界）解析返回 `Optional.empty()`；② 写满 `segmentSizeBytes` 自动轮转，旧段状态 `READONLY` 且 `publishedWritePos` 被 `finalLength` 冻结，之后任何 `append` 抛 `IllegalStateException`；③ **轮转延迟：`NextSegmentPreparer` 命中时 p99 < 50µs，1 万次轮转中 fallback 次数为 0**（预热完成后）；④ `remaining < frameLen` 时轮转而**不拆帧**（断言不跨段）；⑤ `.tmp` → `ATOMIC_MOVE` 重命名，且创建后对父目录做 `force(true)`（Linux 上验证，Windows 上 no-op 不报错）；⑥ `wal.meta` 写入含 CRC 且用"临时文件 + ATOMIC_MOVE"原子替换，读到半截 meta 时能识别并重建；⑦ `findSegmentFor(lsn)` 在 1000 个段上二分定位 p99 ≤ 1µs |
| **T06** | **稀疏索引**（16B 索引项 + 堆内二分 + 顺序扫描兜底） | `spi/IndexStore.java`, `spi/IndexEntry.java`, `spi/IndexHit.java`, `internal/segment/SparseIndex.java`, `internal/spiimpl/DefaultIndexStore.java`；测试：`segment/SparseIndexTest.java` | T05 | **M** | ① 100 万条记录的段 → 恰好 1000 条索引项，堆内占用 ≈ 16KB（`long[]×2`）；② `lookup(任意 lsn)` 返回 `≤ lsn` 的最近索引项，**p99 ≤ 10µs**（预热后 1 万次测量）；③ `lsn` 小于段内首条索引项时返回 `IndexHit.NONE` 并触发"从段首扫"兜底，结果正确；④ 索引项被人为删掉中间一条时，兜底顺序扫描仍能定位（不抛异常）；⑤ `.idx` 文件 16B/项且头 16B 含 magic+firstLsn+itemCount+crc，头 CRC 错时 `openSegment` 返回需重建信号；⑥ `truncateAfter(watermark)` 后二分不越界、`lastIndexedLsn()` 正确；⑦ `memoryBytes()` 计入 512MB 预算（超过 `maxIndexHeapBytes=192MB` 时降级为纯文件二分而非 OOM） |
| **T07** | **组提交管线**（三重攒批 + 三档 fsync 策略 + 失败重试 + `flushAndWait`） | `internal/append/DirtyTracker.java`, `internal/groupcommit/GroupCommitCoordinator.java`, `internal/groupcommit/FlushBatch.java`, `internal/groupcommit/FlushTicket.java`, `internal/append/StagingBuffer.java`；测试：`groupcommit/GroupCommitTest.java`、`groupcommit/FlushTicketTest.java` | T02, T04 | **L** | ① **三重触发**各自可独立验证：`batchIntervalMicros=200` 只靠时间触发；`batchMaxRecords` 达阈立即触发；`batchMaxBytes` 达阈立即触发（三者用 mock `SegmentHandle` 计数 `force` 次数断言）；② `ALWAYS` 每次 append 都 `force`；`BATCH` 按批；`PERIODIC` 只在 `flushIntervalMillis` 到期时（三档行为差异用时间轴断言）；③ **`BATCH` 模式下写入线程从不调用 `force()`**（mock 记录调用线程名断言）；④ `flushAndWait` 快路径（`durableLsn≥lsn`）**零 `force` 调用、零 `park`**；慢路径能正确唤醒，10 万次等待无丢失唤醒（无超时）；⑤ `force()` 抛 `IOException` → 置 `health=DEGRADED`、**不推进 `durableLsn`**、所有等待者收到 `IoException`、5s 后自动重试；⑥ `dirtyMask` 用 `getAndSet(0)` 原子取走，4096 分区（64 word）下无丢失；⑦ `FlushTicket` 对象池化（1 万次 `flushAndWait` 的分配字节数 < 1MB）；⑧ 组提交线程 `Throwable` 后自动重启（最多 3 次），之后置 `FAILED` 且 `wal.stats().health()` 可见 |
| **T08** | **写入管线集成**（`append` / `appendSync` / `appendBatch` + 背压 + 端到端打通） | `api/Wal.java`, `api/WalStats.java`, `internal/DefaultWal.java`（**一次性写全所有方法骨架**）, `internal/append/AppendPipeline.java`；测试：`append/AppendPipelineTest.java`、`append/ConcurrentAppendTest.java` | T05, T07 | **L** | ① `append` 返回全局唯一递增 LSN；**同分区并发 8 线程写入后，文件内 LSN 序列严格递增**（关键不变量，必须专测）；② 全分区视角 LSN 连续无空洞（写 1000 万条后断言 `max-last` 差 = 条数-1）；③ 16 分区 × 1 线程吞吐相对单分区衰减 **≤ 10%**（在 Windows 开发机用相对值断言，绝对值在 T19 测）；④ 背压三场景（staging 满 / `dirtyBytes>32MB` / 段未就绪）分别抛 `BackpressureException` 或在超时内阻塞后成功，行为符合 ARCH §3.5；⑤ `append()` 路径**零对象分配**（用 `com.sun.management.ThreadMXBean.getThreadAllocatedBytes` 断言 10 万次循环分配 < 100KB）；⑥ `appendBatch` 返回 LSN 数组且元素连续；⑦ `Wal` 全部方法（含 `close`/`addPartition`/`stats`/`pluginManager`）编译通过，未实现者抛 `UnsupportedOperationException` 并带清晰消息 |
| **T09** | **游标与顺序读**（seek / next / nextBatch / 断点续读） | `api/WalCursor.java`, `api/WalRecordVisitor.java`, `api/CursorOptions.java`, `internal/read/CursorImpl.java`, `internal/read/CursorCheckpointStore.java`；测试：`read/CursorTest.java` | T05, T06 | **M** | ① 从任意 LSN `seekTo` 正确：覆盖 `{1, 段首, 段首-1, 段中, 段尾, 段尾+1, 最后一条, 最后一条+1, 越界}` 九类位置；② 跨段连续读 300 万条**无重复无遗漏**（维护集合断言）；③ 游标重启后从 checkpoint 续读，断点后的记录一条不少一条不重；④ `nextBatch(1000, visitor)` 与逐条 `next()` 结果完全一致且 visitor 复用的 `WalRecord` 不被误持有（断言引用被下一批覆盖）；⑤ 顺序读吞吐 ≥ 300MB/s（Linux NVMe，T19 复测）或开发机上相对基线合理；⑥ `checkpoint` 文件用"临时文件 + ATOMIC_MOVE"原子写，读到半截内容时忽略并用上一条有效值 |
| **T10** | **崩溃恢复、撕裂写检测、索引重建、重放回调** | `internal/recovery/RecoveryManager.java`, `internal/recovery/SegmentScanner.java`, `internal/recovery/TornWriteDetector.java`, `internal/recovery/IndexRebuilder.java`, `api/ReplayListener.java`, `api/RecoveryReport.java`；测试：`recovery/RecoveryTornWriteTest.java`（**≥20 形态**）、`recovery/RecoveryCorruptionPolicyTest.java`、`recovery/IndexRebuildTest.java` | T05, T06 | **L** | ① **≥ 20 种尾部损坏形态全部正确截断**：截断 1/8/63/64/65 字节；追加 1/4/64/256/4096 字节随机垃圾；追加 4096 字节 0x00；magic 被改；`frameLen` 被改为 0/负数/超上限；`crc32c` 被翻转 1 bit；payload 中部翻转 1 bit；payload 全零；两帧粘连后半帧——每种都验证"截断到最近完整帧 + 后续可继续 append + LSN 连续"；② **ARCH §9.2 的五条规则各至少有 4 个用例命中**；③ 三档 `CorruptionPolicy` 行为差异正确（`TAIL_TRUNCATE` 截断 / `FAIL_FAST` 抛异常且**文件未被修改** / `SKIP_RECORD` 在 4KB 窗口内重同步并产出 `CorruptionEvent`）；④ **非最后段损坏 → 抛 `SegmentMissingException`/`CorruptedException`，绝不静默截断**（专测）；⑤ `IndexRebuilder` 重建结果与原 `.idx` **逐项相等**（1000 项全比）；⑥ 多分区恢复时重放回调按 **LSN 全局升序**（收集全部 LSN 断言有序）；⑦ `onProgress` 每 10 万条回调一次；⑧ 恢复后 `nextLsn = max(lastLsn)+1` 且各分区 `appendPosition == lastGoodOffset` |
| **T11** | **SPI 插件框架**（发现 / 优先级仲裁 / 类加载隔离 / 热切换 / 失败降级） | `spi/WalPlugin.java`, `spi/PluginContext.java`, `spi/Priority.java`, `spi/PluginException.java`, `internal/plugin/PluginManager.java`, `internal/plugin/PluginClassLoader.java`, `internal/plugin/PluginDiagnostics.java`, `internal/plugin/PluginType.java`；测试：`spi/PluginLoadingTest.java` | T01 | **L** | ① 5 类 SPI（codec/storage/index/retention/replication）的 `ServiceLoader` 发现全部生效（用测试期 `META-INF/services` 提供 2 个候选）；② `@Priority` 高者胜、同分按 `id()` 字典序、`WalConfig` 显式指定 `wal.spi.codec=xxx` 时**强制**该实现；③ 重复 `id` 首个胜出并记 `diagnostics.duplicateIds`；④ `apiVersion != WalVersion.API_VERSION` 的候选被剔除并记 `incompatiblePlugins`；⑤ `init()` 抛异常被剔除并记 `failedPlugins`，**不污染**已成功者；⑥ `FALLBACK_DEFAULT`（默认）用内置实现且记指标 `wal.plugin.fallback`；`FAIL_FAST` 抛出异常，**消息必须含：接口全名 + 已尝试的 ClassLoader 列表 + 期望的 provider 文件名 + 已发现的 id 列表**（P0-09 ③ 强制）；⑦ `PluginClassLoader` 对 `com.walcore.spi.*`/`com.walcore.api.*` **强制 parent-first**（用双份同名类检测 `ClassCastException` 不发生）；⑧ 插件 JAR 内含 `com/walcore/spi/RecordCodec.class` 时**拒绝加载**并给出明确原因（shade 踩坑防护）；⑨ `swap` 成功时新旧实例正确交接；`swap` 失败（新实例 init 抛异常）**自动回滚且服务不中断**；⑩ `DRAIN_THEN_CLOSE` 下旧实例在引用计数归零后才 `close` |
| **T12** | **生命周期与优雅关闭** | `internal/DefaultWal.java`（补全 `close` 系列 + 打开/恢复编排）, `api/CloseReport.java`；测试：`lifecycle/CloseFlushTest.java`、`lifecycle/ReopenTest.java` | T08 | **M** | ① `close()` 后**立即** kill -9 → 重开 100% 记录可读且 CRC 正确（P0-10 ①）；② `close()` 期间及之后 `append` 抛 `WalException.ClosedException`，`errorCode()=="WAL_CLOSED"`（P0-10 ②）；③ `close(Duration)` 超时返回 `CloseReport(clean=false)` 并列出未刷盘分区与最大 LSN，**不抛异常**；④ `close()` 两阶段顺序正确：先 `force(true)` 各活跃段 → 再写 `wal.meta(cleanShutdown=true)` → 再释放 `LOCK`（用文件状态断言）；⑤ `LOCK` 释放后第二个实例可打开；未释放时第二个实例抛 `AlreadyOpenException`；⑥ `cleanShutdown=true` 时重启**跳过逐帧 CRC**（RTO < 100ms，用一个 256MB 段测）；⑦ `close()` 幂等（连续调用两次不抛异常） |
| **T13** | **实时 Tailer 订阅** | `api/WalTailer.java`, `api/TailHandler.java`, `api/TailerOptions.java`, `internal/read/TailerImpl.java`；测试：`read/TailerTest.java` | T08 | **M** | ① 同进程订阅延迟 **p99 ≤ 200µs**（写入线程记录 `timestampNanos`，订阅回调时刻比对，1 万样本）；② 从历史 LSN 起：先追历史（速度 ≈ 顺序读）→ 追平后转实时，**切换点无空隙无重复**（用连续 LSN 集合断言）；③ 三种溢出策略生效：`DISCONNECT`（回调 `onOverflow` 并关闭）、`DROP_TO_LATEST`（跳到最新并记 `wal.tailer.dropped`）、`BLOCK`（背压到写入方）；④ 慢订阅者（回调 sleep 1ms）时**写入吞吐下降 < 5%**；⑤ `lagRecords()` 单调合理（追平后为 0）；⑥ `close()` 后不再回调且线程干净退出（无泄漏线程，用 `Thread.activeCount` 或线程名扫描断言） |
| **T14** | **保留策略与安全删除** | `spi/RetentionPolicy.java`, `spi/RetentionContext.java`, `spi/RetentionDecision.java`, `spi/SegmentInfo.java`, `internal/spiimpl/TimeSizeRetentionPolicy.java`, `internal/retention/RetentionManager.java`；测试：`retention/RetentionTest.java` | T05 | **M** | ① 时间（7 天）/ 大小（500GB）双维度**取更严格**（构造两种冲突场景断言）；② `minPinnedLsn` 保护生效：有活跃 tailer/cursor 时不删其之后的段；`force=true` 时忽略保护并记 WARN；③ 删除期间写入吞吐下降 < 5%，且不抛异常；④ 段 `readerCount>0` 时本轮跳过、下轮重试（用慢游标构造场景）；⑤ `onSegmentDeleted` 回调被调用且参数正确；⑥ 替换为自定义 `RetentionPolicy` 插件时生效（与 T11 联动）；⑦ 删段后 `physicalLength` 统计与 `wal.stats().totalLogBytes()` 一致 |
| **T15** | **快照与 checkpoint 联动** | `spi/SnapshotStore.java`, `spi/SnapshotMeta.java`, `internal/snapshot/FileSnapshotStore.java`, `internal/snapshot/CheckpointManager.java`；测试：`snapshot/SnapshotTest.java` | T05, T14 | **M** | ① 打快照 → 删除快照点之前的段 → 读 `lsn ≥ 快照点` 完全不受影响；② 恢复时"最近快照 + 之后日志"的重放条数相比全量减少 **≥ 80%**（构造 10 段日志，快照在第 9 段首）；③ SHA-256 校验失败时拒绝加载并抛出含期望/实际摘要的明确异常；④ `pinnedLsn(pid)` = 最近快照 `uptoLsn`，正确阻止 T14 删除该点之后的段；⑤ `write` 用"临时文件 + ATOMIC_MOVE"，中断后不留下半截快照；⑥ `list()` 按 `uptoLsn` 升序，`deleteBefore` 精确删除 |
| **T16** | **动态增删分区** | `internal/partition/PartitionRegistry.java`（补全）, `internal/partition/PartitionImpl.java`（补全）, `internal/DefaultWal.java`（补 `addPartition`/`removePartition` 方法体）；测试：`partition/DynamicPartitionTest.java` | T04, T08 | **M** | ① 运行期 `addPartition()` 立即可写，首条记录 LSN 正确且全局单调性不破（插入后整体 LSN 序列仍严格递增）；② `addPartition(4096)` 抛 `PartitionException`（越界）；重复 id 抛异常；③ `FREEZE` 后写入抛 `PartitionException`，**已有游标可继续读到段尾**；④ `PURGE` 后目录被删、`p.XXXX.state=PURGED`、`wal.meta.purgedPartitions` 有记录；`allowPartitionIdReuse=false` 时重新 `addPartition(同 id)` 抛异常；⑤ `ARCHIVE` 模式下 `RetentionPolicy` 被通知；⑥ 扩容/缩容**期间**其他分区吞吐衰减 < 5%；⑦ `activeIds()` 与实际存活分区一致；⑧ 重启后动态加的分区被正确恢复（`wal.meta.partitionIds` 持久化） |
| **T17** | **指标埋点**（零依赖 + 可选外抛桥接） | `internal/metrics/MetricsRegistry.java`, `internal/metrics/LatencyHistogram.java`, `internal/metrics/MeterSink.java`, `internal/metrics/NoopMeterSink.java`, `api/WalStats.java`（补全）；测试：`metrics/MetricsTest.java` | T08 | **M** | ① 覆盖 PRD P1-05 六项：写入吞吐（条/s + 字节/s）、`append` 与 `flushAndWait` 的 p50/p99/p999、组提交批大小分布、日志总大小、fsync 耗时、健康度；② 直方图桶边界固定：`{1,2,5,10,50,100,500,1000,5000,10000,50000} µs`，分位数计算误差 < 5%；③ **埋点开销 < 总写入耗时 1%**（同一压测关/开指标各跑一轮比对）；④ `MeterSink` 接口可被外部实现（测试里写一个内存 sink 断言收到回调）；⑤ 指标名称/单位有 Javadoc，`wal.stats()` 能一次性取到全部快照；⑥ 指标读路径无锁（`LongAdder` 语义或原子累加），不影响写入 |
| **T18** | **完整测试套件与覆盖率达标** | 补齐 `src/test/java` 至 22 个文件（含 `it/ThroughputIT`、`it/LatencyBenchIT`、`it/RecoveryKill9IT` 的骨架） | T09, T10, T12, T13, T14, T15, T16, T17 | **L** | ① `mvn test` 全绿（Windows 开发机）；② 核心模块行覆盖率 **≥ 80%**（`jacoco` 可选，若无 jacoco 则用 surefire 报告 + 人工核对清单）；③ **崩溃恢复用例 ≥ 30 个**（T10 的 20+ 形态 + 策略 + 索引重建 + 多分区）；④ 三后端 × 全部契约测试都跑通；⑤ 无 `@Disabled` 遗留的解释性测试（性能测试用 `@Tag("perf")` 且默认跳过，需显式 `-Dgroups=perf`）；⑥ 测试**不依赖网络、不依赖外部路径**，全部用 `@TempDir` |
| **T19** | **性能基准与达标验证** | `it/ThroughputIT.java`, `it/LatencyBenchIT.java`, `it/RecoveryKill9IT.java`, `docs/PERF-wal.md`（新建） | T18 | **M** | ① `append()` p50 ≤ 5µs、p99 ≤ 100µs、p999 ≤ 1ms（输出实测分位表）；② **单分区 ≥ 60 万 TPS、16 分区 ≥ 100 万 TPS**（256B/条，`BATCH`，Linux NVMe）；③ `flushAndWait` 单独报告实测 p50/p99（与 `append` 口径分开，见 ARCH §15.2 的诚实说明）；④ 顺序读 ≥ 300MB/s；⑤ RTO ≤ 30s；⑥ `kill -9` 注入（Linux 用 `kill -9`，Windows 用 `taskkill /F` 子进程）后**零已确认数据丢失**；⑦ 结果写入 `docs/PERF-wal.md`，含环境、命令、原始数据、结论、未达标项与原因 |
| **T20** | **文档、示例与交付** | `README.md`（补全）, `src/test/java/com/walcore/examples/spi/Lz4LikeCodec.java`, `.../CountBasedIndexStore.java`, `.../PrintlnReplicationSink.java`, `spi/ExamplePluginsTest.java`, `src/main/resources/META-INF/services/*`（6 个文件）, `src/test/resources/META-INF/services/*` | T11, T19 | **M** | ① README 含：5 分钟上手（10 行代码示例）、架构一图流、**性能口径说明（`append` vs `flushAndWait` 两个口径分开写，明确"100 万 TPS"与"100µs 持久确认"不可同时达成及原因）**、SPI 扩展示例（三类）、故障排查（15 条失败模式对应表）；② 每类 SPI 有 1 个内置默认实现 + 1 个可运行示例第三方实现（P0-09 ①）；③ `mvn -q package` 产出 JAR，JAR 内 `META-INF/services/` 6 个文件齐全且内容正确；④ `java -jar` 不需要（纯库）；⑤ `mvn dependency:tree -Dscope=compile` 仍为空（最终守门）；⑥ `.gitignore` 覆盖 `target/`、`*.wal`、`*.idx`、IDE 目录 |

---

## 3. 任务 → PRD 需求对照（保证不漏需求）

| PRD 编号 | 主要任务 | 辅助任务 |
|---|---|---|
| WAL-P0-01 追加写入 API | T08 | T01(常量) T02(帧) T04(LSN) |
| WAL-P0-02 分段轮转 | T05 | T03(后端) |
| WAL-P0-03 CRC 校验 | T02 | T06(索引无关) |
| WAL-P0-04 组提交 + fsync 策略 | T07 | T01(配置) T17(指标) |
| WAL-P0-05 崩溃恢复 + 尾部截断 | T10 | T05(段) T06(索引) T12(meta) |
| WAL-P0-06 顺序读 / 游标 | T09 | T06(索引) |
| WAL-P0-07 稀疏索引 | T06 | T09 |
| WAL-P0-08 并发多分区 | T04, T08 | T07(组提交) |
| WAL-P0-09 SPI 扩展点（5 类） | T11 | T02(codec) T03(storage) T06(index) T14(retention) T20(示例) |
| WAL-P0-10 优雅关闭 flush | T12 | T07 |
| WAL-P1-01 实时 tail 订阅 | T13 | T07 |
| WAL-P1-02 快照 + 截断 | T15 | T14 |
| WAL-P1-03 保留策略 | T14 | T15(pinnedLsn) |
| WAL-P1-04 动态增删分区 | T16 | T04 |
| WAL-P1-05 指标埋点 | T17 | T20(文档) |
| WAL-P1-06 幂等去重 | **T08 的 `attributes` 字段 + T02 的 `flags.IDEMPOTENT_KEY`** | 注：默认关闭；去重的**判定**由上层用 `attributes` 做，框架只负责"把 key 存进帧、重放时回吐"（避免框架沾业务逻辑）。若需要框架内去重索引，作为 P1 的可选增量，在 T08 完成后单独评估 |

> **WAL-P1-06 的实现边界（重要，避免工程师做多）**：框架提供 `AppendOptions.idempotentKeyHash(long)` → 存进 `attributes`；`FrameDecoder` 回吐到 `WalRecord.attributes()`；`flags.IDEMPOTENT_KEY` 置位。**框架不建去重 HashSet**——因为"什么算同一条业务记录"是业务语义，且 1 万 key 的 HashSet 会破坏"append 零分配"。此边界需在 README 中写明。

---

## 4. 工程师最容易踩的 8 个坑（按踩坑概率排序）

| # | 坑 | 现象 | 正确做法 |
|---|---|---|---|
| 1 | **CRC 覆盖区间算错** | 编码/解码单测能过，但恢复测试随机失败 | CRC 覆盖 `[0,20) ∪ [24,frameLen)`。计算时先写 `crc32c=0` 占位，算完回填，**不要**把 crc 字段算进去。`Crc32cUtil` 提供 `compute(addr, frameLen)` 单一入口，禁止各处自己拼 |
| 2 | **LSN 在锁外分配** | 单线程测试全绿，并发测试偶发"读到的 LSN 逆序" | `allocate()` 必须**在分区写锁内**、且**紧邻**写帧之后。T08 必须写一条专测：8 线程 × 10 万条写同一分区，读回后断言 LSN 严格递增 |
| 3 | **`headerLen` 写死在解码器里** | 未来加字段时，老数据读不出来 | 解码器所有偏移都从 `FrameLayout` 常量取；遇到 `headerLen > 64` 时用 `headerLen` 跳过未知区域，**不得**直接用 64 |
| 4 | **mmap 段未 unmap 就删文件（Windows 必炸）** | 测试留下无法删除的 `*.wal`，CI 磁盘越跑越满 | `MappedSegmentHandle.close()` 必须调 `MemoryUtils.unmap(buffer)`；取不到 `Unsafe.invokeCleaner` 时 `System.gc()` + 重试 3 次；仍失败把路径丢进 `pendingDelete`，下次 `open()` 清理。T03 ⑥ 强制验收 |
| 5 | **`MappedSegmentHandle.force()` 用成 `MappedByteBuffer.force()`** | Linux 上看似正常，Windows 上 `force` 语义不全 + 与 `FileChannel` 混用后水位错乱 | `force()` 一律走 `fileChannel.force(false)`；mmap 只用于**写数据**，不用于刷盘 |
| 6 | **插件 JAR 用 shade 把 `com.walcore.*` 打进去** | `instanceof` 永远 false，`ServiceLoader` 找不到但也不报错 | `PluginClassLoader` 检测到插件含 `com/walcore/` 就**拒绝加载**（T11 ⑧）；README 明确写"插件只能 `provided` 依赖 wal-core" |
| 7 | **`flushAndWait` 用假 fsync 省事** | 吞吐"超标"，但 kill -9 测试丢数据 | 禁止"只 force 一个分区假装覆盖全部"。ARCH §7.3 已说明为什么必须按分区 force，以及为什么性能仍可接受 |
| 8 | **`Segment` 类被三个任务同时改** | 合并冲突 + 状态机错乱 | T05 一次定型 `Segment` 的公开方法集（6 个方法 + `handle()`/`index()` 访问器），后续任务**只调用不修改** |

---

## 5. 完成定义（DoD，全部满足才算交付）

- [ ] `mvn -q clean package` 成功，产出 `wal-core-<version>.jar`
- [ ] `mvn dependency:tree -Dscope=compile` **输出为空**
- [ ] `mvn test` 全绿，核心模块行覆盖率 ≥ 80%，崩溃恢复用例 ≥ 30
- [ ] PRD 十条 P0 需求的验收标准**逐条有对应测试**（见 §3 对照表）
- [ ] `docs/PERF-wal.md` 有实测分位与 TPS，未达标项写明原因
- [ ] README 含 5 分钟上手 + 性能口径说明（两个口径分开）
- [ ] 三后端（mmap / FileChannel / Heap）全部通过契约测试
- [ ] Windows 开发机可完整跑通 `mvn test`（Linux 性能指标不要求在 Windows 达标）
- [ ] 五类 SPI 各有内置实现 + 可运行示例，`META-INF/services` 齐全
- [ ] 无 `TODO` / `FIXME` 遗留在 `src/main`（`grep -rn "TODO\|FIXME" src/main` 为空）
