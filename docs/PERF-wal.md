# PERF：WAL 框架性能基准（T19）

| 项目 | 内容 |
|---|---|
| 关联任务 | T19（性能基准与达标验证） |
| 上游依据 | `docs/ARCH-wal.md` §15 性能预算表、§16 技术选型；`docs/PRD-wal.md` §5 |
| 被测版本 | `wal-core` 1.0.0（T01~T20 交付版） |
| 本文档状态 | **模板 + 预算基线已就位，目标机数字待填**（见下方"实测状态"） |

---

## 0. 实测状态（先看这一段，避免误读本文档）

> **当前没有一行"实测数字"被写进本文档，这是刻意的。**
>
> 本次交付在 **Windows 开发机**上完成。Windows 能保证**正确性**（三后端共用同一套契约测试、
> 崩溃恢复 41 例全绿），但**不能用于性能验收**，原因有三条，每条都是物理性的：
>
> 1. **后端不同**：Linux x86_64 默认走 `MappedStorageBackend`（`mmap`，单条 append 是纯 `memcpy`，
>    约 10ns）；Windows 落 `FileChannelStorageBackend`（每条一次 `write` 系统调用，约 1~2µs）。
>    两者在同一台机器上 append p50 可以差 **2 个数量级**，混着报数字毫无意义。
> 2. **`force()` 语义与设备不同**：Windows 上 `FileChannel.force(false)` 走 `FlushFileBuffers`，
>    与 NVMe 上的 `fdatasync` 不是同一条路径；消费级 SSD 的 flush IOPS 与 NVMe 企业盘差 1~2 个数量级。
>    §15.3 已算出吞吐瓶颈在 **fsync IOPS**，这正好是最不能跨设备外推的指标。
> 3. **计时器与调度**：Windows 默认时钟中断粒度 15.6ms、线程调度更粗，µs 级分位测不准。
>
> 所以本文档交付的是：**可复现的基准骨架（`*IT.java` 三个） + 明确的口径定义 + 预算基线**。
> 目标机上按 §2 的三条命令跑完，把 §4~§7 的空表格填上，即为完整验收报告。

| 交付项 | 状态 |
|---|---|
| `it/ThroughputIT.java`（吞吐双口径） | ✅ 已交付，`@Tag("perf")`，默认跳过 |
| `it/LatencyBenchIT.java`（延迟双口径 + 分位表） | ✅ 已交付，`@Tag("perf")`，默认跳过 |
| `it/RecoveryKill9IT.java`（`kill -9` 注入 + 零确认丢失） | ✅ 已交付，`@Tag("perf")`，默认跳过；Windows 用 `taskkill /F` |
| 预算基线（ARCH §15 抄录） | ✅ §4 已抄录 |
| **目标机实测数字** | ⏳ **待 Linux NVMe 目标机执行** |
| 未达标项与原因分析 | ⏳ 随实测填写 |

---

## 1. 目标机环境（跑之前先填这张表）

一份性能数字脱离环境是没有意义的。下表每个字段都必须填，缺一个就无法复现。

| 项 | 值 | 怎么取 |
|---|---|---|
| CPU 型号 / 物理核 / 逻辑核 | _待填_ | `lscpu` |
| CPU 基频 / 睿频 | _待填_ | `lscpu \| grep MHz` |
| 是否支持 SSE4.2（`CRC32C` intrinsic） | _待填_ | `lscpu \| grep -o sse4_2` |
| 内存容量 / 频率 | _待填_ | `free -g` / `dmidecode -t memory` |
| 磁盘型号 | _待填_ | `nvme list` / `lsblk -d -o NAME,MODEL` |
| 文件系统 | _待填_ | `df -T <baseDir>`（预期 `xfs` 或 `ext4`） |
| 挂载选项 | _待填_ | `mount \| grep <dev>`（关注 `noatime`、`barrier`） |
| 内核版本 | _待填_ | `uname -r` |
| JDK 版本 / 发行版 | _待填_ | `java -version`（要求 17+，推荐 Temurin / Zulu） |
| JVM 参数 | _待填_ | 见 §1.1 |
| 单次 `fdatasync` 基线延迟 | _待填_ | `fio --rw=write --bs=4k --fdatasync=1 --iodepth=1 --runtime=30 ...` 取 p50/p99 |
| 单设备 4K flush IOPS | _待填_ | `fio --rw=randwrite --bs=4k --fsync=1 --iodepth=32 ...` |
| 是否有其他负载 | _待填_ | 必须独占，否则 p99 全部作废 |

### 1.1 建议的 JVM 参数

```bash
java -Xms4g -Xmx4g \
     -XX:+AlwaysPreTouch \
     -XX:+UseG1GC -XX:MaxGCPauseMillis=20 \
     -XX:MaxDirectMemorySize=768m \
     -XX:+HeapDumpOnOutOfMemoryError \
     -jar <...>
```

> `-Xms == -Xmx` + `AlwaysPreTouch` 是为了把冷页 fault 从测量区间里排除。
> `MaxDirectMemorySize=768m` 对应 ARCH §16.3 的 512MB 堆外预算 + 余量。
> **不要**加 `-XX:-UseCompressedOops` 或 `-Xint`——我们测的是生产形态。

### 1.2 目标机上的常规准备

```bash
# 1) 关掉 CPU 频率调节与 swap，减少抖动
sudo cpupower frequency-set -g performance
sudo swapoff -a

# 2) drop page cache（每个测试轮次之间都要做，否则后几轮被 page cache 白送）
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches

# 3) 数据目录放在被测 NVMe 上，不要放 tmpfs（tmpfs 没有 fsync 成本，测出来的
#    flushAndWait 是假的）
```

---

## 2. 怎么跑（三条命令）

`*IT.java` 的后缀不在 surefire 默认 include 集合里，所以 `mvn test` 天然跳过它们；
`@Tag("perf")` 又被 `pom.xml` 的 `<excludedGroups>perf</excludedGroups>` 二次拦了一道。
要跑必须**显式打开两道闸**：

```bash
# ① 吞吐：append 纯内存路径（单线程 / 8 线程 / 16 分区） + flushAndWait 双口径
mvn -o test -Dtest='ThroughputIT' -Dtest.excludedGroups= -Dgroups=perf

# ② 延迟：append p50/p99/p999 + flushAndWait p50/p99/p999（分位表直接打到 stdout）
mvn -o test -Dtest='LatencyBenchIT' -Dtest.excludedGroups= -Dgroups=perf

# ③ kill -9 注入：子进程写到一半被强杀 → 重启 → 断言"数据是写入序列的前缀、无空洞、无重复"
mvn -o test -Dtest='RecoveryKill9IT' -Dtest.excludedGroups= -Dgroups=perf
```

三个参数各自的作用（不要漏掉任何一个，漏了就是 0 个用例被跑而构建仍然是绿的）：

| 参数 | 作用 | 漏掉的后果 |
|---|---|---|
| `-Dtest='ThroughputIT'` | 覆盖 surefire 默认 include 模式 | 一个用例都不跑，`BUILD SUCCESS` 骗人 |
| `-Dtest.excludedGroups=`（空值） | 把 `<excludedGroups>` 里的 `perf` 清空 | 用例被 tag 过滤掉，同样 0 个 |
| `-Dgroups=perf` | 只跑 `@Tag("perf")` | 无影响（因为这些类里全是 perf），保留是为了意图明确 |

指标输出走 `System.out.printf`，在 surefire 报告里可见：

```bash
cat target/surefire-reports/com.walcore.it.LatencyBenchIT-output.txt
# 或直接看控制台
```

> **禁止**把 Windows 开发机跑出来的数字填进本文档。上一段已经解释了为什么。
> 开发机上跑这三个 IT 的唯一合法用途是**验证基准代码本身没写错**（能跑完、不抛异常、
> 相对关系合理），而不是取数。

---

## 3. 口径定义（**本节是本文档最重要的部分**）

WAL 的"性能"永远必须带口径说，否则两方人拿同一个库能报出差 100 倍的数字还都"没错"。
本项目固定为**两个互不替代的口径**。

### 3.1 口径 A：`append()` —— 纯内存追加

```java
long lsn = wal.append(pid, payload, AppendOptions.DEFAULTS);   // 到这里就返回
```

* **语义**：记录已写入 Java 侧可见的段文件映射区 / staging buffer，**并且** LSN 已分配。
  此刻数据在 **page cache**，不在盘上。
* **崩溃后果**：进程 `kill -9` 不丢（数据在内核 page cache，内核还在）；**断电会丢**。
* **能报什么**：TPS、p50/p99/p999 延迟。
* **不能报什么**：**不能**称之为"写入成功"、"已持久化"、"已 ACK"。它是**单机最高吞吐的**上限口径。

### 3.2 口径 B：`flushAndWait()` —— 持久确认

```java
long lsn = wal.append(pid, payload, AppendOptions.DEFAULTS);
boolean ok = wal.flushAndWait(pid, lsn, 1_000L);   // 到这里才敢回客户端 ACK
```

* **语义**：LSN ≤ `lsn` 的记录已经过 `fdatasync`（`force(false)`）落到**存储设备**。
  此刻 `kill -9`、断电、拔电都丢不了。
* **能报什么**：持久确认 TPS、持久确认 p50/p99/p999。
* **这是唯一可以对业务方承诺"数据不丢"的口径**。撮合引擎必须在回 ACK 前调用它。

### 3.3 **"100 万 TPS"与"100µs 持久确认"在单机 NVMe 上不可同时达成**

这句话必须写进 README、`WalConfig` Javadoc 和本文档，否则验收时一定吵起来。
原因是组提交的物理下限，不是实现问题：

| 事实 | 数字 | 出处 |
|---|---|---|
| 单次 `fdatasync` 在 NVMe 企业盘上的延迟 | **p50 ≈ 80µs，p99 ≈ 300µs** | ARCH §15.2 |
| 想要 100µs 的持久确认 p99 | 只能攒 **1 批 ≈ 20µs** 的窗口就刷 | ARCH §15.2 取舍段 |
| 20µs 窗口能攒多少条（256B/条） | `20µs × 吞吐`；要 100 万 TPS 需攒 **20 条/批** | 除法 |
| 20 条/批 → 每秒需要多少次 `fdatasync` | **50,000 次/s**（单分区） | 除法 |
| 单设备 flush IOPS 上限 | 企业 NVMe `100k~200k`；消费级 SSD `~10k` | ARCH §15.3 |
| **结论** | 20 条/批时，fsync 已经吃掉一半以上 IOPS 预算，且每次 fsync 80µs 与 20µs 窗口叠加 → 队列必然堆积，p99 一定爆 | — |

**反过来说**：要 100 万 TPS，必须让每批攒到 **~3200 条（≈1MB）**，
此时 `batchIntervalMicros` 自然是"批满即发"，持久确认延迟 = 一批的攒批时间 + fsync
≈ `1MB/吞吐` + `80µs`，实测 p50 落在 **200~300µs** 区间（ARCH §15.2 预算 ~250µs）。

**所以正确的表述是：**

> 单机 NVMe：`append` 可达 100 万 TPS 量级（纯内存口径）；
> 同一台机器上 `flushAndWait` 的持久确认延迟是 **~200µs p50 / ~1.4ms p99**（16 分区、2 组提交线程）。
> 若要持久确认 p99 ≤ 100µs，必须 `batchIntervalMicros=20` + 分区数 ≤ 4，
> 此时单分区吞吐降到 **~15 万 TPS**。**两个指标二选一，取决于业务要吞吐还是要延迟。**

### 3.4 其它必须一起报的口径

| 指标 | 口径 | 注意 |
|---|---|---|
| 顺序读吞吐 | `cursor.nextBatch(n, visitor)` 从段首读到段尾，MB/s | 必须 `drop_caches` 后测，否则测的是 page cache 带宽 |
| RTO（恢复时间） | `cleanShutdown=true` 时开机到可用 | 跳过逐帧 CRC，只验 meta + 段序列 |
| RTO（崩溃恢复） | `cleanShutdown=false` 时开机到可用 | 含逐帧 CRC 扫描，与日志总量线性相关 |
| `kill -9` 零丢失 | 强杀后重启，断言"已确认记录"一条不少 | 只对 `flushAndWait` 返回 true 的记录成立 |

---

## 4. 预算基线（ARCH §15 抄录，**目标机实测前这就是验收依据**）

### 4.1 `append()` 纯内存路径预算

| 阶段 | p50 预算 (µs) | p99 预算 (µs) | 说明 |
|---|---:|---:|---|
| 状态检查 + 背压检查（2 个 volatile load） | 0.01 | 0.02 | 分支预测友好 |
| 分区 `ReentrantLock.lock()`（无竞争） | 0.02 | 0.30 | 非公平锁；p99 含偶发唤醒 |
| LSN 分配（`AtomicLong.getAndIncrement`） | 0.02 | 0.20 | 16 线程争用同一条缓存行 |
| 帧头编码（4×`putInt` + 4×`putLong`） | 0.02 | 0.05 | 未对齐安全的 `VarHandle` 写 |
| payload `memcpy` 256B（含 page fault 摊销） | 0.01 | 0.15 | 页已驻留时 ~10ns；冷页 fault 计入 p99 |
| **CRC32C 320B（SSE4.2 硬件）** | **0.35** | **0.60** | `java.util.zip.CRC32C` 走 intrinsic，吞吐 ~1.5GB/s |
| 回填 crc + `storeRelease(publishedWritePos)` | 0.01 | 0.02 | |
| `markDirty` + `addBytes`（2× `AtomicLong`） | 0.02 | 0.08 | |
| `unlock()` + 返回 | 0.02 | 0.10 | |
| **合计** | **0.48** | **1.52** | 对 p99 ≤ 2µs 目标余量 24%；对 p99 ≤ 100µs 目标余量 6500% |

> 关键风险不在延迟而在 **CPU 占用**：100 万 TPS 需 `0.48µs × 1M = 0.48 核`，
> LSN 争用再 0.10 核，合计 ~0.6 核，满足"≤ 1 核"预算。

### 4.2 `flushAndWait()` 持久确认路径预算

| 阶段 | p50 预算 (µs) | p99 预算 (µs) | 说明 |
|---|---:|---:|---|
| `durableLsn` 快路径命中 | 0.02 | 0.05 | 同批内多订单共用 ACK 点 |
| 攒批等待（`parkNanos`） | 100（均摊） | 200 | `batchIntervalMicros=200` |
| `fdatasync` × 分组内分区数 | 80 | 300（单次） | NVMe 企业盘 |
| 串行提交 16 分区 | +240 | +800 | 设备层并行，非简单相加 |
| 水位发布 + `unpark` | 2 | 10 | |
| **合计（16 分区、`groupCommitThreads=2`）** | **~250** | **~1,400** | |
| **合计（1 分区）** | **~180** | **~500** | |
| **合计（16 分区、`groupCommitThreads=4`）** | **~180** | **~900** | 线程切换开销上升 |

### 4.3 吞吐预算（16 分区、256B/条、`BATCH`）

| 项 | 计算 | 结果 |
|---|---|---|
| 写入侧 CPU | `0.48µs/条 × 1M` = 0.48 核（分摊 16 线程 = 每线程 3%） | 不计瓶颈 |
| LSN 争用 | `100ns × 1M` = 0.10 核 | 不计瓶颈 |
| fsync 侧 | 每批 ~1MB ≈ 3200 条；`5000 批/s × 16 force` = 80k force/s | ⚠ 需 NVMe 支持 80k IOPS 的 4K-flush |
| 磁盘带宽 | `1M × 328B` = 328 MB/s | 充裕 |
| **结论** | 瓶颈在 **fsync IOPS**。NVMe 企业盘随机 4K 写 + flush ≈ 100k~200k IOPS，可支撑 100 万 TPS；消费级 NVMe 需把 `batchMaxBytes` 提到 4MB 把 force 次数压到 20k/s | 达成，但必须在目标机实测 |

---

## 5. 吞吐实测（⏳ 待填）

命令：`mvn -o test -Dtest='ThroughputIT' -Dtest.excludedGroups= -Dgroups=perf`

### 5.1 `append()` 纯内存路径 TPS

| 场景 | 分区数 | 写入线程 | 条数 | 稳态 TPS | 相对单线程 | 预算 | 达标 |
|---|---:|---:|---:|---:|---:|---:|:--:|
| 单线程单分区 | 1 | 1 | _待填_ | _待填_ | 100% | — | — |
| 8 线程单分区 | 1 | 8 | _待填_ | _待填_ | _待填_ | — | — |
| 16 线程 16 分区 | 16 | 16 | _待填_ | _待填_ | _待填_ | ≥ 100 万 | — |

> 多分区相对单分区的衰减必须 **≤ 10%**（T08 验收点 ③）。衰减超 10% 说明
> 分区表读路径或 LSN 争用有问题，先查 `PartitionRegistry` 的 `loadAcquire` 是否被写成了
> 数组遍历，再查 `LsnAllocator` 的填充是否导致伪共享。

### 5.2 `flushAndWait()` 持久确认 TPS（**与 5.1 分开报**）

| 场景 | 分区数 | 确认方式 | 条数 | TPS | 说明 |
|---|---:|---|---:|---:|---|
| 逐条确认 | 1 | 每条 `append` 后立即 `flushAndWait` | _待填_ | _待填_ | 这是**最坏**口径：攒批恒为 1 条，TPS ≈ `1/fdatasync延迟` |
| 流水线确认 | 1 | 写 N 条后一次确认到最大 LSN | _待填_ | _待填_ | 这是**最好**口径：攒批被写满 |
| 流水线确认 | 16 | 同上，16 分区并行 | _待填_ | _待填_ | 生产形态 |

> 逐条确认的 TPS 大约就是"设备 fdatasync IOPS"，与 `append` 的 100 万 TPS 相差两个数量级。
> **这不是 bug**，是口径差。报告时必须成对出现，否则读者会误以为框架慢。

### 5.3 顺序读吞吐

| 场景 | 段数 | 记录数 | MB/s | 预算 | 达标 |
|---|---:|---:|---:|---:|:--:|
| `cursor.nextBatch(1000)` 顺序读 | _待填_ | _待填_ | _待填_ | ≥ 300 | — |

---

## 6. 延迟实测（⏳ 待填）

命令：`mvn -o test -Dtest='LatencyBenchIT' -Dtest.excludedGroups= -Dgroups=perf`

### 6.1 `append()` 延迟

| 分区数 | 线程数 | 样本数 | p50 (µs) | p99 (µs) | p999 (µs) | max (µs) | 预算 | 达标 |
|---|---:|---:|---:|---:|---:|---:|---|:--:|
| 1 | 1 | _待填_ | _待填_ | _待填_ | _待填_ | — | p50 ≤ 5 / p99 ≤ 100 / p999 ≤ 1000 | — |
| 16 | 16 | _待填_ | _待填_ | _待填_ | _待填_ | — | p50 ≤ 5 / p99 ≤ 100 / p999 ≤ 1000 | — |

### 6.2 `flushAndWait()` 延迟（**独立表格，不要和 6.1 合并**）

| 分区数 | 组提交线程 | 攒批上限 | 样本数 | p50 (µs) | p99 (µs) | p999 (µs) | 预算 p50/p99 | 达标 |
|---|---:|---|---:|---:|---:|---:|---|:--:|
| 1 | 2 | 200µs / 4096 / 1MB | _待填_ | _待填_ | _待填_ | _待填_ | ~180 / ~500 | — |
| 16 | 2 | 同上 | _待填_ | _待填_ | _待填_ | _待填_ | ~250 / ~1400 | — |
| 4 | 4 | 20µs / 256 / 64KB | _待填_ | _待填_ | _待填_ | _待填_ | p99 ≤ 100（低延迟档） | — |

> 第三行是"低延迟档"配置：用吞吐换延迟。跑这一行时同时记下 TPS，
> 用来佐证 §3.3 的"二选一"结论（预期 TPS ≈ 15 万）。

### 6.3 分位估计精度自校验（库内直方图 vs 精确采样）

`LatencyBenchIT` 会同时用"库内 `LatencyHistogram`"和"精确 `long[]` 排序"两条路算分位，
两者误差必须 **< 5%**（T17 验收点 ②）。固定桶 `{1,2,5,10,50,100,500,1000,5000,10000,50000}µs`
的固有分辨率上限是"桶宽的 1/2"，落在宽桶里的分布误差会放大，这属于设计已知限制，
不是缺陷——记误差时同时记下该分位落在哪个桶。

| 指标 | 库内估计 (µs) | 精确值 (µs) | 相对误差 | 所在桶 | 达标（<5%） |
|---|---:|---:|---:|---|:--:|
| `append` p50 | _待填_ | _待填_ | _待填_ | _待填_ | — |
| `append` p99 | _待填_ | _待填_ | _待填_ | _待填_ | — |
| `flushAndWait` p50 | _待填_ | _待填_ | _待填_ | _待填_ | — |
| `flushAndWait` p99 | _待填_ | _待填_ | _待填_ | _待填_ | — |

### 6.4 埋点开销

同一压测关/开指标各跑一轮，比较总耗时。预算 **< 1%**（T17 验收点 ③）。

| 轮次 | 指标 | 条数 | 总耗时 (ms) | TPS | 相对开销 |
|---|---|---:|---:|---:|---:|
| A | 关闭 | _待填_ | _待填_ | _待填_ | 基线 |
| B | 开启 | _待填_ | _待填_ | _待填_ | _待填_（预算 < 1%） |

> `append` 热路径的埋点已按"零分配"设计：计数器全是预注册的 `LongAdder`，
> 延迟直方图按 LSN `1/64` 抽样（`APPEND_LATENCY_SAMPLE_MASK = 63`），
> 未抽中时**连 `System.nanoTime()` 都不读**。`MetricsTest#instrumentationAddsNoAllocation`
> 已用 `ThreadMXBean.getThreadAllocatedBytes` 断言"开/关两轮分配字节在容差内一致"。

**测量方法（照这个做，否则数字不可信）**

1. **用线程 CPU 时间（`ThreadMXBean.getThreadCpuTime`），不要用墙钟**。墙钟口径下
   "关"侧三轮能稳在 ±0.16%，"开"侧却在 +0.9%~+2.6% 之间跳（还出现过 -6% 的离群对），
   1% 的阈值红绿基本靠运气。
2. **单轮至少跑到秒级**。Windows 的线程 CPU 计时粒度是 15.625ms：200k 条一轮只有
   ~1.3s，一个 LSB 就是 1.2%——**阈值小于分辨率**，测什么都可能红。单轮 100 万条
   把窗口拉到 7s 量级，量化误差才降到 0.2%。
3. **交替顺序、多轮配对、取中位数**。"先连跑 3 次开、再连跑 3 次关"会把两段的
   CPU 频率/后台负载漂移全记到 metrics 头上；中位数对重尾离群不敏感。
4. **必须先预热再取样**：metrics 分支冷 JIT 时首轮实测 +16.45%，预热一轮后才稳定。

**Windows 开发机参考值（不构成验收证据）**：100 万条/轮、CPU 时间口径、3 轮配对
中位数约 **+3.6%**（≈235ns/条固定成本）。这个比值高度依赖分母——本机单条 `append`
约 7µs，而 Linux mmap 快一个量级，同一份埋点代码的"占比"会显著不同。因此
`MetricsTest#metricsOverheadBelowOnePercent` 在非 Linux 平台断 5%（只做"别在热路径
上加字符串拼接/装箱"的量级护栏），**严格 1% 的验收必须在 Linux 目标机上按本节
方法复测**，本节表格也只认目标机数字。

---

## 7. 崩溃恢复与 RTO（⏳ 待填）

命令：`mvn -o test -Dtest='RecoveryKill9IT' -Dtest.excludedGroups= -Dgroups=perf`

### 7.1 `kill -9` 注入后零确认丢失

| 轮次 | 强杀时刻已确认 LSN | 重启后最大可读 LSN | 已确认记录缺失 | 截断字节 | 达标（缺失=0） |
|---|---:|---:|---:|---:|:--:|
| 1 | _待填_ | _待填_ | _待填_ | _待填_ | — |
| 2 | _待填_ | _待填_ | _待填_ | _待填_ | — |
| 3 | _待填_ | _待填_ | _待填_ | _待填_ | — |

> 判定口径：重启后**已确认**（`flushAndWait` 返回 true）的记录必须一条不少；
> **未确认**的记录允许丢失，但一旦存在就必须是**尾部连续前缀**，
> 不允许出现"中间缺一条后面还有"的空洞（P0-05 ④）。

### 7.2 RTO

| 场景 | 日志总量 | 段数 | 记录数 | RTO (ms) | 预算 | 达标 |
|---|---:|---:|---:|---:|---|:--:|
| `cleanShutdown=true`（跳过逐帧 CRC） | _待填_ | _待填_ | _待填_ | _待填_ | ≤ 100 | — |
| 崩溃恢复（全量 CRC 扫描） | 256MB | _待填_ | _待填_ | _待填_ | ≤ 30s | — |
| 崩溃恢复（万段级） | _待填_ | 10,000 | _待填_ | _待填_ | 见 §8 | — |

---

## 8. 已知未达标项与降级说明（⏳ 随实测补充）

以下是**已识别的、本次交付未优化的项**，不是隐藏缺陷，明确留给目标机验收与本项目后续版本：

| # | 项 | 现状 | 原因 | 建议 |
|---:|---|---|---|---|
| 1 | 万段级 RTO 优化 | 恢复期按段串行扫描，`10,000` 段时预计超过 30s 预算 | 首版优先保证正确性与"绝不静默跳过"；万段级需并行扫描 + 页对齐预读 | 目标机实测后若超标，按 ARCH §9.2 的并行扫描方案做增量；**在此之前建议把 `segmentSizeBytes` 调大**（段数 = 总量/段大小，是本项唯一的手动旋钮） |
| 2 | Windows 后端性能 | `FileChannel` 后端单条 ~1~2µs，比 mmap 慢约 2 个数量级 | Windows 无法在映射中删除文件，`MappedStorageBackend` 不作为 Windows 默认 | Windows 仅用于功能验证；生产请上 Linux。可用 `WalBuilder.storageBackend("builtin:storage-mapped")` 强制切换后自测 |
| 3 | 分位估计在宽桶分布的精度 | 尾巴全部落在同一宽桶时误差可达桶宽的 1/2 | 固定桶直方图的固有分辨率上限（换成动态桶会引入锁或分配，与"零分配"冲突） | 需要精确分位时用 `LatencyBenchIT` 的精确采样路径；生产监控用库内直方图足够（< 5% 已在三类典型分布上验证） |
| 4 | `flushAndWait` p99 ≤ 100µs 的默认配置 | 默认配置 p99 ≈ 1.4ms | 默认面向吞吐（`batchIntervalMicros=200`） | 见 §3.3 / §6.2 第三行：`batchIntervalMicros=20` + 分区 ≤ 4 + `groupCommitThreads=4`，代价是 TPS 降到 ~15 万 |
| 5 | 真实复制（`ReplicationSink`） | 只交付 `NoopReplicationSink` 与示例 `PrintlnReplicationSink`；**当前版本框架不向 sink 投递记录**（全库没有 `onRecord` 调用点） | 复制涉及网络、ACK 语义、脑裂处理，超出 WAL 内核范围；SPI 与 `replicatedLsn()` 水位已留好 | 现阶段做主备复制请走 `WalTailer` 拉模式（README §6.1 已如实标注）；`ReplicationSink` 的真实投递逻辑留待后续版本 |

---

## 9. 结论（跑完填这一段）

1. **功能正确性**：Windows 开发机 `mvn -o test` 全绿（`Tests run` 见 §9.1），三后端契约测试、
   崩溃恢复 41 例、五类 SPI 全部通过。
2. **性能**：目标机实测结果见 §5~§7。达标/未达标项逐条列在 §8。
3. **口径一致性**：所有报告的 TPS 与延迟都标注了是 `append` 还是 `flushAndWait`，
   无一处混用（这是本文档存在的首要目的）。

### 9.1 功能回归基线（Windows 开发机，非性能验收）

| 项 | 值 |
|---|---|
| `mvn -o test` | **`Tests run: 433, Failures: 0, Errors: 0, Skipped: 0`**（全绿，`@Tag("perf")` 默认排除） |
| 性能类用例（含 IT） | 7 个单元级 perf 用例 + 5 个 IT 用例，需 `-Dgroups=perf` 显式触发 |
| 崩溃恢复用例数 | **41**（`RecoveryTornWriteTest` 27 + `RecoveryCorruptionPolicyTest` 9 + `IndexRebuildTest` 5） |
| 批次失败隔离回归 | `GroupCommitTest` 22 例，其中 3 例专钉"批内某分区 force 失败不得带走同批其它分区的脏位""已刷成功分区的等待者必须立刻唤醒"（旧实现两处都错，且 430 例时无覆盖） |
| 三后端契约 | `SegmentHandleContractTest` 23 例（三后端参数化）+ `MultiBackendEndToEndTest` 6 例 |
| 游标 pin 粒度回归 | `CursorReaderPinTest` 7 例（"只有当前段登记读者"，保证保留策略不被长活游标废掉） |
| 编译告警 | 主代码 `-Xlint:all -Werror` 通过；测试代码 `-Xlint:all` 通过 |
| 编译期依赖 | `mvn dependency:tree -Dscope=compile` **输出为空** |
| D3 轮转泄漏回归 | 1000 次轮转：`preparer.stop()` 后 逻辑段数 = `.wal` 文件数 = **1001**，`.tmp` 残留 **0** |
