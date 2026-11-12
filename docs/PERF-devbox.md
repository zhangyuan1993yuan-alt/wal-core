# PERF-devbox：Windows 开发机压测快照（2026-11-11）

> **本文件不是验收报告。** 它记录的是"当前这台 Windows 开发机"上的实测数字，
> 用途是**发现数量级异常、验证基准代码可用、建立本机回归基线**。
> 正式验收数字必须按 `docs/PERF-wal.md` §2 在 Linux NVMe 目标机上跑出来。
> 理由见 `PERF-wal.md` §0：Windows 落 `FileChannelStorageBackend`（每条 append 一次 `write`），
>`force()` 走 `FlushFileBuffers` 而非 `fdatasync`，且系统定时器粒度 15.6ms。
> 本文 §5 用同机对照实验把这三点各自量化了。

---

## 1. 环境

| 项 | 值 |
|---|---|
| CPU | Intel Core i7-10700K，8 物理核 / 16 逻辑核，基频 3.8GHz |
| 内存 | 32GB（压测时空闲约 7.7GB） |
| 磁盘 | 测试数据落 `C:\Users\Y\AppData\Local\Temp` → **Samsung SSD 970 EVO Plus 500GB（消费级 NVMe）** |
| OS | Windows 10 专业版 10.0.19045 |
| JDK | 17.0.14（Microsoft），Maven 3.9.9 离线模式 |
| 存储后端 | `builtin:storage-fc`（Windows 平台默认） |
| 段大小 | 64MB（RecoveryKill9 为 4MB） |
| payload | 256B/条，与 ARCH §15.3 预算口径一致 |
| 背景负载 | 压测期间 CPU 总占用约 26%，IDEA（1.9GB）+ 多个 WorkBuddy 进程常驻 |

> 背景负载这一项直接决定 p99/p999 的可信度：本机不是独占环境，
> 因此下文所有 **p999 与 max 都应视为上界，而非框架能力**。

---

## 2. 吞吐实测

命令：`mvn -o test -Dtest='ThroughputIT' -Dtest.excludedGroups= -Dgroups=perf`

### 2.1 `append()` 纯内存路径

| 场景 | 分区池 | 写入线程 | 稳态 TPS | 备注 |
|---|---:|---:|---:|---|
| 单线程单分区 | 2 | 1 | **137,924** | 约 7.25 µs/条 |
| 多线程 | 2 | 8 | **143,657** | 仅 1.04×，见下方说明 |

`ThroughputIT` 的多线程段用 `t % 2` 分摊到 **2 个分区**，而分区写锁是并行的唯一单元，
所以 8 线程挤在 2 个分区上等于排队。**这不是框架缺陷**：§5 的对照实验里
同样 8 线程改用 8 分区可到 **318,258 条/s（2.6× 单线程）**。
`PERF-wal.md` §5.1 的"16 线程 16 分区"行才是真正的扩展性口径。

### 2.2 `flushAndWait()` 持久确认（两个口径必须成对看）

配置：`groupCommitThreads=4`、`batchIntervalMicros=200`、`FsyncPolicy.BATCH`、2 分区、4 写者。

| 口径 | 说明 | TPS | 单条均值 |
|---|---|---:|---:|
| A 逐条确认 | 每条 append 后立即等它持久 | **1,659** | 602.6 µs |
| B 流水线确认 | 连写 64 条后只等第 64 条 | **52,779** | 18.9 µs（64 条摊薄） |

B/A = **31.8×** —— 组提交在"业务允许攒批"的形态下收益约为一个半数量级，
符合 ARCH §15.3 的判断：逐条确认的 TPS 约等于设备 flush IOPS，与 append 的百万级差两个数量级。

库内直方图口径：flush p50 = 2.19ms / p99 = 5.74ms / p999 = 15.01ms；
共 4,712 批，平均每批 11.9 条（攒批未写满 4096 上限，受 200µs 间隔限制）。

---

## 3. 延迟实测

命令：`mvn -o test -Dtest='LatencyBenchIT' -Dtest.excludedGroups= -Dgroups=perf`

### 3.1 `append()`（纯内存路径，n=200,000）

| mean | p50 | p90 | p99 | p999 | max |
|---:|---:|---:|---:|---:|---:|
| 7.99 µs | **6.20 µs** | 8.70 µs | **17.00 µs** | 72.60 µs | 5,207 µs |

库内直方图（1/64 抽样，n=3,125）：p50 7.10 µs / p99 34.49 µs / p999 85.90 µs。
两套口径 p50 相差 0.9 µs，属抽样误差，可信。

对照 PRD §5（p50 ≤ 5µs、p99 ≤ 100µs）：p99 有明显余量，
**p50 6.20 µs 略超 5µs 目标 —— 这是 FileChannel 后端每条一次 `write` 的直接代价**，
换成 mmap 后 p50 降到 0.30 µs（§5），远优于 §4.1 的 0.48 µs 预算。

### 3.2 `flushAndWait()`（持久确认，n=2,000，groupCommitThreads=2）

| mean | p50 | p90 | p99 | p999 | max |
|---:|---:|---:|---:|---:|---:|
| 15,061 µs | **14,996 µs** | 15,584 µs | 17,340 µs | 23,093 µs | 23,953 µs |

**这组数字（p50 正好 15ms、p90 几乎等于 p50）是 Windows 定时器伪影，不是 WAL 性能。**
证据见 §5.1：本机 `LockSupport.parkNanos(200µs)` 实测 p50 = **15.03ms**。
`flushAndWait` 的攒批等待走 `parkNanos`，一旦写入速率不足以让 flusher 被数据驱动唤醒，
每次确认就要等满一个 15.6ms 的系统时钟节拍。

同一份代码在"有持续写入压力"时完全正常：§2.2 的 4 写者口径下确认延迟是 **602 µs**，
§5.3 的 8 线程口径下是 **1.90 ms**。**不要用本节的 15ms 做任何容量规划。**

---

## 4. 崩溃恢复（`kill -9` 注入）

命令：`mvn -o test -Dtest='RecoveryKill9IT' -Dtest.excludedGroups= -Dgroups=perf`
（Windows 下用 `taskkill /F` 模拟 SIGKILL）

| 项 | 值 |
|---|---:|
| 段大小 | 4,194,304 B |
| 恢复条数 | 30,529 |
| **RTO** | **701 ms** |
| 丢失记录 | **0** |
| 有效尾部 LSN | 30,530 |

零丢失符合预期（`kill -9` 只丢未确认数据，已确认的必须全在）。RTO 701ms 对应 30k 条 / 4MB 段，
约合 0.7ms/MB —— 这属于"段数少、总量小"的场景；万段级 RTO 是 v1.1 的已知待办（见 MEMORY 待办 #3）。

---

## 5. 对照实验（本次为解释上面数字而额外做的）

`target/bench/BackendCompare.java`（临时脚本，`mvn clean` 会清掉，可重跑）。
同一台机器、同一块 NVMe、同一 JVM 参数，先测系统定时器，再让两个后端跑同一段逻辑。

### 5.1 Windows 定时器粒度（关键）

| 请求时长 | 实测 p50 | 实测 p99 |
|---|---:|---:|
| `LockSupport.parkNanos(200 µs)` | **15.03 ms** | 15.73 ms |
| `Thread.sleep(1 ms)` | 1.51 ms | — |

`parkNanos` 被拉伸 75 倍。**任何依赖"短超时"的攒批/退避逻辑在 Windows 上都测不出真实值**，
这是 §3.2 那组 15ms 数字的唯一原因。

### 5.2 后端对照：FileChannel（现状）vs mmap（Linux 生产路径）

| 场景 | FileChannel | mmap | 倍差 |
|---|---:|---:|---:|
| 1 线程 1 分区 append | 121,953 条/s | **1,229,624 条/s** | **10.1×** |
| ↳ p50 延迟 | 6.30 µs | **0.30 µs** | **21×** |
| ↳ p99 延迟 | 20.60 µs | 2.90 µs | 7.1× |
| ↳ p999 延迟 | 93.10 µs | 47.70 µs | 2.0× |
| 8 线程 8 分区 append | 318,258 条/s | **2,938,195 条/s** | 9.2× |
| 16 线程 16 分区 append | 309,815 条/s | 2,103,153 条/s | 6.8× |
| 8 线程逐条 flushAndWait | 526 条/s | 529 条/s | **1.01×** |

三条结论：

1. **append 路径的 Windows 数字对 Linux 毫无参考价值**（10 倍差、p50 差 21 倍），
   这正是 `PERF-wal.md` §0 禁止把本机数字填进验收表的实证。
   mmap 的 p50 0.30 µs 甚至优于 §4.1 预算的 0.48 µs，说明"零系统调用追加"的设计是成立的。
2. **`flushAndWait` 与后端完全无关**（526 vs 529）。因为瓶颈在 `force()` 与设备，
   不在数据拷贝。这反过来印证 ARCH §15.3：吞吐瓶颈是 fsync IOPS，不是 CPU、不是带宽。
3. **扩展性在 16 线程时回落**：mmap 从 2.94M（8 线程）降到 2.10M（16 线程），FC 也从 318k 降到 310k。
   i7-10700K 是 8 物理核，16 逻辑核里后 8 个是超线程，两个线程抢一个物理核的写带宽必然互损。
   **结论：分片数取物理核数，不要取逻辑核数。**

---

## 6. 与预算基线对照（仅在本机口径下看）

| 指标 | 预算（ARCH §15） | 本机 FC 实测 | 本机 mmap 实测 | 判读 |
|---|---|---:|---:|---|
| append p50 | 0.48 µs | 6.20 µs | **0.30 µs** | FC 超 13×，mmap 达标 |
| append p99 | 1.52 µs | 17.00 µs | 2.90 µs | 同上 |
| flushAndWait p50（16 分区/2 flusher） | ~250 µs | 无用（15ms 伪影） | 无用 | 需 Linux 独占机 |
| 逐条确认 TPS | ≈ 设备 flush IOPS | 1,659（2 分区/4 写者） | — | 消费级盘合理 |
| 百万 TPS | NVMe 100k+ IOPS 前提 | 143,657 / 310k | 2.10M~2.94M | **不可判定**，需目标机 |

---

## 7. 本次压测的结论

1. **没有发现数量级错误。** 所有数字落在"Windows + FileChannel + 消费级 NVMe + 26% 背景负载"的合理区间内。
2. **一个真实可改进点**：`ThroughputIT` 的多线程段把线程分摊到 2 个分区（`t % 2`），
   导致"多线程吞吐"这个指标被分区数锁死，看起来像"多线程无收益"。建议改成可配置分区数，
   或者直接对齐 `PERF-wal.md` §5.1 的 16 线程 16 分区口径。
3. **一个必须写进文档的坑**：Windows 上 `parkNanos` 有 15.6ms 下限，
   任何"单线程逐条 `flushAndWait`"的延迟测量在 Windows 上都是废数。已在本文 §5.1 留证。
4. **验收数据仍然只能在 Linux NVMe 独占机上取。** 本文件的所有数字都不能进 `PERF-wal.md` §5~§7。

---

## 8. 复现命令

```bash
# 吞吐（约 52s）
mvn -o test -Dtest='ThroughputIT' -Dtest.excludedGroups= -Dgroups=perf

# 延迟（约 48s）
mvn -o test -Dtest='LatencyBenchIT' -Dtest.excludedGroups= -Dgroups=perf

# 崩溃恢复（约 5s）
mvn -o test -Dtest='RecoveryKill9IT' -Dtest.excludedGroups= -Dgroups=perf

# 后端 / 定时器对照（约 21s，临时脚本）
javac -encoding UTF-8 -cp "target/classes;target/test-classes" -d target/bench/classes target/bench/BackendCompare.java
java -Xms1g -Xmx1g -cp "target/classes;target/test-classes;target/bench/classes" BackendCompare
```

> 三次 surefire 运行均为 `Tests run: 5, Failures: 0, Errors: 0`，基准断言（量级下限）全部通过。
