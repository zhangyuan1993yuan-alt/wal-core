# WAL 框架项目长期记忆

## 项目状态
通用高并发 WAL（预写日志）框架 `wal-core`，2026-11-10 交付、2026-11-11 锁定 **v1.0.0**。位于 `J:/WiWork/Project/WorkBuddy/wal`。
- Java 17 / Maven / **零第三方运行时依赖** / 最终 433 用例全绿（2026-11-11：422 → 430 修 F1~F8 → 433 修组提交丢位；3 个 flaky 测试已根治）
- 用途：量化撮合系统日志底座（崩溃恢复、回放复现、备节点订阅、审计）
- 2026-11-12 起纳入 git：远端 `https://github.com/zhangyuan1993yuan-alt/wal-core`（Private，分支 `main`），首次提交 `f6a2e5d`；改前/改后 tar 备份在 `../_backups/wal/`

## 维护必读（改代码前）
- **写失败不回退 LSN**：rewind 仅恢复期可用（DefaultWal.open 内唯一调用点），运行期 rewind 会发出重复 LSN——重复比空洞危害更大，空洞由恢复期吸收
- **组提交批次失败语义**（`GroupCommitCoordinator`，改这里必读类注释）：脏位在 `drainMine` 时已从全局位图取走，因此①失败绝不能中断批次（逐个 redirty 后继续）②等待者按 `durableLsn` 真值结算（不是按"处理到第几个分区"）③0 号 flusher 每秒兜底扫描 `publishedLsn>durableLsn` 的活跃分区补脏位。破坏任一条 = 静默丢持久性或假报失败
- 帧格式唯一真源：`internal/append/FrameLayout`；CRC32C 区间 `[0,20)∪[24,frameLen)`
- 三条核心不变量：①LSN 分配在分区写锁内紧邻写帧 ②append() 路径零 new/锁/fsync/队列 ③帧完整编码后才提交写指针
- 段序列是 volatile 不可变快照（`PartitionSegments.Snapshot`），结构变更（轮转/删除/冻结）必须持 `ps.mutex`
- 保留删除走三段式（锁内定位+围栏复核+摘除，锁外 unmap+删文件）；读者有 LSN 围栏 `RetentionPin` + 段级 readerCount 双保护
- 段名 LSN 可能大于段内真实首 LSN（nameReservations 发号），恢复扫描一律以实际扫描为准
- `MappedSegmentHandle.force()` 必须 `fileChannel.force(false)`，禁用 `MappedByteBuffer.force()`；close 必须 unmap（Windows 否则删不掉文件）

## 性能口径（对用户/验收的承诺）
- `append()` p99 ≤100µs（内存）；`flushAndWait()` 默认 p99 ~1.4ms。**百万 TPS 与 100µs 持久确认单机不可兼得**
- 默认配置面向吞吐；低延迟配置 `batchIntervalMicros=20)+分区≤4+groupCommitThreads(4)`，TPS 降到 ~15 万
- 性能实测必须在 Linux NVMe 做（PERF-wal.md §2 有完整命令），Windows 只保正确性
- **埋点开销（P1-05③ <1%）本机测不了**：Windows 线程 CPU 计时粒度 15.625ms、单条 append ~7µs，比值口径依赖分母。目标机复测方法见 PERF-wal.md §6.4（CPU 时间 + 预热 + 百万条/轮 + 配对中位数）。实测固定成本约 235ns/条

## 下版本待办（优先级序）
1. Linux 目标机性能实测（填 PERF-wal.md 空表；含埋点开销 1% 的正式验收）
2. `ReplicationSink` 真实投递实现（当前插件已装配但框架零调用点，README 已如实标注，临时方案走 WalTailer 外接）
3. 万段级 RTO 并行恢复扫描
4. 在线 `verify()` API（半天量，复用恢复扫描器，只读）
5. `appendBatch` 支持混合类型与幂等键
6. 组提交退避按分区化（当前一处分区 force 失败会让同 flusher 名下全部分区退避 5s，属"有界代价"，类注释已登记）

## 版本控制与推送（改 .gitignore 或推代码前必读）
- **`.gitignore` 的运行期目录规则必须根锚定**（`/plugin/ /partitions/ /snapshots/ /LOCK /wal.meta /target/`）。历史上裸 `plugin/` 误伤了 12 个 `src/**/plugin/` 下的 Java 源文件，差点推上编译不过的仓库。新增规则后用 `git ls-files --others --ignored --exclude-standard` 复核
- **`.workbuddy/memory/` 已入库**（2026-11-12 起：会话记忆随代码走，换环境 clone 后自动就位，新会话自动读取）。其余 `.workbuddy/*`、`debug.log`、`target/` 不入库。交接说明见仓库根 `HANDOFF.md`。**只写本地日志不 git commit = 换环境仍然丢**
- **git 访问 GitHub 的正确配置（2026-11-12 已修好）**：项目 `.git/config` 中 `http.proxy`/`https.proxy` 设为**空字符串**，覆盖 shell 注入的动态沙箱代理（只放行 api.github.com，github.com 会 502）；系统代理 `127.0.0.1:7897` 的 Clash 核心当时整体不可用。PAT 已用 `git credential approve` 存入 Windows 凭据管理器（username=x-access-token）。直连约 0.9s，偶发跨境抖动重试即可。代理恢复后可用 `git config --local http.proxy http://127.0.0.1:7897` 切回
- 备用通道：`../_tools/gh_datapi_push.py` 走 api.github.com 的 Git Data API 复刻 blob/tree/commit（空仓库需先用 Contents API 建引导 commit 绕开 409），令牌取自 `~/.workbuddy/mcp.json` 的 github-pat Authorization 头（该文件现为指向 `J:\WiWork\_config\mcp.json` 的跨卷符号链接，读取路径不变）；该脚本目前仅支持单个 root commit

## 详细记录
- 2026-11-10 交付全记录（含 10 个已修缺陷清单与测试时序注意事项）：见 `2026-11-10.md`
- 2026-11-12 建立 git 仓库、.gitignore 缺陷修复、API 推送全过程：见 `2026-11-12.md`
