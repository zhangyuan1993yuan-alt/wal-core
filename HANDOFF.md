# HANDOFF — 换环境 / 新会话接手指南

> 本文件随代码入库。用于「换一台机器」或「开一个新对话」时快速恢复上下文。
> 最后更新：2026-11-12

## 1. 记忆分三层，能带走的是哪一层

| 层 | 位置 | 换环境后 | 要手动搬吗 |
|---|---|---|---|
| 云端长期画像 + 历史会话检索 | WorkBuddy 服务端，按账号管理 | 登录同一账号自动恢复 | 不用 |
| 用户级跨项目偏好 | `~/.workbuddy/MEMORY.md` | 会丢 | 要（当前该文件不存在） |
| **项目工作区记忆** | `<仓库>/.workbuddy/memory/` | **已纳入本仓库**，clone 后自动就位 | 不用 |

新会话启动时会自动读取工作区 `.workbuddy/memory/MEMORY.md` 和按日日志。所以只要 clone 本仓库、并在该目录打开任务，历史上下文就回来了。这是本次改动的核心目的。

## 2. 资产地图：哪些 clone 得到，哪些得不到

**仓库内（clone 即有）**
- `src/` 177 个源文件、`pom.xml`、`README.md`、`docs/` 下的设计与性能文档
- `.workbuddy/memory/` — 4 个文件：`MEMORY.md`（长期笔记）+ `2026-11-10/11/12.md`（按日日志）
- `HANDOFF.md` — 本文件

**另一个仓库（同样 Private，需单独 clone）**
- `wal-core-demo` → https://github.com/zhangyuan1993yuan-alt/wal-core-demo ：Disruptor + WAL 分片撮合 Demo，11 个文件 / 38KB（8 个 Java 源 + README + pom + `.gitignore`），首次提交 `12dbf19`。运行期 `match-demo-data/`（约 3.1GB WAL 段）与 `target/` 已由 `.gitignore` 根锚定规则排除，clone 后首次运行自动重建。

**仓库外（clone 拿不到，需单独带走）**

| 资产 | 旧环境位置 | 说明 |
|---|---|---|
| 版本快照 | `../_backups/wal/` | 改前/改后 tar、`wal-core-1.0.0.jar`、SHA256SUMS、`BACKUPS.md` 索引 |
| 推送备用脚本 | `../_tools/gh_datapi_push.py` | `github.com` 不可达时走 Git Data API 的备用通道 |
| MCP 配置 | `J:\WiWork\_config\mcp.json` | github-pat 令牌真身；`~\.workbuddy\mcp.json` 是指向它的符号链接 |

## 3. 新机器恢复清单

1. `git clone https://github.com/zhangyuan1993yuan-alt/wal-core.git`（Private，需凭据），在 clone 出来的目录打开 WorkBuddy 任务。
2. 先读 `.workbuddy/memory/MEMORY.md`：重点是「维护必读」与「下版本待办」两节，再按需翻按日日志。
3. 环境：JDK 17 + Maven。本地仓库已有 junit 时 `mvn -o clean install`，否则联网跑 `mvn clean install`；期望 **433/433 通过**。
4. 性能验收只在 **Linux NVMe 独占机**做，命令见 `docs/PERF-wal.md` §2；Windows 数字（`docs/PERF-devbox.md`）只是本机快照，不作验收证据。
5. 需要 GitHub 写权限时：PAT 存入系统凭据管理器 + 按需配置代理，细节见 `MEMORY.md`「版本控制与推送」。
6. 要用 Demo：`git clone https://github.com/zhangyuan1993yuan-alt/wal-core-demo.git`（Private）。先按第 3 步把 `wal-core` 装进本地 Maven 仓库（Demo 依赖坐标 `com.walcore:wal-core:1.0.0`），再按其 README「运行」章节执行；目录名以实际 clone 结果为准。

## 4. 旧环境信息不要照搬

记忆日志里的下列内容是**旧机器快照**，换环境一律按实际重配：

- 绝对路径 `J:/WiWork/...`、`C:\Users\Y\...`
- 代理端口 `7897`（Clash Verge）、shell 沙箱动态代理端口、git 的 `http.proxy=""` 直连覆盖
- Windows 凭据管理器条目 `git:https://github.com`
- `~\.workbuddy\mcp.json` 的跨卷符号链接（新机器重建，或直接写普通文件）
- 内网 IP、主机名、账号相关信息

## 5. 状态速览（详见 MEMORY.md）

- `v1.0.0`，433 用例全绿，零第三方运行时依赖，6 个 SPI 服务文件
- 远端 Private / 分支 `main` / 首次提交 `f6a2e5d`
- 待办（优先级序）：Linux 目标机性能实测 → `ReplicationSink` 真实投递 → 万段级 RTO 并行恢复 → 在线 `verify()` → `appendBatch` 混合类型与幂等键 → 组提交退避分区化

## 6. 维护约定（决定「下次换环境还灵不灵」）

- 每次完成实质工作后，**追加**当日 `.workbuddy/memory/YYYY-MM-DD.md`（append-only，不要覆盖），并**随代码一起提交**。只写在本地不提交 = 换环境仍然丢。
- 日志满 30 天，按主题蒸馏进 `MEMORY.md`，再删旧日志。
- 改 `.gitignore` 前先读 `MEMORY.md` 的「版本控制与推送」：运行期目录规则**必须根锚定**；改完用 `git ls-files --others --ignored --exclude-standard` 复核（历史上一条裸 `plugin/` 误伤了 12 个 Java 源文件）。
- 敏感信息：本仓库是 **Private**，记忆里含本机路径、内网 IP、账号名（不含令牌/密码）。若将来转 Public，先清理 `.workbuddy/memory/` 再改可见性。

## 7. 给新会话的第一句话

> 「先读 `HANDOFF.md` 和 `.workbuddy/memory/MEMORY.md`，按『下版本待办』接着做；性能数字只在 Linux NVMe 上验收。」
