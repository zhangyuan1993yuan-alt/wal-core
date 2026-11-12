# archive/ — v1.0.0 版本锁定前后的备份快照

本目录原本位于 `J:\WiWork\Project\WorkBuddy\_backups\wal\`，现在随仓库一起分发，换环境 `clone` 后即可取得。

| 文件 | 说明 |
|---|---|
| `wal-core-prelock-1.0.0-SNAPSHOT-20261111-170210.tar.gz` | **改版本号前**的完整源码快照（pom 仍为 `1.0.0-SNAPSHOT`）。这个状态不在 git 历史里，是本归档独有的价值 |
| `wal-core-1.0.0-release-20261111-170436.tar.gz` | 锁定 `1.0.0` 后的源码快照 |
| `wal-core-1.0.0-20261111-170436.jar` | 该次构建产出制品（177 class / 6 个 SPI 服务 / 342KB） |
| `SHA256SUMS.txt` | 上述三项的校验和 |
| `BACKUPS.md` | 备份当时的原始索引（含改动清单、验证结果、还原方法） |

校验：`cd archive && sha256sum -c SHA256SUMS.txt`

## BACKUPS.md 是历史文档，其中结论已部分过时

为保持归档原貌，**没有改动 BACKUPS.md 正文**。阅读时请注意：

1. 开头「这个项目不是 git 仓库、归档是唯一的版本回滚手段」以及 §5 缺口 1「无版本控制」**均已解决**：仓库现已 `git init` 并推送到 `https://github.com/zhangyuan1993yuan-alt/wal-core`，`git log`/`git diff` 才是现在的回滚手段；本目录只是快照留存。
2. §4 还原命令里的 `J:/WiWork/...` 是旧环境绝对路径，换环境后应改用本目录（`tar -xzf archive/<包名>`）。
3. §5 缺口 2（jar 的 MANIFEST 无 `Implementation-Version`）与缺口 3（本地 Maven 仓库残留 `1.0.0-SNAPSHOT` 目录）**仍未处理**。

## 内容边界

两个 tar 包各含 190 个文件的源码快照（259 条目，解压约 1.96MB），内含当时的 3 个旧版会话记忆：`2026-11-10.md`、`2026-11-11.md`、`MEMORY.md`。它们是**历史快照**，与仓库当前的 `.workbuddy/memory/`（4 个文件、内容已更新）不同；WorkBuddy 只读取 `.workbuddy/memory/` 路径，不会把 tar 内的旧记忆当作当前上下文。

归档内**不含**任何令牌、密码或 `mcp.json`。

## tools/ 里的备用脚本

仓库根的 `tools/gh_datapi_push.py` 是 `github.com` 不可达时走 `api.github.com` Git Data API 复刻提交树的备用通道。前提：本机有可用的 PAT 配置；**当前脚本只支持单个 root commit**（空仓库首次推送场景），有增量提交时需要扩展 parents 递归上传。日常 `git push` 正常时不必使用。
