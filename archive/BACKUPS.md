# wal-core 备份索引

备份位置：`J:\WiWork\Project\WorkBuddy\_backups\wal\`
**这个项目不是 git 仓库**（无 `.git`），因此这里的归档是唯一的版本回滚手段。

---

## 1. 归档清单

| 文件 | 时间 | 内容 | SHA-256 |
|---|---|---|---|
| `wal-core-prelock-1.0.0-SNAPSHOT-20261111-170210.tar.gz` | 2026-11-11 17:02:10 | **改版本号前的完整源码快照**（pom 仍为 `1.0.0-SNAPSHOT`） | `5c6e4b29…3ea1995` |
| `wal-core-1.0.0-release-20261111-170436.tar.gz` | 2026-11-11 17:04:36 | 锁定 `1.0.0` 后的源码快照（pom / README / docs 已同步） | `35d4e0d9…f48d6aee` |
| `wal-core-1.0.0-20261111-170436.jar` | 2026-11-11 17:04 | 该次构建产出制品（177 class，6 个 SPI 服务文件，342KB） | `85ac802c…df13b27d` |
| `SHA256SUMS.txt` | — | 上述三项的校验和 | — |

两个 tar 包均**排除 `target/`**（构建产物可由源码重建），包含 `src/`、`docs/`、`README.md`、`pom.xml`、`.workbuddy/memory/`，各 259 个条目。

校验：`sha256sum -c SHA256SUMS.txt`

---

## 2. 本次版本锁定改动

`1.0.0-SNAPSHOT` → `1.0.0`

| 文件 | 处数 | 说明 |
|---|---:|---|
| `wal/pom.xml` | 1 | 唯一真源（`<version>`） |
| `wal/README.md` | 5 | 依赖坐标 ×2、配置说明、"已定义未接线"标题、jar 文件名、文末版本行 |
| `wal/docs/PERF-wal.md` | 1 | 被测版本行 |
| `wal-demo/pom.xml` | 1 | 对 wal-core 的依赖坐标（不改则 demo 无法解析） |

**刻意未改**：`.workbuddy/memory/*.md` 下的历史工作日志 —— 那是历史记录，记录的是当时状态，
改了就失真。历史日志里出现 `1.0.0-SNAPSHOT` 属正常。

顺带修掉一处既存不一致：`WalVersion.LIBRARY_VERSION` 早已是 `"1.0.0"`，
而 pom 还是 `SNAPSHOT`，导致"运行时自报版本"与"制品坐标"不符。锁版本后两者一致。

---

## 3. 锁定后验证结果

| 检查项 | 命令 | 结果 |
|---|---|---|
| 全量测试 | `mvn -o clean install` | **433/433 通过**，0 失败 0 错误 |
| 制品名 | — | `target/wal-core-1.0.0.jar`（342KB） |
| 制品内容 | `unzip -l` | 177 个 class，6 个 `META-INF/services`（与 README 声明一致） |
| 零第三方运行时依赖 | `mvn -o dependency:tree -Dscope=compile` | **输出为空**，硬约束保持 |
| 安装到本地仓库 | — | `D:\Develop\apache-maven-repo\com\walcore\wal-core\1.0.0\` |
| 下游可解析 | `cd ../wal-demo && mvn -o clean compile` | **BUILD SUCCESS**，8 个源文件编译通过 |
| 残留 SNAPSHOT | `grep -rn 1.0.0-SNAPSHOT` | 源码/文档/两处 pom 均 **0 处**（记忆日志除外） |

> 注：`mvn -o clean install` 跑的是默认分组，`@Tag("perf")` 的性能用例仍被 pom 排除（符合设计）。
> 性能数据见 `docs/PERF-devbox.md`。

---

## 4. 还原方法

```bash
cd J:/WiWork/Project/WorkBuddy

# 回到锁定前（SNAPSHOT 状态）
tar -xzf _backups/wal/wal-core-prelock-1.0.0-SNAPSHOT-20261111-170210.tar.gz

# 回到 1.0.0 发布快照
tar -xzf _backups/wal/wal-core-1.0.0-release-20261111-170436.tar.gz
```

解包会覆盖同名文件。若只想取单个文件：`tar -xzOf <包> wal/pom.xml > pom.xml`。

---

## 5. 已知缺口（建议后续补）

1. **无版本控制**。当前靠 tar 归档回滚，无法 diff、无法分支、无法追溯单行改动来源。
   强烈建议 `git init` 并把本目录纳入版本控制（`target/` 加 `.gitignore`）。
2. **jar 的 MANIFEST 无版本字段**。当前只有 `Manifest-Version` / `Created-By` / `Build-Jdk-Spec`，
   运行时无法从制品直接读到 `1.0.0`。建议在 `maven-jar-plugin` 加 `manifestEntries`
   写入 `Implementation-Version` 与 `Implementation-Title`。
3. **本地仓库仍留着 `1.0.0-SNAPSHOT` 目录**（`D:\Develop\apache-maven-repo\com\walcore\wal-core\1.0.0-SNAPSHOT\`）。
   不影响使用，但会让"当前生效版本"产生歧义，可手工清理。
