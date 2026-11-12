"""Push the local HEAD commit to GitHub via the Git Data API.

Used when github.com (git smart HTTP) is unreachable but api.github.com is.
The script recreates blobs, tree and commit so the remote commit SHA can be
made identical to the local one, then points refs/heads/main at it.
The token is read from ~/.workbuddy/mcp.json and never printed.
"""
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

ROOT = r"J:\WiWork\Project\WorkBuddy\wal"
OWNER, REPO = "zhangyuan1993yuan-alt", "wal-core"
API = "https://api.github.com"
BASE = f"/repos/{OWNER}/{REPO}"

cfg = json.load(open(os.path.expanduser("~/.workbuddy/mcp.json"), encoding="utf-8"))
auth = cfg["mcpServers"]["github-pat"]["headers"]["Authorization"]
TOKEN = auth.split(" ", 1)[1] if " " in auth else auth

# api.github.com is reachable directly; bypass the local proxy entirely.
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def api(method, path, payload=None):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(API + path, data=data, method=method, headers={
        "Authorization": "token " + TOKEN,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "wal-core-git-data-push",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    for attempt in range(4):
        try:
            with OPENER.open(request, timeout=25) as resp:
                body = resp.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")[:300]
            if e.code in (403, 429, 500, 502, 503) and attempt < 3:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {detail}")
        except Exception:
            if attempt < 3:
                time.sleep(1.0 * (attempt + 1))
                continue
            raise


def git(*args):
    return subprocess.check_output(["git"] + list(args), cwd=ROOT)


def iso_from_git(epoch, tz):
    sign = 1 if tz[0] == "+" else -1
    offset = timedelta(hours=int(tz[1:3]), minutes=int(tz[3:5])) * sign
    return datetime.fromtimestamp(int(epoch), tz=timezone(offset)).isoformat()


def ensure_object_database():
    """A brand-new empty GitHub repo has no git object store; the Git Data API
    replies 409 until a first commit exists. Seed it via the Contents API.
    That bootstrap commit is later detached when main is force-moved to the
    real root commit, so it never appears in the branch history."""
    try:
        api("GET", BASE + "/git/ref/heads/main")
        print("仓库已有 main 引用")
        return
    except RuntimeError:
        pass
    content = base64.b64encode(b"initialize git object database\n").decode("ascii")
    api("PUT", BASE + "/contents/.gitkeep",
        {"message": "chore: initialize repository", "content": content})
    print("已创建引导提交，git 对象库就绪")
    for _ in range(6):
        try:
            api("GET", BASE + "/git/ref/heads/main")
            return
        except RuntimeError:
            time.sleep(1)
    print("警告: main 引用尚未可见，继续尝试")


def main():
    local_head = git("rev-parse", "HEAD").decode().strip()
    local_tree = git("rev-parse", "HEAD^{tree}").decode().strip()
    print(f"本地 HEAD : {local_head}")
    print(f"本地 tree: {local_tree}")
    ensure_object_database()

    # --- 1. enumerate the exact blobs stored in the local commit (not working-copy files) ---
    entries = []
    for rec in git("ls-tree", "-r", "-z", "HEAD").split(b"\0"):
        if not rec:
            continue
        meta, path = rec.split(b"\t", 1)
        mode, typ, sha = meta.split()
        entries.append((mode.decode(), typ.decode(), sha.decode(), path.decode("utf-8")))
    print(f"待上传对象: {len(entries)} 个 blob")

    # --- 2. create blobs, verify each SHA matches the local one ---
    def create_blob(entry):
        mode, typ, sha, path = entry
        content = git("cat-file", "blob", sha)
        body = {"content": base64.b64encode(content).decode("ascii"), "encoding": "base64"}
        result = api("POST", BASE + "/git/blobs", body)
        return path, sha, result.get("sha")

    mismatches = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(create_blob, e) for e in entries]
        done = 0
        for fut in as_completed(futures):
            path, want, got = fut.result()
            done += 1
            if want != got:
                mismatches.append((path, want, got))
            if done % 40 == 0:
                print(f"  blob 进度 {done}/{len(entries)}")
    print(f"blob 上传完成: {len(entries) - len(mismatches)}/{len(entries)} SHA 一致")
    if mismatches:
        print("SHA 不一致示例:", mismatches[:3])
        return 1

    # --- 3. recreate the tree ---
    tree_payload = {"tree": [
        {"path": p, "mode": m, "type": t, "sha": s} for m, t, s, p in entries
    ]}
    remote_tree = api("POST", BASE + "/git/trees", tree_payload)["sha"]
    print(f"远端 tree: {remote_tree}  {'MATCH' if remote_tree == local_tree else 'DIFFERENT'}")

    # --- 4. recreate the commit with identical author/committer/message ---
    raw = git("cat-file", "commit", "HEAD").decode("utf-8")
    header, message = raw.split("\n\n", 1)
    author_line = next(l for l in header.splitlines() if l.startswith("author "))
    committer_line = next(l for l in header.splitlines() if l.startswith("committer "))
    pat = r"^(?:author|committer) (.*) <(.*)> (\d+) ([+-]\d{4})$"

    def person(line):
        m = re.match(pat, line)
        if not m:
            raise RuntimeError("无法解析身份行: " + line)
        return {"name": m.group(1), "email": m.group(2), "date": iso_from_git(m.group(3), m.group(4))}

    author, committer = person(author_line), person(committer_line)
    base_commit = {"tree": remote_tree, "parents": [], "author": author, "committer": committer}

    remote_commit = None
    for variant, msg in (("去除尾部换行", message.rstrip("\n")), ("保留尾部换行", message)):
        result = api("POST", BASE + "/git/commits", dict(base_commit, message=msg))
        sha = result["sha"]
        print(f"远端 commit({variant}): {sha}  {'MATCH' if sha == local_head else 'DIFFERENT'}")
        if sha == local_head:
            remote_commit = sha
            break
        if remote_commit is None:
            remote_commit = sha
            fallback = result
    if remote_commit is None:
        return 1

    # --- 5. point main at the commit (force: the real commit is an
    #        unrelated root, so this detaches the bootstrap commit) ---
    try:
        api("GET", BASE + "/git/ref/heads/main")
        ref_exists = True
    except RuntimeError:
        ref_exists = False
    if ref_exists:
        api("PATCH", BASE + "/git/refs/heads/main", {"sha": remote_commit, "force": True})
        print("已将 main 强制指向复刻的 root commit（引导提交脱离历史）")
    else:
        api("POST", BASE + "/git/refs", {"ref": "refs/heads/main", "sha": remote_commit})
        print("已创建 refs/heads/main")
    try:
        api("PATCH", BASE, {"default_branch": "main"})
        print("默认分支已设为 main")
    except RuntimeError as e:
        print("默认分支设置跳过:", str(e)[:120])

    with open(os.path.join(ROOT, "target", "bench", "push_result.json"), "w", encoding="utf-8") as f:
        json.dump({"local_head": local_head, "remote_head": remote_commit,
                   "local_tree": local_tree, "remote_tree": remote_tree,
                   "matched": remote_commit == local_head}, f, indent=2)
    print("RESULT:", "SHA 完全一致，远端与本地仓库同步" if remote_commit == local_head
          else "SHA 不同，需要本地重新对齐")
    return 0


if __name__ == "__main__":
    sys.exit(main())
