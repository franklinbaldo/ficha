"""Fan-out spawner: read spawn_queue.json, spawn one Jules session per entry.

Triggered by push to this branch when the queue file changes (see
.github/workflows/claude-jules-fanout.yml). Idempotent: before
spawning, lists existing sessions for this repo and skips any whose
title contains the queue entry's `id` (so re-pushing the same queue
doesn't double-spawn).

Queue schema (etl/scripts/jules/spawn_queue.json):

    [
      {
        "id": "w41-2026-05-08",
        "title": "W4.1 length-14 branch",
        "prompt_file": "w41_search_length14_branch",
        "starting_branch": "main",
        "automation_mode": "AUTO_CREATE_PR",
        "require_plan_approval": false
      },
      ...
    ]

The `id` is embedded in the spawned session title as ` [<id>]` so the
de-dup check can find it. Re-using an `id` in a new queue entry is a
no-op; bump the id (e.g. add a date suffix) to force a respawn.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

JULES_API_BASE = os.environ.get("JULES_API_BASE", "https://jules.googleapis.com/v1alpha")
# Path resolution: when this script runs from the GHA workflow, cwd is
# the etl/ subdirectory; when run from the repo root, cwd is repo root.
# Resolve the queue and prompts dir relative to *this file* so both
# work — the layout is fixed: etl/scripts/jules/{spawn_queue.json, prompts/}.
_HERE = Path(__file__).resolve().parent
QUEUE_PATH = Path(os.environ.get("SPAWN_QUEUE", _HERE / "spawn_queue.json"))
PROMPTS_DIR = _HERE / "prompts"


def _key() -> str:
    k = os.environ.get("JULES_API_KEY", "").strip()
    if not k:
        sys.exit("::error::JULES_API_KEY secret is not set")
    return k


def _api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{JULES_API_BASE}/{path.lstrip('/')}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "X-Goog-Api-Key": _key(),
            "Content-Type": "application/json",
            "User-Agent": "ficha-jules-fanout/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        print(f"::error::Jules API {method} {path} → HTTP {e.code}: {body_text[:500]}")
        return {}
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        # Don't let a single slow paginated request stall the whole
        # fan-out — the de-dup is best-effort. Log and treat as empty.
        print(f"::warning::Jules API {method} {path} timed out / network error: {e}")
        return {}


def _list_existing_titles(repo: str) -> set[str]:
    """Titles of all sessions for this repo's source.

    Paginates the global session list filtered client-side. Capped at 5
    pages (250 sessions) — the Jules API doesn't expose a server-side
    source filter, so a maintainer with many sessions across other
    projects would otherwise pay 20 × HTTP-roundtrip per fan-out. 250
    is enough to de-dup against the last few weeks of activity for any
    one repo; the fan-out de-dup is best-effort, not safety-critical
    (worst case: a duplicate session, easy to cancel).
    """
    own = f"sources/github/{repo}"
    titles: set[str] = set()
    page_token = None
    for _ in range(5):
        path = "/sessions?pageSize=50"
        if page_token:
            path += f"&pageToken={page_token}"
        resp = _api("GET", path)
        for s in resp.get("sessions", []) or []:
            if s.get("sourceContext", {}).get("source") == own:
                titles.add(s.get("title", ""))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return titles


def _gh_pr_comment(pr: str, repo: str, body: str) -> None:
    Path("/tmp/fanout_comment.md").write_text(body)
    subprocess.run(
        ["gh", "pr", "comment", pr, "--repo", repo, "--body-file", "/tmp/fanout_comment.md"],
        check=False,
    )


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    target_pr = os.environ.get("FANOUT_TARGET_PR", "31")

    if not QUEUE_PATH.exists():
        print(f"No queue file at {QUEUE_PATH} — nothing to spawn")
        return 0

    queue = json.loads(QUEUE_PATH.read_text())
    if not isinstance(queue, list):
        sys.exit(f"::error::{QUEUE_PATH} must be a JSON array")

    print(f"=== fanout: {len(queue)} entries in {QUEUE_PATH} ===")

    if not queue:
        print("Queue is empty — nothing to spawn")
        return 0

    existing = _list_existing_titles(repo)
    print(f"Found {len(existing)} existing session(s) for sources/github/{repo}")

    spawned: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    for entry in queue:
        eid = entry.get("id")
        if not eid:
            print(f"::warning::queue entry missing id, skipping: {entry}")
            failed.append({"entry": entry, "reason": "missing id"})
            continue

        title = entry.get("title", "(no title)")
        decorated_title = f"{title} [{eid}]"

        # Hard skip: explicit `_done` (terminal: merged/discarded) or
        # `_spawning` (currently in flight) marker. Honored independently
        # of the live-session de-dup because Jules sessions.list pagination
        # caps at 5 pages — sessions older than that window or running on
        # a busy account can be missed by the title-based check, which
        # caused W2.1 and W10 to spawn duplicates after the _done fix
        # commit (9a98cf07) re-triggered the fanout.
        #
        # State machine for a queue entry:
        #   pending  → no _spawning, no _done   (will spawn)
        #   running  → _spawning: <session_id>  (skip; awaiting completion)
        #   terminal → _done: <merged|discarded|note>  (skip; remember)
        # After the fan-out posts a "Spawned" comment, the maintainer
        # should manually edit the entry to set _spawning. After the PR
        # is merged or the session is discarded, swap _spawning → _done.
        if entry.get("_done"):
            print(f"  SKIP (_done={entry['_done']}): {decorated_title}")
            skipped.append({"id": eid, "title": title, "reason": "_done"})
            continue
        if entry.get("_spawning"):
            print(f"  SKIP (_spawning={entry['_spawning']}): {decorated_title}")
            skipped.append({"id": eid, "title": title, "reason": "_spawning"})
            continue

        # De-dup by id-suffixed title (best-effort against active sessions)
        if any(decorated_title in t for t in existing):
            print(f"  SKIP (already exists): {decorated_title}")
            skipped.append({"id": eid, "title": title, "reason": "already exists"})
            continue

        prompt_file = entry.get("prompt_file")
        if not prompt_file:
            print(f"::warning::entry {eid} missing prompt_file, skipping")
            failed.append({"entry": entry, "reason": "missing prompt_file"})
            continue
        prompt_path = PROMPTS_DIR / f"{prompt_file}.md"
        if not prompt_path.exists():
            print(f"::error::entry {eid} prompt_file not found: {prompt_path}")
            failed.append({"entry": entry, "reason": f"prompt_file not found: {prompt_path}"})
            continue
        prompt = prompt_path.read_text()

        # `source` defaults to the workflow's own repo, but a queue entry
        # may target a different Jules-registered source (e.g.,
        # 'sources/github/franklinbaldo/verne' to delegate work on the
        # verne CLI from this ficha repo's queue). NB: cross-source
        # sessions won't be picked up by claude-jules-poller.yml (which
        # filters to this repo's source); use claude-jules-inspect.yml
        # to monitor them on-demand.
        source = entry.get("source") or f"sources/github/{repo}"
        body = {
            "title": decorated_title,
            "prompt": prompt,
            "requirePlanApproval": entry.get("require_plan_approval", False),
            "automationMode": entry.get("automation_mode", "AUTO_CREATE_PR"),
            "sourceContext": {
                "source": source,
                "githubRepoContext": {"startingBranch": entry.get("starting_branch", "main")},
            },
        }

        print(f"  SPAWN: {decorated_title} (prompt={prompt_file}, {len(prompt)} chars)")
        resp = _api("POST", "/sessions", body)
        sid = resp.get("id") or resp.get("name", "").removeprefix("sessions/")
        if not sid:
            print(f"::error::spawn failed for {eid}: {resp}")
            failed.append({"entry": entry, "reason": f"no session id in response: {resp}"})
            continue

        spawned.append(
            {
                "id": eid,
                "title": decorated_title,
                "session_id": sid,
                "url": f"https://jules.google.com/task/{sid}",
            }
        )

    # Post a single roll-up comment
    md = ["## 📡 Jules fan-out result\n"]
    if spawned:
        md.append(f"### Spawned ({len(spawned)})\n")
        for s in spawned:
            md.append(f"- [`{s['session_id']}`]({s['url']}) — **{s['title']}**")
        md.append(
            '\n**Reminder:** add `"_spawning": "<session_id>"` to each '
            "spawned entry in `spawn_queue.json` to prevent re-fires on the "
            "next push. Swap to `_done` once the session's PR is merged "
            "or discarded."
        )
    if skipped:
        md.append(f"\n### Skipped (already running) ({len(skipped)})\n")
        for s in skipped:
            md.append(f"- `{s['id']}` — {s['title']}")
    if failed:
        md.append(f"\n### Failed ({len(failed)})\n")
        for f in failed:
            md.append(f"- `{f['entry'].get('id', '?')}` — {f['reason']}")
    if not (spawned or skipped or failed):
        md.append("(empty queue)")
    _gh_pr_comment(target_pr, repo, "\n".join(md))

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
