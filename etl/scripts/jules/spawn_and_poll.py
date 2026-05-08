"""Spawn a Jules session via API, poll activities, post each as a PR comment.

Per developers.google.com/jules/api:

- Auth: X-Goog-Api-Key header
- POST /v1alpha/sessions       create session
- GET  /v1alpha/sessions/{id}  get session (includes outputs.pullRequest after AUTO_CREATE_PR)
- GET  /v1alpha/sessions/{id}/activities  paginated activity stream
- POST /v1alpha/sessions/{id}:approvePlan  if requirePlanApproval=true
- POST /v1alpha/sessions/{id}:sendMessage  reply to agent

Activity types observed:
- planGenerated      — agent emits a plan {steps:[{title,index}]}
- planApproved       — user (or auto-approve) accepted plan
- progressUpdated    — agent emits progress {title, description?, artifacts?}
- sessionCompleted   — terminal success

Terminal detection: an activity with `sessionCompleted` field present.
Failures: HTTP errors during poll, or sessions that produce no activities
within MAX_IDLE_MIN minutes.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

JULES_API_BASE = os.environ.get("JULES_API_BASE", "https://jules.googleapis.com/v1alpha")
POLL_INTERVAL_S = 30
MAX_IDLE_MIN = 30  # if no new activity in this window, declare stuck
MAX_TOTAL_MIN = 350  # workflow timeout is 360


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
            "User-Agent": "ficha-jules-runner/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        sys.exit(f"::error::Jules API {method} {path} → HTTP {e.code}: {body_text[:500]}")


def _gh_pr_comment(pr: str, repo: str, body: str) -> None:
    """Post a comment on the target PR using gh CLI (already authenticated via GH_TOKEN)."""
    try:
        with open("/tmp/jules_comment.md", "w") as f:
            f.write(body)
        subprocess.run(
            ["gh", "pr", "comment", pr, "--repo", repo, "--body-file", "/tmp/jules_comment.md"],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        # Don't fail the whole job over a missed comment
        print(f"::warning::gh pr comment failed: {e.stderr.decode('utf-8', 'replace')}")


def _format_activity(act: dict) -> tuple[str, bool]:
    """Render an activity as markdown. Returns (markdown, is_terminal)."""
    aid = act.get("id", "?")[:8]
    when = act.get("createTime", "")
    who = act.get("originator", "?")

    if "planGenerated" in act:
        plan = act["planGenerated"].get("plan", {})
        steps = plan.get("steps", [])
        body = f"**🧭 Plan generated** ({who}, `{aid}`, {when})\n\n"
        for i, s in enumerate(steps):
            body += f"{i + 1}. {s.get('title', '?')}\n"
        return body, False

    if "planApproved" in act:
        return f"**✅ Plan approved** ({who}, `{aid}`)", False

    if "progressUpdated" in act:
        pu = act["progressUpdated"]
        title = pu.get("title", "(no title)")
        desc = pu.get("description", "")
        body = f"**⚙️  {title}** (`{aid}`, {when})"
        if desc:
            body += f"\n\n{desc[:1500]}"
        # Surface artifact previews if any
        for a in act.get("artifacts", []):
            if "bashOutput" in a:
                cmd = a["bashOutput"].get("command", "").strip()
                out = a["bashOutput"].get("output", "")[:500]
                body += f"\n\n```bash\n$ {cmd}\n{out}\n```"
            elif "changeSet" in a:
                patch = a["changeSet"].get("gitPatch", {})
                if patch.get("suggestedCommitMessage"):
                    body += f"\n\n📝 Commit: `{patch['suggestedCommitMessage'].splitlines()[0]}`"
        return body, False

    if "sessionCompleted" in act:
        return f"**🏁 Session completed** (`{aid}`, {when})", True

    if "userMessage" in act:
        return (
            f"**💬 User message** (`{aid}`)\n\n{act['userMessage'].get('prompt', '')[:1000]}",
            False,
        )

    if "agentMessage" in act:
        return (
            f"**🤖 Agent message** (`{aid}`)\n\n{act['agentMessage'].get('text', '')[:1500]}",
            False,
        )

    # Unknown shape — dump the keys
    keys = [
        k for k in act.keys() if k not in ("id", "name", "createTime", "originator", "artifacts")
    ]
    return f"**❓ Activity** (`{aid}`, fields: {keys})", False


def main() -> int:
    if len(sys.argv) < 2:
        sys.exit("Usage: spawn_and_poll.py <prompt_file>")
    prompt = Path(sys.argv[1]).read_text()

    title = os.environ["JULES_TITLE"]
    source = os.environ["JULES_SOURCE"]
    branch = os.environ.get("JULES_STARTING_BRANCH", "main")
    automation = os.environ.get("JULES_AUTOMATION_MODE", "AUTO_CREATE_PR")
    require_approval = os.environ.get("JULES_REQUIRE_PLAN_APPROVAL", "false").lower() == "true"
    target_pr = os.environ["JULES_TARGET_PR"]
    repo = os.environ["JULES_REPO"]

    print(f"=== Spawning Jules session: {title!r} ===")
    print(f"  source:     {source}")
    print(f"  branch:     {branch}")
    print(f"  automation: {automation}")
    print(f"  approval:   {'manual' if require_approval else 'auto'}")
    print(f"  prompt:     {len(prompt)} chars")

    create = _api(
        "POST",
        "/sessions",
        {
            "title": title,
            "prompt": prompt,
            "requirePlanApproval": require_approval,
            "automationMode": automation,
            "sourceContext": {
                "source": source,
                "githubRepoContext": {"startingBranch": branch},
            },
        },
    )

    sid = create.get("id") or create.get("name", "").removeprefix("sessions/")
    if not sid:
        sys.exit(f"::error::create session returned no id: {create}")

    web_url = f"https://jules.google.com/task/{sid}"
    print(f"=== Session created: {sid} ===")
    print(f"  URL: {web_url}")

    _gh_pr_comment(
        target_pr,
        repo,
        f"## 🚀 Jules session spawned: `{title}`\n\n"
        f"- **Session:** [`{sid}`]({web_url})\n"
        f"- **Source:** `{source}` @ `{branch}`\n"
        f"- **Mode:** `{automation}`, plan approval: `{'manual' if require_approval else 'auto'}`\n"
        f"- **Prompt size:** {len(prompt)} chars\n\n"
        f"Activities will be posted here as they arrive (~30s polling cadence).",
    )

    # Poll loop
    seen_ids: set[str] = set()
    last_activity_at = time.monotonic()
    started_at = time.monotonic()

    while True:
        elapsed_min = (time.monotonic() - started_at) / 60
        if elapsed_min > MAX_TOTAL_MIN:
            print(f"::error::Session ran past {MAX_TOTAL_MIN} min wall-time budget")
            return 1

        idle_min = (time.monotonic() - last_activity_at) / 60
        if idle_min > MAX_IDLE_MIN:
            print(f"::error::No new activities in {MAX_IDLE_MIN} min — assuming stuck")
            return 1

        try:
            resp = _api("GET", f"/sessions/{sid}/activities?pageSize=50")
        except SystemExit:
            raise

        activities: list[dict[str, Any]] = resp.get("activities", [])
        new = [a for a in activities if a.get("id") and a["id"] not in seen_ids]

        if new:
            last_activity_at = time.monotonic()
            # API returns newest-first; reverse for chronological posting
            for act in reversed(new):
                seen_ids.add(act["id"])
                md, is_terminal = _format_activity(act)
                print(f"--- {md.splitlines()[0]} ---")
                _gh_pr_comment(target_pr, repo, md)
                if is_terminal:
                    # Fetch session to get the PR url if AUTO_CREATE_PR
                    sess = _api("GET", f"/sessions/{sid}")
                    pr_url = None
                    for out in sess.get("outputs", []) or []:
                        if "pullRequest" in out:
                            pr_url = out["pullRequest"].get("url")
                    final = f"## 🏁 Jules session done\n\n[Session `{sid}`]({web_url})"
                    if pr_url:
                        final += f"\n\n**PR created:** {pr_url}"
                    _gh_pr_comment(target_pr, repo, final)
                    return 0
        else:
            print(f"  …no new activities ({elapsed_min:.1f}m elapsed, {idle_min:.1f}m idle)")

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    sys.exit(main())
