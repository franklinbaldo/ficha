"""Spawn a Jules session via API and exit. Polling is handled by the
separate `claude-jules-poller.yml` workflow.

POSTs to /v1alpha/sessions, prints the new session ID + web URL, posts
a single PR comment, and exits in seconds. The global poller picks up
the new session on its next 60s tick.
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
            "User-Agent": "ficha-jules-spawner/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        sys.exit(f"::error::Jules API {method} {path} → HTTP {e.code}: {body_text[:500]}")


def main() -> int:
    if len(sys.argv) < 2:
        sys.exit("Usage: spawn_session.py <prompt_file>")
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

    body = (
        f"## 🚀 Jules session spawned: `{title}`\n\n"
        f"- **Session:** [`{sid}`]({web_url})\n"
        f"- **Source:** `{source}` @ `{branch}`\n"
        f"- **Mode:** `{automation}`, plan approval: `{'manual' if require_approval else 'auto'}`\n"
        f"- **Prompt size:** {len(prompt)} chars\n\n"
        f"State changes (completion, awaiting approval, idle) will trigger "
        f"the global poller workflow `claude-jules-poller.yml`, which uploads "
        f"a session-snapshot artifact and fails so PR subscribers wake up."
    )
    Path("/tmp/spawn_comment.md").write_text(body)
    subprocess.run(
        ["gh", "pr", "comment", target_pr, "--repo", repo, "--body-file", "/tmp/spawn_comment.md"],
        check=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
