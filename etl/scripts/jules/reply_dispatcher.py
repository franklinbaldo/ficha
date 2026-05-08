"""Reply dispatcher: send messages / cancel Jules sessions per a queue file.

Mirror of fanout_spawner.py, but for outbound actions on EXISTING
sessions instead of creating new ones. Triggered by push to this
branch when reply_queue.json changes.

Schema (etl/scripts/jules/reply_queue.json):

    [
      {
        "id": "unique-id-for-this-action",
        "type": "send_message",
        "session_id": "17967018632018838778",
        "message": "Yes, please use the existing strip_accents helper.",
        "_sent": false
      },
      {
        "id": "another-id",
        "type": "cancel",
        "session_id": "9821037508487127161",
        "_sent": false
      }
    ]

Idempotency: each entry's `id` is the de-dup key. The dispatcher
processes only entries WITHOUT `_sent: true`. To mark an entry as
done, manually edit the queue file to set `_sent: true` (the workflow
doesn't commit back — keeps blast radius small).

Pushing the same entry twice (without marking _sent) WILL send the
message twice — Jules treats them as distinct user messages.

Endpoints used (per developers.google.com/jules/api/reference/rest):
- POST /v1alpha/sessions/{id}:sendMessage  body: {"prompt": "..."}
- POST /v1alpha/sessions/{id}:approvePlan  body: {}

The v1alpha API does NOT expose a cancel/delete operation on
sessions. To stop a running session, either:
  (a) send a sendMessage asking the agent to stop and not commit
      anything (best-effort — Jules may still emit one more
      activity before parking)
  (b) cancel manually from jules.google.com/task/{id}
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
_HERE = Path(__file__).resolve().parent
QUEUE_PATH = Path(os.environ.get("REPLY_QUEUE", _HERE / "reply_queue.json"))


def _key() -> str:
    k = os.environ.get("JULES_API_KEY", "").strip()
    if not k:
        sys.exit("::error::JULES_API_KEY secret is not set")
    return k


def _api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Returns (status_code, response_json). status=0 on network error."""
    url = f"{JULES_API_BASE}/{path.lstrip('/')}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "X-Goog-Api-Key": _key(),
            "Content-Type": "application/json",
            "User-Agent": "ficha-jules-reply/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        print(f"::warning::Jules API {method} {path} → HTTP {e.code}: {body_text[:300]}")
        return e.code, {}
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"::warning::Jules API {method} {path} network error: {e}")
        return 0, {}


def _gh_pr_comment(pr: str, repo: str, body: str) -> None:
    Path("/tmp/reply_comment.md").write_text(body)
    subprocess.run(
        ["gh", "pr", "comment", pr, "--repo", repo, "--body-file", "/tmp/reply_comment.md"],
        check=False,
    )


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    target_pr = os.environ.get("REPLY_TARGET_PR", "31")

    if not QUEUE_PATH.exists():
        print(f"No queue file at {QUEUE_PATH} — nothing to do")
        return 0

    queue = json.loads(QUEUE_PATH.read_text())
    if not isinstance(queue, list):
        sys.exit(f"::error::{QUEUE_PATH} must be a JSON array")

    print(f"=== reply: {len(queue)} entries in {QUEUE_PATH} ===")
    if not queue:
        return 0

    sent: list[dict] = []
    skipped: list[dict] = []
    failed: list[dict] = []

    for entry in queue:
        eid = entry.get("id")
        if not eid:
            print(f"::warning::queue entry missing id, skipping: {entry}")
            failed.append({"entry": entry, "reason": "missing id"})
            continue

        if entry.get("_sent"):
            print(f"  SKIP (_sent): {eid}")
            skipped.append({"id": eid, "reason": "_sent"})
            continue

        action = entry.get("type", "send_message")
        sid = entry.get("session_id")
        if not sid:
            print(f"::warning::entry {eid} missing session_id")
            failed.append({"entry": entry, "reason": "missing session_id"})
            continue

        if action == "send_message":
            message = entry.get("message", "")
            if not message:
                failed.append({"entry": entry, "reason": "empty message"})
                continue
            print(f"  SEND_MESSAGE: {eid} → {sid} ({len(message)} chars)")
            status, resp = _api("POST", f"/sessions/{sid}:sendMessage", {"prompt": message})
            if 200 <= status < 300:
                sent.append(
                    {
                        "id": eid,
                        "type": action,
                        "session_id": sid,
                        "url": f"https://jules.google.com/task/{sid}",
                    }
                )
            else:
                failed.append(
                    {"entry": entry, "reason": f"HTTP {status}", "response": str(resp)[:200]}
                )

        elif action == "approve_plan":
            print(f"  APPROVE_PLAN: {eid} → {sid}")
            status, resp = _api("POST", f"/sessions/{sid}:approvePlan", {})
            if 200 <= status < 300:
                sent.append(
                    {
                        "id": eid,
                        "type": action,
                        "session_id": sid,
                        "url": f"https://jules.google.com/task/{sid}",
                    }
                )
            else:
                failed.append(
                    {"entry": entry, "reason": f"HTTP {status}", "response": str(resp)[:200]}
                )

        else:
            # 'cancel' is intentionally not implemented — the v1alpha API
            # has no cancel/delete endpoint. Use type=send_message with
            # a stop instruction, or cancel manually from
            # jules.google.com/task/{session_id}.
            failed.append(
                {
                    "entry": entry,
                    "reason": (
                        f"unknown type: {action!r} (supported: send_message, approve_plan)"
                    ),
                }
            )

    md = ["## 📨 Jules reply dispatch result\n"]
    if sent:
        md.append(f"### Sent ({len(sent)})\n")
        for s in sent:
            md.append(f"- `{s['id']}` — **{s['type']}** → [`{s['session_id']}`]({s['url']})")
        md.append(
            "\n**Reminder:** edit `etl/scripts/jules/reply_queue.json` "
            "and set `_sent: true` on these entries to prevent re-firing on the next push."
        )
    if skipped:
        md.append(f"\n### Skipped ({len(skipped)})\n")
        for s in skipped:
            md.append(f"- `{s['id']}` ({s['reason']})")
    if failed:
        md.append(f"\n### Failed ({len(failed)})\n")
        for f in failed:
            md.append(f"- `{f['entry'].get('id', '?')}` — {f['reason']}")
    if not (sent or skipped or failed):
        md.append("(empty queue)")
    _gh_pr_comment(target_pr, repo, "\n".join(md))

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
