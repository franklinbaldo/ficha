"""Poll all Jules sessions for this repo every 60s.

Listens via Jules API GET /v1alpha/sessions (filtered to current
sourceContext) and tracks state + last-seen activity per session.

Exits non-zero (and uploads an artifact via the workflow) as soon as
ANY session reaches a notable state:

- sessionCompleted activity emitted
- requirePlanApproval=true session has a planGenerated awaiting approval
- session has been idle (no new activity) for IDLE_THRESHOLD_MIN

Artifacts written to ./artifacts/<session_id>/:
- session.json   — full GET /sessions/{id} response
- activities.json — full activities response
- SUMMARY.md     — human-readable summary linkable from the GHA run

The artifact upload happens in the workflow (always: true), so even
a successful timeout exit still ships the latest snapshot.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

JULES_API_BASE = os.environ.get("JULES_API_BASE", "https://jules.googleapis.com/v1alpha")
POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "60"))
IDLE_THRESHOLD_MIN = int(os.environ.get("IDLE_THRESHOLD_MIN", "15"))
MAX_TOTAL_MIN = int(os.environ.get("MAX_TOTAL_MIN", "330"))
ARTIFACTS_DIR = Path(os.environ.get("ARTIFACTS_DIR", "./artifacts"))
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "")  # owner/repo (set by Actions)


def _key() -> str:
    k = os.environ.get("JULES_API_KEY", "").strip()
    if not k:
        # Graceful no-op when the secret isn't configured. The poller is
        # push-triggered on this branch, so every script edit fires it;
        # before JULES_API_KEY is actually set we don't want each push to
        # produce a "failed CI" notification. Caller (main()) checks for
        # this sentinel via _api_key_available().
        return ""
    return k


def _api_key_available() -> bool:
    return bool(os.environ.get("JULES_API_KEY", "").strip())


def _api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{JULES_API_BASE}/{path.lstrip('/')}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "X-Goog-Api-Key": _key(),
            "Content-Type": "application/json",
            "User-Agent": "ficha-jules-poller/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        print(f"::warning::Jules API {method} {path} → HTTP {e.code}: {body_text[:300]}")
        return {}


def _own_source() -> str:
    """sources/github/{owner}/{repo} for the current workflow's repo."""
    return f"sources/github/{GITHUB_REPO}"


def _list_active_sessions() -> list[dict]:
    """List sessions belonging to this repo. Paginate through all results."""
    sessions: list[dict] = []
    own = _own_source()
    page_token = None
    for _ in range(20):  # cap at 20 pages = 1000 sessions
        path = "/sessions?pageSize=50"
        if page_token:
            path += f"&pageToken={page_token}"
        resp = _api("GET", path)
        page = resp.get("sessions", []) or []
        for s in page:
            src = s.get("sourceContext", {}).get("source", "")
            if src == own:
                sessions.append(s)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return sessions


def _activity_summary(act: dict) -> str:
    """One-line summary of an activity for SUMMARY.md."""
    aid = act.get("id", "?")[:8]
    when = act.get("createTime", "")
    if "planGenerated" in act:
        n = len(act["planGenerated"].get("plan", {}).get("steps", []))
        return f"[{when}] planGenerated ({n} steps) ({aid})"
    if "planApproved" in act:
        return f"[{when}] planApproved ({aid})"
    if "sessionCompleted" in act:
        return f"[{when}] sessionCompleted ({aid})"
    if "progressUpdated" in act:
        title = act["progressUpdated"].get("title", "")[:80]
        return f"[{when}] progressUpdated: {title} ({aid})"
    if "agentMessage" in act:
        return f"[{when}] agentMessage ({aid})"
    if "userMessage" in act:
        return f"[{when}] userMessage ({aid})"
    keys = [k for k in act if k not in ("id", "name", "createTime", "originator", "artifacts")]
    return f"[{when}] {keys} ({aid})"


def _dump_session(sid: str, session: dict, activities: list[dict], reason: str) -> None:
    """Write artifact files for a triggering session."""
    out = ARTIFACTS_DIR / sid
    out.mkdir(parents=True, exist_ok=True)
    (out / "session.json").write_text(json.dumps(session, indent=2))
    (out / "activities.json").write_text(json.dumps({"activities": activities}, indent=2))

    title = session.get("title", "(no title)")
    web = f"https://jules.google.com/task/{sid}"
    pr_url = None
    for o in session.get("outputs", []) or []:
        if "pullRequest" in o:
            pr_url = o["pullRequest"].get("url")

    md = [f"# Jules session `{sid}`\n"]
    md.append(f"- **Title:** {title}")
    md.append(f"- **Source:** `{session.get('sourceContext', {}).get('source', '?')}`")
    md.append(f"- **Web:** {web}")
    md.append(f"- **Trigger reason:** {reason}")
    if pr_url:
        md.append(f"- **PR created:** {pr_url}")
    md.append(f"\n## Activities ({len(activities)})\n")
    for act in reversed(activities):  # API returns newest-first
        md.append(f"- {_activity_summary(act)}")
    (out / "SUMMARY.md").write_text("\n".join(md))


def _summary_index(triggers: list[tuple[str, str, dict]]) -> None:
    """Write artifacts/INDEX.md listing all sessions that triggered exit."""
    md = ["# Poller summary\n"]
    md.append(f"Poller exited because {len(triggers)} session(s) reached a notable state:\n")
    for sid, reason, sess in triggers:
        title = sess.get("title", "(no title)")
        md.append(f"- **{sid}** — {title} → {reason}")
    md.append("\nSee per-session directories for full session.json + activities.json + SUMMARY.md.")
    (ARTIFACTS_DIR / "INDEX.md").write_text("\n".join(md))


def main() -> int:
    if not _api_key_available():
        print("::warning::JULES_API_KEY is not set; poller exiting cleanly (no-op).")
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        (ARTIFACTS_DIR / "INDEX.md").write_text(
            "# Poller no-op\n\nJULES_API_KEY secret is not configured yet. "
            "The poller will resume tracking once the secret is set in repo settings."
        )
        return 0

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"=== Jules poller for {_own_source()} ===")
    print(f"  poll interval: {POLL_INTERVAL_S}s")
    print(f"  idle threshold: {IDLE_THRESHOLD_MIN} min")
    print(f"  max wall-time: {MAX_TOTAL_MIN} min")

    started_at = time.monotonic()
    # Per-session state: { sid: { "last_activity_at": monotonic_ts,
    #                             "last_activity_id": str|None,
    #                             "completed": bool,
    #                             "awaiting_approval": bool } }
    state: dict[str, dict] = {}
    triggers: list[tuple[str, str, dict]] = []

    while True:
        elapsed_min = (time.monotonic() - started_at) / 60
        if elapsed_min > MAX_TOTAL_MIN:
            print(f"=== Wall-time budget {MAX_TOTAL_MIN} min reached; exiting clean ===")
            # Always dump current snapshot as artifact for the upload step.
            for sess in _list_active_sessions():
                sid = sess.get("id") or sess.get("name", "").removeprefix("sessions/")
                if not sid:
                    continue
                acts = _api("GET", f"/sessions/{sid}/activities?pageSize=200").get("activities", [])
                _dump_session(sid, sess, acts, "(wall-time snapshot)")
            _summary_index([])
            return 0

        sessions = _list_active_sessions()
        print(f"[{elapsed_min:.1f}m] tracking {len(sessions)} session(s) for {_own_source()}")

        for sess in sessions:
            sid = sess.get("id") or sess.get("name", "").removeprefix("sessions/")
            if not sid:
                continue
            st = state.setdefault(
                sid,
                {
                    "last_activity_at": time.monotonic(),
                    "last_activity_id": None,
                    "completed": False,
                    "awaiting_approval": False,
                },
            )
            if st["completed"]:
                continue  # already triggered

            acts_resp = _api("GET", f"/sessions/{sid}/activities?pageSize=200")
            acts = acts_resp.get("activities", []) or []

            # Detect: new activity since last poll
            newest_id = acts[0].get("id") if acts else None
            if newest_id and newest_id != st["last_activity_id"]:
                st["last_activity_at"] = time.monotonic()
                st["last_activity_id"] = newest_id

            # Detect: sessionCompleted
            for a in acts:
                if "sessionCompleted" in a:
                    st["completed"] = True
                    triggers.append((sid, "sessionCompleted", sess))
                    _dump_session(sid, sess, acts, "sessionCompleted")
                    print(f"  → session {sid} COMPLETED")
                    break

            if st["completed"]:
                continue

            # Detect: planGenerated awaiting approval (when requirePlanApproval=true)
            require_approval = sess.get("requirePlanApproval", False)
            if require_approval and not st["awaiting_approval"]:
                # Find newest planGenerated; if no planApproved follows, it's awaiting.
                latest_plan_idx = None
                latest_approve_idx = None
                for i, a in enumerate(acts):
                    if "planGenerated" in a and latest_plan_idx is None:
                        latest_plan_idx = i
                    if "planApproved" in a and latest_approve_idx is None:
                        latest_approve_idx = i
                # API returns newest-first, so smaller index = newer.
                if latest_plan_idx is not None and (
                    latest_approve_idx is None or latest_plan_idx < latest_approve_idx
                ):
                    st["awaiting_approval"] = True
                    triggers.append((sid, "awaiting plan approval", sess))
                    _dump_session(sid, sess, acts, "awaiting plan approval")
                    print(f"  → session {sid} AWAITING APPROVAL")

            # Detect: idle for too long
            idle_min = (time.monotonic() - st["last_activity_at"]) / 60
            if idle_min > IDLE_THRESHOLD_MIN and not st["awaiting_approval"]:
                triggers.append((sid, f"idle for {idle_min:.0f} min", sess))
                st["awaiting_approval"] = True  # mark to avoid re-trigger
                _dump_session(sid, sess, acts, f"idle {idle_min:.0f} min")
                print(f"  → session {sid} IDLE for {idle_min:.0f}m")

        if triggers:
            print(f"=== {len(triggers)} session(s) triggered exit ===")
            _summary_index(triggers)
            for sid, reason, _sess in triggers:
                print(f"  - {sid}: {reason}")
            return 1

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    sys.exit(main())
