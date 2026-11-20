"""Fail-closed monthly rerun gate tested against the real workflow Bash.

Attempt >1 is explicitly denied before shard receipt creation and remote PUT.
This does not provide recovery and does not execute RFB/IA requests.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

_WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml"
_GUARD = "Refuse unproven monthly workflow attempt"


def _steps(job: str = "shards") -> list[dict]:
    config = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    return config["jobs"][job]["steps"]


def test_rerun_refusal_precedes_shard_write_and_preserves_receipts() -> None:
    steps = _steps()
    named = {step["name"]: step for step in steps if "name" in step}
    assert steps[0]["name"] == _GUARD
    assert named[_GUARD]["env"]["RUN_ATTEMPT"] == "${{ github.run_attempt }}"
    assert not named[_GUARD].get("continue-on-error", False)
    assert "exit 1" in named[_GUARD]["run"]
    assert "Restore previous receipt on job rerun" not in named
    assert "Require receipt stability across attempts" not in named
    persist = named["Persist shard production receipt before PUT"]
    assert persist["uses"].startswith("actions/upload-artifact@")
    assert not persist["with"].get("overwrite", False)
    names = [step.get("name") for step in steps]
    assert names.index(_GUARD) < names.index("Produce shard bytes and receipt — no remote write")
    assert names.index("Persist shard production receipt before PUT") < names.index(
        "Submit exactly the receipted bytes"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Workflow Bash requires Linux runner")
@pytest.mark.parametrize(
    ("attempt", "allowed"), [("1", True), ("2", False), ("11", False), ("", False)]
)
def test_rerun_guard_exits_fail_closed(attempt: str, allowed: bool) -> None:
    gate = _steps()[0]
    assert gate["name"] == _GUARD
    env = {**os.environ, "RUN_ATTEMPT": attempt}
    completed = subprocess.run(
        ["bash", "-c", gate["run"]],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert (completed.returncode == 0) is allowed, (
        f"attempt={attempt!r}, exit={completed.returncode}, "
        f"stdout={completed.stdout!r}, stderr={completed.stderr!r}"
    )
    if not allowed:
        assert "::error::" in completed.stdout
