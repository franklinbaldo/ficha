"""Offline contract tests for the optional diagnostics dispatch boundary (#268)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = (
    Path(__file__).parents[2] / ".github" / "workflows" / "claude-diagnostics.yml"
)


def _steps() -> dict:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return {
        step["name"]: step
        for step in workflow["jobs"]["diagnose"]["steps"]
        if "name" in step
    }


def _resolve(
    tmp_path: Path,
    *,
    script: str = "",
    target_pr: str = "",
    month: str = "",
    event: str = "workflow_dispatch",
    branch: str = "main",
) -> tuple[subprocess.CompletedProcess[str], Path]:
    output = tmp_path / "github-output"
    output.unlink(missing_ok=True)
    env = os.environ | {
        "REQUESTED_SCRIPT": script,
        "REQUESTED_TARGET_PR": target_pr,
        "REQUESTED_MONTH": month,
        "GITHUB_OUTPUT": str(output),
        "GITHUB_EVENT_NAME": event,
        "GITHUB_REF_NAME": branch,
    }
    resolve_run = _steps()["Resolve inputs (defaults when triggered by push)"]["run"]
    result = subprocess.run(
        ["bash", "-e", "-c", resolve_run],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, output


def test_dispatch_strings_never_enter_shell_source() -> None:
    steps = _steps()
    for step in steps.values():
        run = step.get("run", "")
        assert "${{ inputs.script }}" not in run
        assert "${{ inputs.target_pr }}" not in run
        assert "${{ inputs.month }}" not in run
        assert "${{ steps.cfg.outputs.script }}" not in run

    assert (
        steps["Run diagnostic script"]["env"]["DIAGNOSTIC_SCRIPT"]
        == "${{ steps.cfg.outputs.script }}"
    )
    assert (
        steps["Build PR comment"]["env"]["DIAGNOSTIC_SCRIPT"]
        == "${{ steps.cfg.outputs.script }}"
    )
    for name in ("Build PR comment", "Post comment on PR"):
        assert "steps.cfg.outcome == 'success'" in steps[name]["if"]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Bash fixture uses POSIX temp paths"
)
def test_valid_push_and_manual_inputs_are_preserved(tmp_path: Path) -> None:
    push, push_output = _resolve(
        tmp_path, event="push", branch="claude/plan-next-steps-l7Dt6"
    )
    assert push.returncode == 0, (push.stdout, push.stderr)
    assert "script=zip_layout_poc\n" in push_output.read_text(encoding="utf-8")
    assert "target_pr=41\n" in push_output.read_text(encoding="utf-8")

    manual, manual_output = _resolve(
        tmp_path, script="phase1", target_pr="123", month="2026-11"
    )
    assert manual.returncode == 0, (manual.stdout, manual.stderr)
    assert manual_output.read_text(encoding="utf-8").splitlines()[:3] == [
        "script=phase1",
        "target_pr=123",
        "month=2026-11",
    ]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Bash fixture uses POSIX temp paths"
)
def test_dispatch_injection_is_refused_before_outputs(tmp_path: Path) -> None:
    marker = tmp_path / "unexpected-command"
    shell_payload = f"$(touch {marker})"
    cases = [
        {"script": f"phase1{shell_payload}"},
        {"script": "../phase1"},
        {"script": "phase1\nforce_fail=1"},
        {"target_pr": f"31{shell_payload}"},
        {"target_pr": "31\nscript=phase1"},
        {"month": "2026-11\nscript=phase1"},
        {"month": "2026-13"},
    ]
    for case in cases:
        result, output = _resolve(tmp_path, **case)
        assert result.returncode != 0, (case, result.stdout, result.stderr)
        assert "::error::" in result.stdout
        assert not marker.exists()
        assert not output.exists() or not output.read_text(encoding="utf-8")
