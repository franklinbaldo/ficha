"""Offline regression tests for status-only diagnostic PR comments (#472)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "claude-diagnostics.yml"
CANARY = "SYNTHETIC-DIAGNOSTIC-CANARY-DO-NOT-PUBLISH"


def _steps() -> dict:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return {step["name"]: step for step in workflow["jobs"]["diagnose"]["steps"] if "name" in step}


def _run_diagnostic(
    tmp_path: Path, *, script_exit: int = 0, tee_fails: bool = False, missing: bool = False
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    uv = bin_dir / "uv"
    uv.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$CANARY"\nprintf "%s\\n" "$CANARY" >&2\n'
        'exit "$FAKE_EXIT"\n',
        encoding="utf-8",
    )
    uv.chmod(0o755)
    if tee_fails:
        tee = bin_dir / "tee"
        tee.write_text("#!/bin/sh\ncat >/dev/null\nexit 9\n", encoding="utf-8")
        tee.chmod(0o755)
    if not missing:
        script = tmp_path / "scripts" / "diagnostics" / "phase1.py"
        script.parent.mkdir(parents=True)
        script.write_text("pass\n", encoding="utf-8")
    output = tmp_path / "run-output"
    env = os.environ | {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "DIAGNOSTIC_SCRIPT": "phase1",
        "FAKE_EXIT": str(script_exit),
        "CANARY": CANARY,
        "GITHUB_OUTPUT": str(output),
    }
    shell = _steps()["Run diagnostic script"]["run"].replace(
        "/tmp/diag.log", str(tmp_path / "diag.log")
    )
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", shell],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    lines = output.read_text(encoding="utf-8").splitlines() if output.exists() else []
    return result, dict(line.split("=", 1) for line in lines)


def _build_comment(
    tmp_path: Path, *, status: str, exit_code: str, run_id: str = "123456"
) -> tuple[subprocess.CompletedProcess[str], Path]:
    comment = tmp_path / "comment.md"
    shell = _steps()["Build PR comment"]["run"].replace("/tmp/comment.md", str(comment))
    env = os.environ | {
        "DIAG_STATUS": status,
        "DIAG_EXIT_CODE": exit_code,
        "GITHUB_RUN_ID": run_id,
        "GITHUB_REPOSITORY": "franklinbaldo/ficha",
    }
    result = subprocess.run(
        ["bash", "-e", "-c", shell],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    return result, comment


def test_comment_steps_are_gated_and_never_read_transcript() -> None:
    steps = _steps()
    build = steps["Build PR comment"]
    post = steps["Post comment on PR"]
    assert "steps.cfg.outcome == 'success'" in build["if"]
    assert "steps.run.outcome == 'success'" in build["if"]
    assert "steps.run.outcome == 'failure'" in build["if"]
    assert "steps.cfg.outcome == 'success'" in post["if"]
    assert "steps.build.outcome == 'success'" in post["if"]
    assert build["env"]["DIAG_EXIT_CODE"] == "${{ steps.run.outputs.exit_code }}"
    assert build["env"]["DIAG_STATUS"] == "${{ steps.run.outcome }}"
    assert "pipeline_status=(\"$"+"{PIPESTATUS[@]}\")" in steps["Run diagnostic script"]["run"]
    for step in (build, post):
        run = step["run"]
        for forbidden in ("diag.log", "tail ", "cat /tmp/comment.md", CANARY, "<details>"):
            assert forbidden not in run
    assert "tee /tmp/diag.log" in steps["Run diagnostic script"]["run"]
    assert "continue-on-error" in steps["Run diagnostic script"]


@pytest.mark.skipif(sys.platform == "win32", reason="Uses POSIX Bash and executable fixtures")
@pytest.mark.parametrize(
    ("script_exit", "tee_fails", "expected"),
    [(0, False, 0), (7, False, 7), (0, True, 9)],
)
def test_status_only_comment_never_contains_synthetic_output(
    tmp_path: Path, script_exit: int, tee_fails: bool, expected: int
) -> None:
    result, output = _run_diagnostic(
        tmp_path, script_exit=script_exit, tee_fails=tee_fails
    )
    assert result.returncode == expected, (result.stdout, result.stderr)
    assert output == {"exit_code": str(expected)}
    status = "success" if expected == 0 else "failure"
    built, comment = _build_comment(
        tmp_path, status=status, exit_code=str(expected)
    )
    assert built.returncode == 0, built.stderr
    assert CANARY not in comment.read_text(encoding="utf-8")
    assert comment.read_text(encoding="utf-8") == (
        "## Diagnostic run\n\n"
        f"**Status:** `{status}`\n"
        f"**Exit code:** `{expected}`\n"
        "**Run:** [`123456`](https://github.com/franklinbaldo/ficha/actions/runs/123456)\n"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Uses POSIX Bash and executable fixtures")
def test_missing_script_records_failure_code_without_comment_output(tmp_path: Path) -> None:
    result, output = _run_diagnostic(tmp_path, missing=True)
    assert result.returncode == 2
    assert output == {"exit_code": "2"}
    built, comment = _build_comment(tmp_path, status="failure", exit_code="2")
    assert built.returncode == 0
    assert CANARY not in comment.read_text(encoding="utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="Uses POSIX Bash")
@pytest.mark.parametrize(
    ("status", "exit_code", "run_id"),
    [("skipped", "0", "123"), ("success", "", "123"), ("success", "0", "invalid")],
)
def test_invalid_metadata_fails_closed(
    tmp_path: Path, status: str, exit_code: str, run_id: str
) -> None:
    built, comment = _build_comment(
        tmp_path, status=status, exit_code=exit_code, run_id=run_id
    )
    assert built.returncode == 2
    assert not comment.exists()
