"""Offline contract for GitHub artifact attempt identity.

No monthly workflow, Receita GET or Internet Archive PUT is executed by these tests.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml"
JOBS = ("resolve", "produce", "inputs-visible", "shards", "finalize")
SUFFIX = "-attempt-${{ github.run_attempt }}"
GUARD = """      # Until immutable producer/receipt recovery exists, fail before any RFB/IA access.
      # Re-running failed jobs is NOT equivalent to a fresh, attested publication.
      - name: Refuse unproven monthly workflow attempt
        shell: bash
        env:
          RUN_ATTEMPT: ${{ github.run_attempt }}
        run: |
          set -euo pipefail
          if [ "$RUN_ATTEMPT" != "1" ]; then
            echo "::error::Monthly attempt $RUN_ATTEMPT refused; recovery needs independent producer attestation"
            exit 1
          fi
"""


def _job_blocks(source: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?m)^  ([a-z][a-z0-9-]*):\n", source))
    return {
        match.group(1): source[match.end() : matches[index + 1].start()]
        if index + 1 < len(matches)
        else source[match.end() :]
        for index, match in enumerate(matches)
    }


def _artifact_steps(source: str) -> list[tuple[str, str]]:
    steps = list(re.finditer(r"(?m)^      - (?:uses|name|run):", source))
    result: list[tuple[str, str]] = []
    for index, match in enumerate(steps):
        block = (
            source[match.start() : steps[index + 1].start()]
            if index + 1 < len(steps)
            else source[match.start() :]
        )
        action = re.search(
            r"(?m)^      - uses: actions/(upload|download)-artifact@v4$"
            r"|^        uses: actions/(upload|download)-artifact@v4$",
            block,
        )
        if action:
            result.append((action.group(1) or action.group(2), block))
    return result


def _check_contract(source: str) -> None:
    jobs = _job_blocks(source)
    assert set(JOBS).issubset(jobs)
    for name in JOBS:
        job = jobs[name]
        assert job.split("    steps:\n", 1)[1].startswith(
            GUARD + "      - uses: actions/checkout@v4"
        )
        # The workflow guard is deliberately before setup and all RFB/IA I/O.
        assert job.index(GUARD) < job.index("actions/checkout@v4")

    steps = _artifact_steps(source)
    assert len([kind for kind, _ in steps if kind == "upload"]) == 7
    assert len([kind for kind, _ in steps if kind == "download"]) == 5
    patterns = 0
    for _, block in steps:
        target = re.search(r"(?m)^          (name|pattern): (.+)$", block)
        assert target, "artifact without an explicit run-scoped selector"
        assert target.group(2).endswith(SUFFIX), target.group(2)
        assert "overwrite: true" not in block
        assert not re.search(r"(?m)^          (run-id|github-token|repository):", block)
        patterns += target.group(1) == "pattern"
    assert patterns == 1, "only finalizer may expand current-attempt shard receipts"


def test_artifacts_isolate_attempts_and_guards_precede_execution() -> None:
    _check_contract(WORKFLOW.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "mutant", ["missing-guard", "inverted-guard", "old-name", "old-pattern", "overwrite"]
)
def test_structural_mutations_are_rejected(mutant: str) -> None:
    source = WORKFLOW.read_text(encoding="utf-8")
    if mutant == "missing-guard":
        changed = source.replace(GUARD, "", 1)
    elif mutant == "inverted-guard":
        changed = source.replace('"$RUN_ATTEMPT" != "1"', '"$RUN_ATTEMPT" = "1"', 1)
    elif mutant == "old-name":
        changed = source.replace(SUFFIX, "", 1)
    elif mutant == "old-pattern":
        changed = source.replace(
            "pattern: shard-receipt-*-${{ needs.resolve.outputs.month }}" + SUFFIX,
            "pattern: shard-receipt-*-${{ needs.resolve.outputs.month }}",
            1,
        )
    else:
        changed = source.replace(
            "          retention-days: 90\n",
            "          retention-days: 90\n          overwrite: true\n",
            1,
        )
    assert changed != source
    with pytest.raises(AssertionError):
        _check_contract(changed)


@pytest.mark.skipif(shutil.which("bash") is None, reason="Bash contract exercised on Linux CI")
@pytest.mark.parametrize(
    ("attempt", "accepted"), [("1", True), ("2", False), ("11", False), ("", False)]
)
def test_actual_guard_bash_rejects_unattested_reruns(attempt: str, accepted: bool) -> None:
    source = WORKFLOW.read_text(encoding="utf-8")
    actual = _job_blocks(source)["produce"].split("    steps:\n", 1)[1]
    assert actual.startswith(GUARD)
    bash_source = textwrap.dedent(
        actual.split("        run: |\n", 1)[1].split("\n      - uses:", 1)[0]
    )
    process = subprocess.run(
        ["bash", "-c", bash_source],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "RUN_ATTEMPT": attempt},
    )
    assert (process.returncode == 0) is accepted
    if not accepted:
        assert "refused" in process.stdout
