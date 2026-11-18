"""Contrato executável da continuidade dos recibos em reruns de shards.

Não executa ETL, upload, rede ou manifest; usa somente Bash local e um workflow
versionado para reproduzir falhas de checkpoint anteriores a qualquer PUT.
"""

from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path

import pytest
import yaml

_WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml"


def _steps() -> dict[str, dict]:
    config = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    return {step["name"]: step for step in config["jobs"]["shards"]["steps"] if "name" in step}


def test_rerun_receipt_download_is_not_optional() -> None:
    steps = _steps()
    restore = steps["Restore previous receipt on job rerun"]
    gate = steps["Require receipt stability across attempts"]
    persist = steps["Persist shard production receipt before PUT"]
    submit = steps["Submit exactly the receipted bytes"]

    assert restore["if"] == "github.run_attempt > 1"
    assert restore["uses"].startswith("actions/download-artifact@")
    assert not restore.get("continue-on-error", False)
    assert gate["env"]["RUN_ATTEMPT"] == "${{ github.run_attempt }}"
    assert gate["run"]
    assert persist["uses"].startswith("actions/upload-artifact@")

    order = list(steps)
    assert order.index("Restore previous receipt on job rerun") < order.index(
        "Require receipt stability across attempts"
    )
    assert order.index("Require receipt stability across attempts") < order.index(
        "Persist shard production receipt before PUT"
    )
    assert order.index("Persist shard production receipt before PUT") < order.index(
        "Submit exactly the receipted bytes"
    )
    assert submit["run"]


@pytest.mark.skipif(sys.platform == "win32", reason="Bash do GitHub Actions é testado no Linux")
@pytest.mark.parametrize(
    ("attempt", "prior", "allowed"),
    [
        (1, None, True),      # primeira tentativa não precisa de checkpoint prévio
        (2, None, False),     # rerun sem artifact: não permitir novo recibo/PUT
        (2, b"same", True),   # rerun com bytes idênticos
        (2, b"changed", False),  # rerun com recibo divergente
    ],
)
def test_rerun_gate_actually_exits_fail_closed(
    tmp_path: Path, attempt: int, prior: bytes | None, allowed: bool
) -> None:
    gate = _steps()["Require receipt stability across attempts"]
    month_dir = tmp_path / ".publication" / "2026-07"
    month_dir.mkdir(parents=True)
    (month_dir / "receipt-00.json").write_bytes(b"same")
    if prior is not None:
        previous_dir = month_dir / "previous-receipt"
        previous_dir.mkdir()
        (previous_dir / "receipt-00.json").write_bytes(prior)

    env = {**os.environ, "MONTH": "2026-07", "PREFIX": "00", "RUN_ATTEMPT": str(attempt)}
    completed = subprocess.run(
        ["bash", "-e", "-u", "-o", "pipefail", "-c", gate["run"]],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert (completed.returncode == 0) is allowed, (
        f"attempt={attempt}, prior={prior!r}, exit={completed.returncode}, "
        f"stdout={completed.stdout!r}, stderr={completed.stderr!r}"
    )
    if attempt > 1 and not allowed:
        assert "::error::" in completed.stdout
