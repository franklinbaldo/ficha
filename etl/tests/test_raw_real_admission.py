"""Fail-closed admission at the existing real raw streaming entrypoint."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from ficha_etl import raw_qrf, upload


@pytest.mark.parametrize("month", ["2026-07", "2026-08"])
@pytest.mark.parametrize("spool_exists", [False, True])
@pytest.mark.parametrize("workers", [0, 1, 4])
def test_unproven_real_route_refuses_before_any_effect(
    monkeypatch, tmp_path, month, spool_exists, workers
):
    calls = []
    before = sorted(tmp_path.iterdir())

    def forbid(*args, **kwargs):
        calls.append("effect")
        pytest.fail("effect before custody admission")

    for name in (
        "item_id",
        "canonical_inventory",
        "_existing_raw_files_on_ia",
        "_ia_s3_put",
        "_stream_one_zip_with_retry",
    ):
        monkeypatch.setattr(upload, name, forbid)
    monkeypatch.setattr(upload.upstream, "discover_token", forbid)
    monkeypatch.setattr(upload.raw_preflight, "stage_all", forbid)
    monkeypatch.setattr(upload.tempfile, "TemporaryDirectory", forbid)
    monkeypatch.setattr(upload.httpx, "Client", forbid)
    monkeypatch.setattr(upload.ia, "upload", forbid)
    monkeypatch.setattr(raw_qrf.OfflineReconciler, "reserve", forbid)
    monkeypatch.setattr(raw_qrf.OfflineReconciler, "admit", forbid)

    with pytest.raises(upload.RawCustodyCapabilityUnproven) as caught:
        upload.stream_raw_zips_to_ia(
            month,
            access_key="offline-access",
            secret_key="offline-secret",
            spool_dir=tmp_path if spool_exists else None,
            workers=workers,
        )

    error = caught.value
    if (
        error.code != "CAPABILITY_UNPROVEN"
        or error.item != f"ficha-{month}"
        or error.key_scope != "raw/*"
        or error.missing != ("S", "U", "ATOMIC_CREATE", "Q", "F")
        or "offline-secret" in str(error)
        or "offline-access" in str(error)
        or calls
        or sorted(tmp_path.iterdir()) != before
    ):
        pytest.fail("wrong admission refusal or an effect escaped")


def test_invalid_month_keeps_value_error_precedence(monkeypatch):
    def forbid(*args, **kwargs):
        pytest.fail("invalid month reached admission")

    monkeypatch.setattr(upload, "_require_real_raw_admission", forbid)
    with pytest.raises(ValueError, match="month must be YYYY-MM"):
        upload.stream_raw_zips_to_ia("invalid", access_key="A", secret_key="S")

def test_admission_oracles_under_optimized_python():
    """Check each expected nodeid, not merely a successful subprocess exit."""
    here = Path(__file__).resolve()
    cases = [
        f"test_unproven_real_route_refuses_before_any_effect[{worker}-{spool}-{month}]"
        for worker in (0, 1, 4)
        for spool in (False, True)
        for month in ("2026-07", "2026-08")
    ]
    cases.append("test_invalid_month_keeps_value_error_precedence")
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-vv",
            *(f"{here}::{case}" for case in cases),
        ],
        env={k: v for k, v in os.environ.items() if k not in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS")},
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if result.returncode:
        pytest.fail(result.stdout + "\n" + result.stderr)
    for case in cases:
        matches = [line for line in result.stdout.splitlines() if f"::{case} PASSED" in line]
        if len(matches) != 1:
            pytest.fail(f"missing unique optimized PASS for {case}: {result.stdout}")
