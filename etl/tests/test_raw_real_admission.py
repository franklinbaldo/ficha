"""Fail-closed admission at the existing real raw streaming entrypoint."""

from __future__ import annotations

import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree

import pytest

from ficha_etl import raw_qrf, upload


def require_replay(report: Path, cases: list[tuple[str, str, str]]) -> None:
    expected = Counter((Path(file).stem, node, status) for file, node, status in cases)
    if not report.is_file():
        pytest.fail("optimized replay did not produce JUnit XML")
    observed = Counter()
    for case in ElementTree.parse(report).iter("testcase"):
        module = case.attrib.get("classname", "").rsplit(".", 1)[-1]
        name = case.attrib.get("name", "")
        skipped = case.find("skipped")
        status = "PASSED"
        if case.find("failure") is not None or case.find("error") is not None:
            status = "FAILED"
        elif skipped is not None:
            status = "XFAIL" if skipped.get("type") == "pytest.xfail" else "SKIPPED"
        observed[module, name, status] += 1
    if observed != expected:
        pytest.fail(f"optimized replay expected {expected!r}, got {observed!r}")


@pytest.mark.parametrize("month", ["2026-07", "2026-08"])
@pytest.mark.parametrize("spool_exists", [False, True])
@pytest.mark.parametrize("workers", [0, 1, 4])
def test_unproven_real_route_refuses_before_any_effect(
    monkeypatch, tmp_path, month, spool_exists, workers
):
    calls = []
    before = sorted(tmp_path.iterdir())
    original_is_dir = type(tmp_path).is_dir

    def forbid(*args, **kwargs):
        calls.append("effect")
        pytest.fail("effect before custody admission")

    def check_spool(candidate):
        if candidate == tmp_path:
            forbid()
        return original_is_dir(candidate)

    monkeypatch.setattr(type(tmp_path), "is_dir", check_spool)

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


def _replay_exact(cases, tmp_path):
    report = tmp_path / "optimized.xml"
    here = Path(__file__).resolve().parent
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "pytest",
            "-vv",
            f"--junitxml={report}",
            "-o",
            "xfail_strict=true",
            *(f"{here / file}::{node}" for file, node in cases),
        ],
        env={k: v for k, v in os.environ.items() if k not in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS")},
        capture_output=True,
        text=True,
        check=False,
        timeout=240,
    )
    if result.returncode:
        pytest.fail(result.stdout + "\n" + result.stderr)
    require_replay(report, [(file, node, "PASSED") for file, node in cases])


def test_admission_oracles_under_optimized_python(tmp_path):
    """Check each expected nodeid, not merely a successful subprocess exit."""
    cases = [
        (
            "test_raw_real_admission.py",
            f"test_unproven_real_route_refuses_before_any_effect[{worker}-{spool}-{month}]",
        )
        for worker in (0, 1, 4)
        for spool in (False, True)
        for month in ("2026-07", "2026-08")
    ]
    cases.append(("test_raw_real_admission.py", "test_invalid_month_keeps_value_error_precedence"))
    _replay_exact(cases, tmp_path)


def test_offline_effect_witnesses_under_optimized_python(tmp_path):
    """Distinct QRF witnesses, not a reenactment of remote process death or HTTP 503."""
    qrf = [
        "test_qrf02_one_shot_two_threads",
        "test_qrf02_ambiguous_attempt_not_rearmed",
        "test_qrf05_wrong_bytes_keep_unknown",
        "test_qrf06_same_domain_two_handles_not_new_sink",
    ]
    offline = [
        "test_exact_aggregate_cap_and_preexisting_match",
        "test_last_redirect_before_reserve",
        "test_ack_lost_after_apply_and_return_lost_after_commit",
        "test_error_before_second_apply_preserves_prefix",
        "test_immutable_parser_and_cumulative_budget[False]",
        "test_immutable_parser_and_cumulative_budget[True]",
        "test_stale_generation_refused_without_overwrite[one-stale]",
        "test_stale_generation_refused_without_overwrite[eighteen-stale]",
    ]
    _replay_exact(
        [("test_raw_qrf.py", node) for node in qrf]
        + [("test_raw_offline_upload.py", node) for node in offline],
        tmp_path,
    )
