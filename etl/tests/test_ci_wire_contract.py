"""Cross-language CI contract: fixture edits must execute Python and TypeScript tests.

This tests path selection and checkout identity, not an ETL, IA upload or a
browser/Internet Archive end-to-end run. All inputs are local.
"""

from __future__ import annotations

from copy import deepcopy
from fnmatch import fnmatchcase
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml"
EXACT_SHA = "${{ github.event.pull_request.head.sha || github.sha }}"

# The existing Vitest case manifest-reader-compat.lagarto.test.ts reads the
# *same* company-v11-ca000001.hex fixture as the Castor Python tests.
PYTHON_TO_WEB = (
    "etl/tests/fixtures/company-v11-ca000001.hex",
    "etl/tests/fixtures/company_pb2_v1_at_021ec8b5.txt",
    "etl/tests/fixtures/company_pb2_v11_at_ef2a7a46.txt",
    "etl/tests/test_lontra_historical_v1_decoder.py",
    "etl/tests/test_castor_shared_wire_boundary.py",
    "etl/tests/test_castor_v11_real_zip.py",
    "etl/src/ficha_etl/proto/ficha/v1/company_pb2.py",
    "etl/src/ficha_etl/pack.py",
    "etl/src/ficha_etl/sharded_pack.py",
)
WEB_TO_PYTHON = (
    "web/src/generated/company.pb.js",
    "web/src/generated/company.pb.d.ts",
    "web/src/lib/companies.ts",
    "web/src/lib/companies-atomic-v11.ts",
    "web/src/schemas/v1/manifest.ts",
    "web/src/schemas/v1/manifest-reader-compat.lagarto.test.ts",
)


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _filters(workflow: dict, job: str) -> list[str]:
    steps = workflow["jobs"][job]["steps"]
    matches = [step for step in steps if step.get("uses") == "dorny/paths-filter@v3"]
    assert len(matches) == 1, f"{job}: missing or duplicated paths filter"
    name = "etl" if job in ("etl", "etl-windows") else "web"
    return yaml.safe_load(matches[0]["with"]["filters"])[name]


def _covers(patterns: list[str], path: str) -> bool:
    return any(fnmatchcase(path, pattern) for pattern in patterns)


def _assert_contract(workflow: dict) -> None:
    web = _filters(workflow, "web")
    for path in PYTHON_TO_WEB:
        assert _covers(web, path), f"web Test skipped for shared wire input: {path}"
    assert not _covers(web, "etl/tests/test_download.py")
    assert not _covers(web, "etl/README.md")

    linux = _filters(workflow, "etl")
    windows = _filters(workflow, "etl-windows")
    assert linux == windows, "Linux/Windows cross-wire selectors differ"
    for path in WEB_TO_PYTHON:
        assert _covers(linux, path), f"Python Test skipped for TS wire input: {path}"
    assert not _covers(linux, "web/src/pages/index.astro")
    assert not _covers(linux, "web/README.md")


def test_wire_paths_run_both_existing_jobs_without_broad_cross_triggers() -> None:
    _assert_contract(_workflow())


def test_tests_are_executed_when_cross_filter_matches() -> None:
    workflow = _workflow()
    for job, name, command in (
        ("web", "web", "bun run test"),
        ("etl", "etl", "uv run pytest tests"),
        ("etl-windows", "etl", "uv run pytest tests -q"),
    ):
        steps = workflow["jobs"][job]["steps"]
        selected = [step for step in steps if step.get("name") == "Test"]
        assert len(selected) == 1
        assert selected[0]["if"] == f"steps.changes.outputs.{name} == 'true'"
        assert selected[0]["run"] == command
        assert selected[0].get("continue-on-error") in (None, False)


def test_jobs_checkout_and_assert_same_literal_source_sha() -> None:
    workflow = _workflow()
    for job in ("web", "etl", "etl-windows", "ficha-py"):
        steps = workflow["jobs"][job]["steps"]
        assert steps[0]["uses"] == "actions/checkout@v4"
        assert steps[0]["with"] == {"ref": EXACT_SHA, "persist-credentials": False}
        assert steps[1]["name"] == "Verify exact CI checkout SHA"
        assert steps[1]["run"] == f'test "$(git rev-parse HEAD)" = "{EXACT_SHA}"'
        assert "if" not in steps[1]
        assert steps[1].get("continue-on-error") in (None, False)


@pytest.mark.parametrize(
    ("job", "pattern"),
    [
        ("web", "etl/tests/fixtures/company-v11-ca000001.hex"),
        ("web", "etl/tests/fixtures/company_pb2_*.txt"),
        ("web", "etl/tests/test_lontra_historical_v1_decoder.py"),
        ("web", "etl/src/ficha_etl/proto/**"),
        ("etl", "web/src/generated/company.pb.*"),
        ("etl", "web/src/lib/companies*.ts"),
        ("etl", "web/src/schemas/v1/manifest*.ts"),
    ],
)
def test_removing_cross_contract_selector_is_caught(job: str, pattern: str) -> None:
    changed = deepcopy(_workflow())
    steps = changed["jobs"][job]["steps"]
    filter_step = next(step for step in steps if step.get("uses") == "dorny/paths-filter@v3")
    filters = yaml.safe_load(filter_step["with"]["filters"])
    name = "web" if job == "web" else "etl"
    assert pattern in filters[name]
    filters[name].remove(pattern)
    filter_step["with"]["filters"] = yaml.safe_dump(filters, sort_keys=False)
    with pytest.raises(AssertionError):
        _assert_contract(changed)
