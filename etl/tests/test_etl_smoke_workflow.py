"""Guard the no-live-network PR smoke against YAML regressions (#268)."""

from copy import deepcopy
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "etl-smoke.yml"
PR = "github.event_name == 'pull_request'"
OPERATIONAL = "always() && github.event_name != 'pull_request'"


def _load() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _steps(workflow: dict) -> dict:
    return {step.get("name"): step for step in workflow["jobs"]["smoke"]["steps"] if step.get("name")}


def _pr_is_fixture_only(workflow: dict) -> bool:
    steps = _steps(workflow)
    fixture = steps.get("PR smoke with fixtures", {})
    upstream = steps.get("Upstream latest snapshot (list-snapshots)", {})
    live = steps.get("Run smoke check", {})
    return (
        fixture.get("if") == PR
        and "pytest" in fixture.get("run", "")
        and "tests/test_smoke.py" in fixture.get("run", "")
        and "tests/test_cli.py" in fixture.get("run", "")
        and "-k smoke" in fixture.get("run", "")
        and upstream.get("if") == OPERATIONAL
        and "ficha-etl list-snapshots" in upstream.get("run", "")
        and live.get("if") == OPERATIONAL
        and "ficha-etl smoke" in live.get("run", "")
    )


def test_pr_runs_http_fixtures_without_live_upstream_or_ia() -> None:
    assert _pr_is_fixture_only(_load())


def test_weekly_and_manual_smoke_remain_live_and_block_on_failure() -> None:
    workflow = _load()
    events = workflow.get("on", workflow.get(True))  # PyYAML YAML 1.1 treats 'on' as bool.
    assert "pull_request" in events
    assert "workflow_dispatch" in events
    assert events["schedule"] == [{"cron": "0 6 * * 1"}]
    steps = _steps(workflow)
    assert steps["Run smoke check"]["run"].rstrip().endswith('exit "$rc"')
    assert steps["Check manifest freshness"]["if"] == (
        "always() && github.event_name != 'pull_request'"
    )


def test_pr_job_has_no_write_permission_or_silent_success() -> None:
    workflow = _load()
    assert workflow["permissions"] == {"contents": "read"}
    assert "exit 0" not in _steps(workflow)["Run smoke check"]["run"]


def test_negative_mutations_detect_real_network_reintroduction() -> None:
    original = _load()
    assert _pr_is_fixture_only(original)
    for step_name in ("Upstream latest snapshot (list-snapshots)", "Run smoke check"):
        mutated = deepcopy(original)
        step = _steps(mutated)[step_name]
        step["if"] = "always()"
        assert not _pr_is_fixture_only(mutated), step_name
    mutated = deepcopy(original)
    _steps(mutated)["PR smoke with fixtures"]["if"] = "github.event_name != 'pull_request'"
    assert not _pr_is_fixture_only(mutated)
    mutated = deepcopy(original)
    _steps(mutated)["PR smoke with fixtures"]["run"] = "echo green"
    assert not _pr_is_fixture_only(mutated)
