"""Guard the no-live-network PR smoke against YAML regressions (#268)."""

from copy import deepcopy
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "etl-smoke.yml"
PR = "github.event_name == 'pull_request'"
OPERATIONAL = "always() && github.event_name != 'pull_request'"
# A weekly/manual smoke must execute the complete, reviewed script. In
# particular a printed command, an rc reset, or a no-op shell must not
# preserve a green policy test while disabling the real probe.
LIVE_RUN = r"""{
  echo "## ETL smoke check"
  echo ""
  echo '```'
} >> "$GITHUB_STEP_SUMMARY"
uv run ficha-etl smoke 2>&1 | tee smoke-output.txt | tee -a "$GITHUB_STEP_SUMMARY"
rc=${PIPESTATUS[0]}
{
  echo '```'
  echo ""
  echo "Exit code: \`$rc\`"
} >> "$GITHUB_STEP_SUMMARY"
{
  echo 'output<<SMOKE_EOF'
  cat smoke-output.txt
  echo 'SMOKE_EOF'
} >> "$GITHUB_OUTPUT"
exit "$rc"
"""



def _load() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _steps(workflow: dict) -> dict:
    result = {}
    for step in workflow["jobs"]["smoke"]["steps"]:
        name = step.get("name")
        if name:
            result[name] = step
    return result


def _pr_is_fixture_only(workflow: dict) -> bool:
    """Allow only the reviewed PR scripts; every other step is operational.

    This intentionally rejects new/duplicated steps and jobs until reviewed.
    Network access for package installation is outside this RFB/IA smoke guard.
    """
    if set(workflow["jobs"]) != {"smoke"}:
        return False
    events = workflow.get("on", workflow.get(True))  # PyYAML YAML 1.1.
    expected_pr = {"paths": ["etl/**", ".github/workflows/etl-smoke.yml"]}
    if (
        not isinstance(events, dict)
        or events.get("pull_request") != expected_pr
        or "pull_request_target" in events
    ):
        return False

    smoke_job = workflow["jobs"]["smoke"]
    if smoke_job.get("if") is not None:  # A job-level guard can skip PR fixtures.
        return False
    if smoke_job.get("continue-on-error") not in (None, False):
        return False
    steps = smoke_job["steps"]
    if len(steps) != 8:
        return False

    expected_setup = [
        {"uses": "actions/checkout@v4"},
        {
            "name": "Install uv",
            "uses": "astral-sh/setup-uv@v3",
            "with": {"enable-cache": True},
        },
        {"run": "uv python install 3.11"},
        {"run": "uv sync"},
    ]
    if steps[:4] != expected_setup:
        return False

    upstream, fixture, live, freshness = steps[4:]
    expected_fixture = {
        "name": "PR smoke with fixtures",
        "if": PR,
        "run": ("uv run --extra dev pytest -q tests/test_smoke.py tests/test_cli.py -k smoke"),
    }
    return (
        upstream.get("name") == "Upstream latest snapshot (list-snapshots)"
        and upstream.get("if") == OPERATIONAL
        and "ficha-etl list-snapshots" in upstream.get("run", "")
        and fixture == expected_fixture
        and live.get("name") == "Run smoke check"
        and live.get("if") == OPERATIONAL
        and live.get("continue-on-error") in (None, False)
        and live.get("run") == LIVE_RUN
        and "|| true" not in live.get("run", "")
        and freshness.get("name") == "Check manifest freshness"
        and freshness.get("if") == OPERATIONAL
        and freshness.get("continue-on-error") in (None, False)
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
    freshness = steps["Check manifest freshness"]["if"]
    assert freshness == OPERATIONAL


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
    # Gavião #280: substrings alone accepted both of these live-PR mutants.
    mutated = deepcopy(original)
    _steps(mutated)["PR smoke with fixtures"]["run"] += "\nuv run ficha-etl smoke"
    assert not _pr_is_fixture_only(mutated)

    mutated = deepcopy(original)
    mutated["jobs"]["smoke"]["steps"].append(
        {"name": "Extra PR live smoke", "if": PR, "run": "uv run ficha-etl smoke"}
    )
    assert not _pr_is_fixture_only(mutated)

    # A duplicate name or a second job must not hide from the dict of steps.
    mutated = deepcopy(original)
    mutated["jobs"]["smoke"]["steps"].append(
        {"name": "PR smoke with fixtures", "if": PR, "run": "uv run ficha-etl smoke"}
    )
    assert not _pr_is_fixture_only(mutated)

    mutated = deepcopy(original)
    mutated["jobs"]["extra"] = {
        "runs-on": "ubuntu-latest",
        "steps": [{"run": "uv run ficha-etl smoke"}],
    }
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_nonblocking_operational_step() -> None:
    mutated = deepcopy(_load())
    _steps(mutated)["Run smoke check"]["continue-on-error"] = True
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_nonblocking_operational_job() -> None:
    mutated = deepcopy(_load())
    mutated["jobs"]["smoke"]["continue-on-error"] = True
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_job_level_pr_skip() -> None:
    for condition in (False, "github.event_name != 'pull_request'"):
        mutated = deepcopy(_load())
        mutated["jobs"]["smoke"]["if"] = condition
        assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_pr_event_scope_changes() -> None:
    original = _load()
    for bad_event in (
        {"paths": ["docs/**"]},
        {
            "paths": ["etl/**", ".github/workflows/etl-smoke.yml"],
            "paths-ignore": ["etl/**"],
        },
    ):
        mutated = deepcopy(original)
        mutated.get("on", mutated.get(True))["pull_request"] = bad_event
        assert not _pr_is_fixture_only(mutated)

    mutated = deepcopy(original)
    mutated.get("on", mutated.get(True))["pull_request_target"] = {}
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_masked_live_smoke_pipeline() -> None:
    mutated = deepcopy(_load())
    live = _steps(mutated)["Run smoke check"]
    command = 'uv run ficha-etl smoke 2>&1 | tee smoke-output.txt | tee -a "$GITHUB_STEP_SUMMARY"'
    assert command in live["run"]
    live["run"] = live["run"].replace(command, command + " || true")
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_nonblocking_freshness_alarm() -> None:
    mutated = deepcopy(_load())
    _steps(mutated)["Check manifest freshness"]["continue-on-error"] = True
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_echo_instead_of_live_probe() -> None:
    mutated = deepcopy(_load())
    live = _steps(mutated)["Run smoke check"]
    live["run"] = live["run"].replace(
        "uv run ficha-etl smoke 2>&1", "echo uv run ficha-etl smoke 2>&1"
    )
    assert not _pr_is_fixture_only(mutated)


def test_negative_mutation_rejects_live_rc_reset() -> None:
    mutated = deepcopy(_load())
    live = _steps(mutated)["Run smoke check"]
    live["run"] = live["run"].replace("rc=${PIPESTATUS[0]}", "rc=${PIPESTATUS[0]}\nrc=0")
    assert not _pr_is_fixture_only(mutated)
