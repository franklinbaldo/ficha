"""W3.1 PR smoke must execute against the literal branch commit, not merge refs.

This is a local structural contract. It neither runs the streaming benchmark
nor invokes any ETL, download, release, or external service.
"""

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/etl-streaming-benchmark.yml"
HEAD = "${{ github.event.pull_request.head.sha || github.sha }}"


def require_exact_streaming_checkout(config: dict) -> None:
    # Shell overrides may ignore the generated script file and silently pass.
    assert "shell" not in config.get("defaults", {}).get("run", {})
    job = config["jobs"]["streaming-benchmark"]
    # Bash sources BASH_ENV for noninteractive scripts before running the guard.
    # An inherited variable could shadow `git` and spoof the SHA comparison.
    for scope in (config, job):
        inherited_env = scope.get("env") or {}
        assert isinstance(inherited_env, dict)
        assert "BASH_ENV" not in inherited_env
    assert "shell" not in job.get("defaults", {}).get("run", {})
    # A skipped or allowed-to-fail job can be green without proving its checkout.
    assert "if" not in job
    assert job.get("continue-on-error") in (None, False)
    # PyYAML 1.1 interprets the unquoted YAML key "on" as boolean True.
    events = config.get("on", config.get(True))
    assert isinstance(events, dict)
    assert "workflow_dispatch" in events
    assert "pull_request_target" not in events
    pull_request = events["pull_request"]
    assert isinstance(pull_request, dict)
    expected_paths = ["etl/**", ".github/workflows/etl-streaming-benchmark.yml"]
    assert pull_request.get("paths") == expected_paths
    steps = job["steps"]
    assert steps[0]["uses"] == "actions/checkout@v4"
    assert steps[0]["with"] == {"persist-credentials": False, "ref": HEAD}
    guard = steps[1]
    assert guard["name"] == "Verify exact streaming checkout SHA"
    assert guard["env"] == {"EXPECTED_SHA": HEAD}
    assert guard["run"] == 'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"'
    assert "if" not in guard
    assert "shell" not in guard
    assert guard.get("continue-on-error") in (None, False)


def test_streaming_workflow_checkout_is_sha_exact_and_fail_closed() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    require_exact_streaming_checkout(workflow)


@pytest.mark.parametrize(
    "mutation",
    (
        "missing-ref",
        "merge-ref",
        "persist-credentials",
        "skip-guard",
        "skip-job",
        "ignore-job-failure",
        "ignore-failure",
        "no-assert",
        "echo-instead-of-test",
        "guard-shell-noop",
        "job-default-shell-noop",
        "workflow-default-shell-noop",
        "missing-pr-event",
        "missing-dispatch-event",
        "missing-etl-path",
        "missing-workflow-path",
        "dual-target-event",
        "workflow-bash-env",
        "job-bash-env",
    ),
)
def test_checkout_policy_rejects_regressions(mutation: str) -> None:
    workflow = deepcopy(yaml.safe_load(WORKFLOW.read_text(encoding="utf-8")))
    checkout, guard = workflow["jobs"]["streaming-benchmark"]["steps"][:2]
    if mutation == "missing-ref":
        del checkout["with"]["ref"]
    elif mutation == "merge-ref":
        checkout["with"]["ref"] = "${{ github.sha }}"
    elif mutation == "persist-credentials":
        checkout["with"]["persist-credentials"] = True
    elif mutation == "skip-guard":
        guard["if"] = "github.event_name == 'push'"
    elif mutation == "skip-job":
        workflow["jobs"]["streaming-benchmark"]["if"] = "false"
    elif mutation == "ignore-job-failure":
        workflow["jobs"]["streaming-benchmark"]["continue-on-error"] = True
    elif mutation == "ignore-failure":
        guard["continue-on-error"] = True
    elif mutation == "no-assert":
        guard["run"] += " || true"
    elif mutation == "guard-shell-noop":
        guard["shell"] = "bash -c 'true' {0}"
    elif mutation == "job-default-shell-noop":
        job_run_defaults = workflow["jobs"]["streaming-benchmark"]["defaults"]["run"]
        job_run_defaults["shell"] = "bash -c 'true' {0}"
    elif mutation == "workflow-default-shell-noop":
        workflow["defaults"] = {"run": {"shell": "bash -c 'true' {0}"}}
    elif mutation == "missing-pr-event":
        del workflow.get("on", workflow.get(True))["pull_request"]
    elif mutation == "missing-dispatch-event":
        del workflow.get("on", workflow.get(True))["workflow_dispatch"]
    elif mutation == "missing-etl-path":
        events = workflow.get("on", workflow.get(True))
        events["pull_request"]["paths"].remove("etl/**")
    elif mutation == "missing-workflow-path":
        events = workflow.get("on", workflow.get(True))
        paths = events["pull_request"]["paths"]
        paths.remove(".github/workflows/etl-streaming-benchmark.yml")
    elif mutation == "dual-target-event":
        workflow.get("on", workflow.get(True))["pull_request_target"] = {}
    elif mutation == "workflow-bash-env":
        workflow["env"] = {"BASH_ENV": "ci/shadow-git.sh"}
    elif mutation == "job-bash-env":
        workflow["jobs"]["streaming-benchmark"]["env"] = {"BASH_ENV": "ci/shadow-git.sh"}
    else:
        guard["run"] = 'echo "$(git rev-parse HEAD)" = "$EXPECTED_SHA"'
    with pytest.raises((AssertionError, KeyError)):
        require_exact_streaming_checkout(workflow)
