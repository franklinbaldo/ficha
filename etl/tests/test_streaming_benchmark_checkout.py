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
    assert "shell" not in job.get("defaults", {}).get("run", {})
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
        "ignore-failure",
        "no-assert",
        "echo-instead-of-test",
        "guard-shell-noop",
        "job-default-shell-noop",
        "workflow-default-shell-noop",
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
    elif mutation == "ignore-failure":
        guard["continue-on-error"] = True
    elif mutation == "no-assert":
        guard["run"] += " || true"
    elif mutation == "guard-shell-noop":
        guard["shell"] = "bash -c 'true' {0}"
    elif mutation == "job-default-shell-noop":
        workflow["jobs"]["streaming-benchmark"]["defaults"]["run"]["shell"] = (
            "bash -c 'true' {0}"
        )
    elif mutation == "workflow-default-shell-noop":
        workflow["defaults"] = {"run": {"shell": "bash -c 'true' {0}"}}
    else:
        guard["run"] = 'echo "$(git rev-parse HEAD)" = "$EXPECTED_SHA"'
    with pytest.raises((AssertionError, KeyError)):
        require_exact_streaming_checkout(workflow)
