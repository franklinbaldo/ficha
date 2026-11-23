"""Fail-closed contract for the dorny outputs that gate CI Test steps (#268).

A green job whose path-filter step had no ``id: changes`` is not evidence
that its Test step ran: ``steps.changes.outputs.*`` may resolve to empty.
This only checks workflow wiring; Actions step execution is separate evidence.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
JOBS = {"web": "web", "etl": "etl", "etl-windows": "etl", "ficha-py": "ficha-py"}
TEST_RUNS = {
    "web": "bun run test",
    "etl": "uv run pytest tests",
    "etl-windows": "uv run pytest tests -q",
    "ficha-py": "uv run pytest tests",
}


def _require(predicate: bool, message: str) -> None:
    if not predicate:
        raise AssertionError(message)


def assert_filter_output_binding(workflow: dict) -> None:
    """Check that every Test uses a live, fail-closed path-filter output."""
    for job, key in JOBS.items():
        job_config = workflow["jobs"][job]
        _require("if" not in job_config, f"{job}: CI job must not be conditional")
        _require(
            "needs" not in job_config,
            f"{job}: independent test gate may not depend on a skipped job",
        )
        _require(
            job_config.get("continue-on-error") in (None, False),
            f"{job}: CI job failure must not be masked",
        )
        steps = job_config["steps"]
        filters = [step for step in steps if step.get("uses") == "dorny/paths-filter@v3"]
        _require(len(filters) == 1, f"{job}: exactly one path-filter step required")
        filter_step = filters[0]
        _require(filter_step.get("id") == "changes", f"{job}: missing outputs producer id")
        _require("if" not in filter_step, f"{job}: filter producer must always execute")
        _require(
            filter_step.get("continue-on-error") in (None, False),
            f"{job}: filter failure may not be masked",
        )
        output_names = yaml.safe_load(filter_step["with"]["filters"])
        _require(
            isinstance(output_names, dict) and set(output_names) == {key},
            f"{job}: filter output key disagrees with Test consumer",
        )
        tests = [step for step in steps if step.get("name") == "Test"]
        _require(len(tests) == 1, f"{job}: exactly one Test step required")
        _require(
            steps.index(filter_step) < steps.index(tests[0]),
            f"{job}: Test must follow its path-filter producer",
        )
        _require(
            tests[0].get("if") == f"steps.changes.outputs.{key} == 'true'",
            f"{job}: Test is not conditional on this producer",
        )
        _require(
            tests[0].get("continue-on-error") in (None, False),
            f"{job}: Test failure may not be masked",
        )
        _require(
            tests[0].get("run") == TEST_RUNS[job],
            f"{job}: Test step must execute the expected test suite",
        )


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_real_workflow_binds_test_to_unconditional_filter_output() -> None:
    assert_filter_output_binding(_workflow())


@pytest.mark.parametrize("job", JOBS)
@pytest.mark.parametrize(
    "mutation",
    (
        "drop-id",
        "rename-id",
        "skip-producer",
        "mask-producer",
        "wrong-output",
        "mask-test",
        "test-before-producer",
        "skip-job",
        "mask-job",
        "skipped-prerequisite",
        "noop-test",
        "missing-test-command",
    ),
)
def test_mismatched_or_skipped_filter_cannot_appear_green(job: str, mutation: str) -> None:
    workflow = deepcopy(_workflow())
    steps = workflow["jobs"][job]["steps"]
    producer = next(step for step in steps if step.get("uses") == "dorny/paths-filter@v3")
    consumer = next(step for step in steps if step.get("name") == "Test")
    if mutation == "skip-job":
        workflow["jobs"][job]["if"] = "${{ false }}"
    elif mutation == "mask-job":
        workflow["jobs"][job]["continue-on-error"] = True
    elif mutation == "skipped-prerequisite":
        workflow["jobs"]["dormant"] = {
            "runs-on": "ubuntu-latest",
            "if": "${{ false }}",
            "steps": [{"run": "true"}],
        }
        workflow["jobs"][job]["needs"] = ["dormant"]
    elif mutation == "drop-id":
        producer.pop("id")
    elif mutation == "rename-id":
        producer["id"] = "changed"
    elif mutation == "skip-producer":
        producer["if"] = "${{ false }}"
    elif mutation == "mask-producer":
        producer["continue-on-error"] = True
    elif mutation == "wrong-output":
        producer["with"]["filters"] = "other:\n  - 'etl/**'\n"
    elif mutation == "mask-test":
        consumer["continue-on-error"] = True
    elif mutation == "noop-test":
        consumer["run"] = "true"
    elif mutation == "missing-test-command":
        consumer.pop("run")
    elif mutation == "test-before-producer":
        steps.remove(consumer)
        steps.insert(steps.index(producer), consumer)
    with pytest.raises((AssertionError, KeyError)):
        assert_filter_output_binding(workflow)
