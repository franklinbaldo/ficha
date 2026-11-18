"""#271: prevent an unapproved data release for *any* Actions event.

This checks the source YAML and evaluates its narrowly supported job condition
against synthetic event payloads. It never runs gh or a publishing workflow.
"""

import ast
import re
from pathlib import Path

import pytest
import yaml

WORKFLOW_PATH = (
    Path(__file__).parents[2] / ".github" / "workflows" / "release-monthly-data.yml"
)


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _allows_job(condition: str, event: str, conclusion: str | None) -> bool:
    """Evaluate only the literal/and/equality subset used by this job gate."""
    assert condition.startswith("${{") and condition.endswith("}}")
    expression = condition[3:-2].strip()
    expression = re.sub(r"\bfalse\b", "False", expression)
    expression = expression.replace("github.event.workflow_run.conclusion", repr(conclusion))
    expression = expression.replace("github.event_name", repr(event))
    expression = expression.replace("&&", " and ")
    expression = " ".join(expression.split())
    node = ast.parse(expression, mode="eval").body

    def evaluate(part: ast.AST) -> object:
        if isinstance(part, ast.Constant):
            return part.value
        if isinstance(part, ast.BoolOp) and isinstance(part.op, ast.And):
            return all(evaluate(value) for value in part.values)
        if (
            isinstance(part, ast.Compare)
            and len(part.ops) == 1
            and isinstance(part.ops[0], ast.Eq)
            and len(part.comparators) == 1
        ):
            return evaluate(part.left) == evaluate(part.comparators[0])
        raise AssertionError(f"Unsupported or unsafe Actions guard expression: {ast.dump(part)}")

    return evaluate(node) is True


def _guard() -> str:
    workflow = _workflow()
    assert list(workflow["jobs"]) == ["release"]
    job = workflow["jobs"]["release"]
    assert "gh release create" in "\n".join(step.get("run", "") for step in job["steps"])
    return job["if"]


def test_guard_has_required_constraints_and_explicit_approval_hold() -> None:
    guard = " ".join(_guard().split())
    assert re.fullmatch(
        r"\$\{\{ false && github\.event_name == 'workflow_run' "
        r"&& github\.event\.workflow_run\.conclusion == 'success' \}\}",
        guard,
    ), guard
    # A success event alone does not prove the same manifest was promoted.
    assert not _allows_job(_guard(), "workflow_run", "success")


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "skipped", "success", None])
@pytest.mark.parametrize("tag_exists", [False, True])
def test_workflow_run_never_creates_release_without_promotion_proof(
    conclusion: str | None, tag_exists: bool
) -> None:
    # If the job doesn't run, existence of a tag is immaterial.
    would_create_release = _allows_job(_guard(), "workflow_run", conclusion) and not tag_exists
    assert not would_create_release


@pytest.mark.parametrize("event", ["push", "workflow_dispatch", "pull_request"])
@pytest.mark.parametrize("tag_exists", [False, True])
def test_no_other_event_can_create_release(event: str, tag_exists: bool) -> None:
    assert not (_allows_job(_guard(), event, None) and not tag_exists)


def test_negative_mutations_prove_the_guard_is_not_vacuous() -> None:
    guard = _guard()
    without_hold = guard.replace("false &&", "", 1)
    assert _allows_job(without_hold, "workflow_run", "success")
    assert not _allows_job(without_hold, "workflow_run", "failure")
    assert not _allows_job(without_hold, "workflow_run", "cancelled")
    assert not _allows_job(without_hold, "workflow_run", "skipped")
    assert not _allows_job(without_hold, "push", None)
    # Without the approval hold, a successful ETL event with an absent tag
    # would reach release creation despite missing promotion identity.
    assert _allows_job(without_hold, "workflow_run", "success")
