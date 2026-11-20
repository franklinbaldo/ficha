"""Keep Pages publishing credentials out of PR build and HTTP verification jobs."""

from copy import deepcopy
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "deploy.yml"
DEPLOY_PERMISSIONS = {"pages": "write", "id-token": "write"}


def violations(workflow: dict) -> list[str]:
    """Check the least-privilege boundary, not the contents of the build."""
    errors = []
    if workflow.get("permissions") != {"contents": "read"}:
        errors.append("only read-only repository permission at workflow scope")
    jobs = workflow.get("jobs") or {}
    for name in ("build", "verify"):
        if name not in jobs or jobs[name].get("permissions") is not None:
            errors.append(f"{name} must inherit read-only permissions")
    deploy = jobs.get("deploy") or {}
    if deploy.get("permissions") != DEPLOY_PERMISSIONS:
        errors.append("Pages write and OIDC token belong only to deploy")
    if deploy.get("if") != "github.event_name != 'pull_request'" or deploy.get("needs") != "build":
        errors.append("deployment must depend on build and never run in PRs")
    return errors


def test_pages_permissions_are_scoped_to_deploy() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    assert violations(workflow) == []


def test_pages_permissions_guard_rejects_regressions() -> None:
    baseline = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    cases = {}
    w = deepcopy(baseline)
    w["permissions"]["pages"] = "write"
    cases["broad Pages permission"] = w
    w = deepcopy(baseline)
    w["permissions"]["id-token"] = "write"
    cases["broad OIDC permission"] = w
    w = deepcopy(baseline)
    w["jobs"]["build"]["permissions"] = {"pages": "write", "id-token": "write"}
    cases["build can publish"] = w
    w = deepcopy(baseline)
    w["jobs"]["verify"]["permissions"] = {"id-token": "write"}
    cases["verify can mint OIDC token"] = w
    w = deepcopy(baseline)
    del w["jobs"]["deploy"]["permissions"]
    cases["deploy loses required publication permissions"] = w
    w = deepcopy(baseline)
    w["jobs"]["deploy"]["if"] = "always()"
    cases["deployment enabled for PRs"] = w
    for label, variant in cases.items():
        assert violations(variant), label
