"""Do not silently resurrect retired one-shot publishing/diagnostic workflows.

Retirement is a CI-scope choice, not authorization for data publication.
This check reads repository files only and never calls Internet Archive or RFB.
"""

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RETIRED_WORKFLOWS = (
    ".github/workflows/etl-bootstrap.yml",
    ".github/workflows/_tmp-ovigia-first-story-probe.yml",
)


def test_retired_one_shot_workflows_stay_absent() -> None:
    for path in RETIRED_WORKFLOWS:
        assert not (REPO / path).exists(), f"retired workflow reintroduced: {path}"


def test_monthly_release_and_site_gates_remain_present() -> None:
    for path in (
        ".github/workflows/etl-monthly.yml",
        ".github/workflows/release-monthly-data.yml",
        ".github/workflows/deploy.yml",
        ".github/workflows/visual-capture.yml",
        ".github/workflows/okf-contract.yml",
    ):
        assert (REPO / path).is_file(), f"protected workflow missing: {path}"
