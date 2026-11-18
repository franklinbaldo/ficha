"""Regressão: não reintroduzir bootstrap de publicação (#268)."""

from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parents[2] / ".github" / "workflows"


def legacy_bootstrap_absent(directory: Path) -> bool:
    return not (directory / "etl-bootstrap.yml").is_file()


def test_legacy_bootstrap_is_retired() -> None:
    assert legacy_bootstrap_absent(WORKFLOWS_DIR)


def test_negative_control_rejects_reintroduction(tmp_path: Path) -> None:
    path = tmp_path / "etl-bootstrap.yml"
    path.write_text("name: obsolete bootstrap\n", encoding="utf-8")
    assert not legacy_bootstrap_absent(tmp_path)


def test_monthly_workflow_is_still_present() -> None:
    assert (WORKFLOWS_DIR / "etl-monthly.yml").is_file()
