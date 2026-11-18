"""Regressão da retirada do bootstrap histórico (#268).

O pipeline atual é etl-monthly.yml; o bootstrap one-shot possuía
workflow_dispatch, contents: write e capacidade de upload. Não pode voltar
por engano como atalho de publicação.
"""

from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parents[2] / ".github" / "workflows"


def _legacy_bootstrap_absent(directory: Path) -> bool:
    return not (directory / "etl-bootstrap.yml").exists()


def test_legacy_bootstrap_is_retired() -> None:
    assert _legacy_bootstrap_absent(WORKFLOWS_DIR), (
        "etl-bootstrap.yml foi aposentado; não reintroduzir publicação "
        "com workflow_dispatch/contents: write fora do ETL mensal"
    )


def test_negative_control_detects_reintroduced_bootstrap(tmp_path: Path) -> None:
    (tmp_path / "etl-bootstrap.yml").write_text(
        "name: bootstrap legado\n", encoding="utf-8"
    )
    assert not _legacy_bootstrap_absent(tmp_path)


def test_supported_monthly_workflow_still_exists() -> None:
    assert (WORKFLOWS_DIR / "etl-monthly.yml").is_file()
