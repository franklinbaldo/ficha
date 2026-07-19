"""Testes estáticos para .github/workflows/etl-monthly.yml.

Cobre a relação entre `inputs.skip_upload` e o step que comita/publica o
manifest em `main`: skip_upload evita o upload pro IA, mas sozinho não
impede que o manifest resultante (apontando pra arquivos nunca enviados)
seja comitado — ver discussão nos PRs #53/#54.
"""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml"


def _load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _step(name: str) -> dict:
    for step in _load_workflow()["jobs"]["run"]["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"step {name!r} não encontrado em {WORKFLOW_PATH.name}")


def test_workflow_is_valid_yaml() -> None:
    workflow = _load_workflow()
    assert workflow["jobs"]["run"]["steps"], "job 'run' sem steps"


def test_commit_step_requires_should_run_and_not_skip_upload() -> None:
    step = _step("Commit updated manifest")
    condition = step["if"]
    assert "steps.month.outputs.should_run == 'true'" in condition
    assert "inputs.skip_upload != true" in condition
    # Sanity: garante que ainda é o step que de fato publica em main —
    # se o nome do step mudar sem atualizar este teste, isto falha alto.
    assert "git commit" in step["run"]
    assert "git push origin HEAD:main" in step["run"]


def test_dry_run_summary_step_is_mutually_exclusive_with_commit() -> None:
    step = _step("Dry-run summary")
    condition = step["if"]
    assert "steps.month.outputs.should_run == 'true'" in condition
    assert "inputs.skip_upload == true" in condition
    assert "git commit" not in step.get("run", "")
    assert "git push" not in step.get("run", "")


def test_skip_upload_input_documents_manifest_behavior() -> None:
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "Dry-run local: não faz upload nem publica o manifesto" in text


# -----------------------------------------------------------------------------
# Finding D do review do owner na PR #70: transform_metrics.json (RFC 0001
# §16/19 — baseline por estágio, RSS, pico de disco/filesystem, chunks)
# nunca saía do runner. Sem um step de artifact, os dados mais valiosos pra
# diagnosticar um incidente (métricas parciais de uma falha no meio do
# pipeline) desapareciam junto com o runner ao fim do job.
# -----------------------------------------------------------------------------


def test_upload_transform_metrics_step_exists_and_runs_always() -> None:
    step = _step("Upload transform metrics")
    # always() precisa estar presente -- é justamente quando o pipeline
    # falha no meio que as métricas parciais mais importam pro diagnóstico.
    assert "always()" in step["if"]
    assert "steps.month.outputs.should_run == 'true'" in step["if"]


def test_upload_transform_metrics_step_uses_upload_artifact_with_expected_shape() -> None:
    step = _step("Upload transform metrics")
    assert step["uses"].startswith("actions/upload-artifact@")
    with_block = step["with"]
    assert with_block["name"] == "transform-metrics-${{ steps.month.outputs.value }}"
    assert with_block["path"] == (
        "etl/.cache/${{ steps.month.outputs.value }}/metrics/transform_metrics.json"
    )
    # warn, não error: uma falha cedo o bastante pra nem criar o diretório
    # de métricas não pode reprovar o job só por causa deste step.
    assert with_block["if-no-files-found"] == "warn"


def test_upload_transform_metrics_step_runs_before_manifest_commit() -> None:
    """Ordem importa só por higiene de leitura do workflow (não há
    dependência de dado entre os dois steps) -- upload de métricas antes do
    commit do manifest, ambos após "Run pipeline"."""
    steps = _load_workflow()["jobs"]["run"]["steps"]
    names = [s.get("name") for s in steps]
    assert names.index("Run pipeline") < names.index("Upload transform metrics")
    assert names.index("Upload transform metrics") < names.index("Commit updated manifest")
