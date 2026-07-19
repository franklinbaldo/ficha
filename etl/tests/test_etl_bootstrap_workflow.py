"""Teste estático focado no step "Upload transform metrics" de etl-bootstrap.yml.

Finding D do review do owner na PR #70: transform_metrics.json (RFC 0001
§16/19 — baseline por estágio, RSS, pico de disco/filesystem, chunks) nunca
saía do runner em nenhum dos dois workflows (monthly e bootstrap). Este
módulo cobre só o bootstrap; o par completo (existência + always() +
shape do upload-artifact + ordem) já vive em test_etl_monthly_workflow.py
para o workflow mensal.
"""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "etl-bootstrap.yml"


def _load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _step(name: str) -> dict:
    for step in _load_workflow()["jobs"]["bootstrap"]["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"step {name!r} não encontrado em {WORKFLOW_PATH.name}")


def test_workflow_is_valid_yaml() -> None:
    workflow = _load_workflow()
    assert workflow["jobs"]["bootstrap"]["steps"], "job 'bootstrap' sem steps"


def test_upload_transform_metrics_step_exists_and_runs_always() -> None:
    step = _step("Upload transform metrics")
    # always() -- é justamente quando "Run pipeline" falha no meio que as
    # métricas parciais mais importam pro diagnóstico do incidente.
    assert step["if"] == "always()"


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


def test_upload_transform_metrics_step_runs_after_run_pipeline() -> None:
    steps = _load_workflow()["jobs"]["bootstrap"]["steps"]
    names = [s.get("name") for s in steps]
    assert names.index("Run pipeline") < names.index("Upload transform metrics")
