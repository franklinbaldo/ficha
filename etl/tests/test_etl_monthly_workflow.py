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
