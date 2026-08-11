"""Guard estático: artifact de workflow não pode afirmar um estado de publicação
que o run não alcançou (#131).

Motivação concreta: o run 31450937194 publicou um artifact chamado
`final-manifest-2026-05` que continha, na prática, o manifesto de `2026-04`
intocado — o pipeline havia sido cancelado durante o upload de `companies.zip`
e nunca chegou a `build_snapshot_entry()`. O gate real funcionou (o step de
commit foi corretamente pulado); quem mentiu foi o *nome*.

O problema é de vocabulário. O pipeline distingue cinco estados e os nomes
atuais não os separam (ver docs/adr/0025):

  1. tentativa de publicação      4. manifesto promovido
  2. materialização da competência 5. estado público final
  3. manifesto candidato

Estas regras são deliberadamente pequenas: não validam o pipeline, só impedem
que um artifact anuncie (4)/(5) quando o run só alcançou (1).
"""

from pathlib import Path

import pytest
import yaml

WORKFLOWS_DIR = Path(__file__).parents[2] / ".github" / "workflows"

# Tokens que, no nome de um artifact, anunciam que a publicação se concretizou.
PROMOTED_TOKENS = ("promoted", "final", "published")

# Prefixos que declaram explicitamente um manifesto ainda NÃO promovido.
NON_PROMOTED_PREFIXES = ("manifest-before", "manifest-candidate", "manifest-attempt")

PUBLIC_MANIFEST = "web/public/manifest.json"


def _iter_steps(workflow: dict):
    for job_name, job in (workflow.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            yield job_name, step


def _is_artifact_upload(step: dict) -> bool:
    return "actions/upload-artifact" in str(step.get("uses", ""))


def _is_ungated(condition: str) -> bool:
    """True quando o step roda mesmo com o gate de publicação reprovado.

    `always()` é o caso explícito. A ausência de `if` também conta como
    gated: o Actions só roda o step se os anteriores tiveram sucesso.
    """
    return "always()" in condition.replace(" ", "")


def _claims_promotion(artifact_name: str) -> bool:
    return any(token in artifact_name.lower() for token in PROMOTED_TOKENS)


def _declares_non_promoted(artifact_name: str) -> bool:
    return artifact_name.lower().startswith(NON_PROMOTED_PREFIXES)


def publication_naming_violations(workflow: dict, source: str) -> list[str]:
    """Devolve as violações de nomenclatura de publicação de um workflow."""
    problems: list[str] = []

    for job_name, step in _iter_steps(workflow):
        if not _is_artifact_upload(step):
            continue

        where = f"{source}:{job_name}:{step.get('name', '<sem nome>')}"
        condition = str(step.get("if", ""))
        with_block = step.get("with") or {}
        artifact_name = str(with_block.get("name", ""))
        path = str(with_block.get("path", ""))

        # R1 — um artifact que anuncia promoção só pode existir quando o gate
        # de promoção passou. `always()` o produz inclusive em run abortado.
        if _claims_promotion(artifact_name) and _is_ungated(condition):
            problems.append(
                f"{where}: artifact {artifact_name!r} anuncia publicação concretizada "
                f"mas roda sob `if: {condition}` — seria produzido mesmo com o run "
                f"abortado antes de build_snapshot_entry()/verify_snapshot_files()"
            )

        # R2 — o manifesto do working tree é o estado (1)/(3). Publicá-lo sem
        # gate exige um nome que declare que ele não foi promovido.
        if PUBLIC_MANIFEST in path and _is_ungated(condition):
            if not _declares_non_promoted(artifact_name):
                problems.append(
                    f"{where}: artifact {artifact_name!r} publica {PUBLIC_MANIFEST} "
                    f"sob `if: {condition}` sem declarar que não foi promovido — "
                    f"use um dos prefixos {NON_PROMOTED_PREFIXES}"
                )

    return problems


def _workflow_files() -> list[Path]:
    return sorted(WORKFLOWS_DIR.glob("*.yml"))


def test_workflows_directory_is_discoverable() -> None:
    # Sem isto, um erro de path tornaria o teste abaixo vacuamente verde.
    assert _workflow_files(), f"nenhum workflow encontrado em {WORKFLOWS_DIR}"


@pytest.mark.parametrize("path", _workflow_files(), ids=lambda p: p.name)
def test_workflow_artifacts_do_not_overstate_publication(path: Path) -> None:
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    problems = publication_naming_violations(workflow, path.name)
    assert not problems, "\n".join(problems)


# --- provas de que as regras realmente pegam o caso observado -----------------
# Sem estes casos sintéticos o teste acima passaria mesmo com as regras quebradas,
# já que hoje nenhum workflow em main publica artifact de manifesto.


def _synthetic(step: dict) -> dict:
    return {"jobs": {"job": {"steps": [step]}}}


def test_rule_catches_the_artifact_from_run_31450937194() -> None:
    """A forma exata que motivou #131."""
    workflow = _synthetic(
        {
            "name": "Upload manifest evidence",
            "if": "always()",
            "uses": "actions/upload-artifact@v4",
            "with": {"name": "final-manifest-2026-05", "path": PUBLIC_MANIFEST},
        }
    )
    problems = publication_naming_violations(workflow, "synthetic.yml")
    assert len(problems) == 2, problems
    assert "anuncia publicação concretizada" in problems[0]
    assert "sem declarar que não foi promovido" in problems[1]


def test_rule_accepts_promoted_artifact_gated_on_promotion_outcome() -> None:
    workflow = _synthetic(
        {
            "name": "Upload manifest evidence",
            "if": "steps.promote.outcome == 'success'",
            "uses": "actions/upload-artifact@v4",
            "with": {"name": "manifest-promoted-2026-05", "path": PUBLIC_MANIFEST},
        }
    )
    assert publication_naming_violations(workflow, "synthetic.yml") == []


def test_rule_accepts_ungated_artifact_that_declares_non_promoted_state() -> None:
    workflow = _synthetic(
        {
            "name": "Upload manifest snapshot before the run",
            "if": "always()",
            "uses": "actions/upload-artifact@v4",
            "with": {"name": "manifest-before-2026-05", "path": PUBLIC_MANIFEST},
        }
    )
    assert publication_naming_violations(workflow, "synthetic.yml") == []


def test_rule_ignores_non_manifest_artifacts() -> None:
    """Métricas parciais sob always() são desejáveis — e não afirmam publicação."""
    workflow = _synthetic(
        {
            "name": "Upload transform metrics",
            "if": "always()",
            "uses": "actions/upload-artifact@v4",
            "with": {"name": "transform-metrics-2026-05", "path": "etl/.cache/m.json"},
        }
    )
    assert publication_naming_violations(workflow, "synthetic.yml") == []
