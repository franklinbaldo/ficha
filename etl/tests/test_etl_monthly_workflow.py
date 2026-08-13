"""Testes estáticos para o workflow mensal shardado.

A publicação agora é uma sequência explícita de jobs. Estes testes protegem as
fronteiras que importam para correção/retomada:

- dry-run para antes de qualquer upload/promoção;
- descriptor de produção é persistido antes do upload dos derivados;
- métricas continuam sobrevivendo a falhas do job de produção;
- promoção só existe depois do gate de shards;
- o manifest candidato é preservado antes do push em ``main``.
"""

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / ".github" / "workflows" / "etl-monthly.yml"


def _load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _job(name: str) -> dict:
    jobs = _load_workflow()["jobs"]
    assert name in jobs, f"job {name!r} não encontrado em {WORKFLOW_PATH.name}"
    return jobs[name]


def _step(job_name: str, name: str) -> dict:
    for step in _job(job_name)["steps"]:
        if step.get("name") == name:
            return step
    raise AssertionError(f"step {name!r} não encontrado no job {job_name!r}")


def test_workflow_is_valid_yaml_and_has_explicit_publication_phases() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert list(jobs) == ["resolve", "produce", "inputs-visible", "shards", "finalize"]
    for name in jobs:
        assert jobs[name]["steps"], f"job {name!r} sem steps"


def test_produce_job_is_gated_by_resolved_freshness() -> None:
    condition = _job("produce")["if"]
    assert "needs.resolve.outputs.should_run == 'true'" in condition


def test_dry_run_stops_before_output_upload_and_promotion() -> None:
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "Dry-run: produz descriptor local, sem upload/shards/promoção" in text

    upload_step = _step("produce", "Upload derived outputs exactly as produced")
    assert "inputs.skip_upload != true" in upload_step["if"]

    dry_step = _step("produce", "Dry-run summary")
    assert "inputs.skip_upload == true" in dry_step["if"]
    assert "git commit" not in dry_step.get("run", "")
    assert "git push" not in dry_step.get("run", "")

    inputs_visible = _job("inputs-visible")
    assert "inputs.skip_upload != true" in inputs_visible["if"]


def test_production_descriptor_is_persisted_before_output_upload() -> None:
    steps = _job("produce")["steps"]
    names = [step.get("name") for step in steps]
    assert names.index("Persist production descriptor before upload") < names.index(
        "Upload derived outputs exactly as produced"
    )

    artifact = _step("produce", "Persist production descriptor before upload")
    assert artifact["uses"].startswith("actions/upload-artifact@")
    with_block = artifact["with"]
    assert with_block["name"] == "production-descriptor-${{ needs.resolve.outputs.month }}"
    assert with_block["if-no-files-found"] == "error"
    assert with_block["retention-days"] == 90


def test_upload_transform_metrics_step_exists_and_runs_always() -> None:
    step = _step("produce", "Upload transform metrics")
    assert "always()" in step["if"]
    assert step["uses"].startswith("actions/upload-artifact@")
    with_block = step["with"]
    assert with_block["name"] == "transform-metrics-${{ needs.resolve.outputs.month }}"
    assert with_block["path"] == (
        "etl/.cache/${{ needs.resolve.outputs.month }}/metrics/transform_metrics.json"
    )
    assert with_block["if-no-files-found"] == "warn"


def test_metrics_and_descriptor_are_persisted_before_output_upload() -> None:
    names = [step.get("name") for step in _job("produce")["steps"]]
    upload_index = names.index("Upload derived outputs exactly as produced")
    assert names.index("Persist production descriptor before upload") < upload_index
    assert names.index("Upload transform metrics") < upload_index


def test_finalize_requires_successful_shards_and_preserves_candidate_before_push() -> None:
    job = _job("finalize")
    assert job["needs"] == ["resolve", "shards"]
    assert "needs.shards.result == 'success'" in job["if"]

    steps = job["steps"]
    names = [step.get("name") for step in steps]
    assert names.index(
        "Verify remote bytes against production receipts and build candidate"
    ) < names.index("Persist exact promoted candidate and evidence")
    assert names.index("Persist exact promoted candidate and evidence") < names.index(
        "Publish verified manifest"
    )

    publish = _step("finalize", "Publish verified manifest")
    assert "git commit" in publish["run"]
    assert "git push origin HEAD:main" in publish["run"]


def test_shard_rerun_compares_receipt_before_overwriting_artifact_or_put() -> None:
    steps = _job("shards")["steps"]
    names = [step.get("name") for step in steps]
    assert names.index("Restore previous receipt on job rerun") < names.index(
        "Produce shard bytes and receipt — no remote write"
    )
    assert names.index("Produce shard bytes and receipt — no remote write") < names.index(
        "Require receipt stability across attempts"
    )
    assert names.index("Require receipt stability across attempts") < names.index(
        "Persist shard production receipt before PUT"
    )
    assert names.index("Persist shard production receipt before PUT") < names.index(
        "Submit exactly the receipted bytes"
    )
    artifact = _step("shards", "Persist shard production receipt before PUT")
    assert artifact["with"]["overwrite"] is True
    compare = _step("shards", "Require receipt stability across attempts")["run"]
    assert "cmp -s" in compare
