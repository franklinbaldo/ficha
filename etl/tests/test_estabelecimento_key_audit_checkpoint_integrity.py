"""Regression tests for the checkpoint-integrity guarantees from issue #103."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

from ficha_etl import estabelecimento_key_audit as key_audit
from ficha_etl import registry


def _row() -> dict[str, str]:
    row = dict.fromkeys(registry.ESTABELECIMENTO_COLUMNS, "")
    row.update(
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "identificador_matriz_filial": "1",
            "situacao_cadastral": "02",
            "data_situacao_cadastral": "20260719",
            "data_inicio_atividade": "19991231",
        }
    )
    return row


def _write_zip(path: Path) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    row = _row()
    writer.writerow([row[name] for name in registry.ESTABELECIMENTO_COLUMNS])
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.ESTABELE", buffer.getvalue().encode("latin-1"))


def _count_rebuilds(monkeypatch):
    calls = {"count": 0}
    original = key_audit.run_part_key_audit_with_metrics

    def counted(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(key_audit, "run_part_key_audit_with_metrics", counted)
    return calls


def test_reader_semantics_are_part_of_checkpoint_fingerprint():
    fingerprints = key_audit._code_fingerprints()  # noqa: SLF001

    assert set(fingerprints) >= {"estabelecimento_key_audit", "registry", "transform"}
    assert len(fingerprints["transform"]) == 64


def test_local_override_checkpoint_is_not_reused_as_remote_evidence(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path)

    first = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    assert first.manifest["source"]["acquisition"] == "local-override"

    calls = _count_rebuilds(monkeypatch)
    second = key_audit.run_part_checkpoint("2026-04", 0, root)

    assert second.reused is False
    assert calls["count"] == 1
    assert second.manifest["source"]["acquisition"] == "local-cache"


def test_tampered_report_invalidates_checkpoint_reuse(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path)

    first = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    first.report_path.write_text('{"tampered": true}\n', encoding="utf-8")

    calls = _count_rebuilds(monkeypatch)
    second = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)

    assert second.reused is False
    assert calls["count"] == 1


def test_tampered_metrics_invalidates_checkpoint_reuse(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path)

    key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    metrics_path = root / "evidence" / "part-0.key-audit.metrics.json"
    metrics_path.write_text('{"tampered": true}\n', encoding="utf-8")

    calls = _count_rebuilds(monkeypatch)
    second = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)

    assert second.reused is False
    assert calls["count"] == 1
