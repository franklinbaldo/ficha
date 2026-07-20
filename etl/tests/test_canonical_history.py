"""Historical canonical shadow orchestration tests."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest

from ficha_etl import canonical_history, canonical_shadow, registry


def _row(**overrides: str) -> dict[str, str]:
    row = {name: "" for name in registry.ESTABELECIMENTO_COLUMNS}
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
    row.update(overrides)
    return row


def _zip_bytes(rows: list[dict[str, str]], *, extra_file: bool = False) -> bytes:
    csv_buffer = io.StringIO(newline="")
    writer = csv.writer(
        csv_buffer,
        delimiter=";",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
    )
    writer.writerows([[row[name] for name in registry.ESTABELECIMENTO_COLUMNS] for row in rows])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.ESTABELE", csv_buffer.getvalue().encode("latin-1"))
        if extra_file:
            archive.writestr("unexpected.txt", b"extra")
    return output.getvalue()


def _write_zip(path: Path, rows: list[dict[str, str]], *, extra_file: bool = False) -> None:
    path.write_bytes(_zip_bytes(rows, extra_file=extra_file))


def test_remote_contract_uses_historical_ia_raw_path():
    remote = canonical_history.estabelecimento_remote("2026-04", 3)
    assert remote.name == "Estabelecimentos3.zip"
    assert remote.kind == "estabelecimentos"
    assert remote.url.endswith("/ficha-2026-04/raw/Estabelecimentos3.zip")

    with pytest.raises(ValueError, match="YYYY-MM"):
        canonical_history.estabelecimento_remote("April-2026", 0)
    with pytest.raises(ValueError, match="between 0 and 9"):
        canonical_history.estabelecimento_remote("2026-04", 10)


def test_local_override_writes_manifest_and_reuses_checksums(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row(nome_fantasia="Linha 1\nLinha 2")])

    first = canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        zip_override=zip_path,
        sample_size=10,
    )
    assert first.reused is False
    assert first.output_path.exists()
    manifest = json.loads(first.manifest_path.read_text())
    assert manifest["status"] == "ok"
    assert manifest["source"]["acquisition"] == "local-override"
    assert manifest["source"]["name"] == "Estabelecimentos0.zip"
    assert manifest["sample_size"] == 10
    assert len(manifest["source"]["zip"]["sha256"]) == 64
    assert len(manifest["output"]["sha256"]) == 64
    assert not (root / "extracted").exists()

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("writer ran despite a valid checksummed checkpoint")

    monkeypatch.setattr(canonical_shadow, "run_shadow_part", should_not_run)
    second = canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        zip_override=zip_path,
        sample_size=10,
    )
    assert second.reused is True
    assert second.manifest == manifest


def test_downloads_from_ia_url_with_mock_transport(tmp_path):
    payload = _zip_bytes([_row()])
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            200,
            content=payload,
            headers={"content-length": str(len(payload))},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = canonical_history.run_historical_shadow(
            "2026-04",
            2,
            tmp_path / "run",
            client=client,
            sample_size=1,
        )

    assert result.reused is False
    assert seen == ["https://archive.org/download/ficha-2026-04/raw/Estabelecimentos2.zip"]
    assert result.manifest["source"]["acquisition"] == "downloaded"


def test_tampered_output_invalidates_resume_but_reuses_cached_zip(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()])
    first = canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        zip_override=zip_path,
    )
    first.output_path.write_bytes(b"tampered")

    calls = 0
    original = canonical_shadow.run_shadow_part

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(canonical_shadow, "run_shadow_part", counted)
    second = canonical_history.run_historical_shadow("2026-04", 0, root)

    assert calls == 1
    assert second.reused is False
    assert second.manifest["source"]["acquisition"] == "local-cache"
    assert second.output_path.read_bytes() != b"tampered"


def test_multiple_zip_members_fail_with_durable_history_evidence(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()], extra_file=True)

    with pytest.raises(RuntimeError, match="expected exactly one extracted CSV"):
        canonical_history.run_historical_shadow(
            "2026-04",
            0,
            root,
            zip_override=zip_path,
        )

    failure_path = root / "evidence" / "part-0.history.failure.json"
    failure = json.loads(failure_path.read_text())
    assert failure["status"] == "failed"
    assert failure["source"]["name"] == "Estabelecimentos0.zip"
    assert len(failure["source"]["zip"]["sha256"]) == 64
    assert "expected exactly one" in failure["error"]


def test_sample_size_or_code_change_invalidates_resume(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()])
    canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        zip_override=zip_path,
        sample_size=1,
    )

    calls = 0
    original = canonical_shadow.run_shadow_part

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(canonical_shadow, "run_shadow_part", counted)
    changed_sample = canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        sample_size=0,
    )
    assert changed_sample.reused is False
    assert calls == 1

    original_fingerprints = canonical_history._code_fingerprints  # noqa: SLF001
    monkeypatch.setattr(
        canonical_history,
        "_code_fingerprints",
        lambda: {**original_fingerprints(), "canonical_history": "changed"},
    )
    changed_code = canonical_history.run_historical_shadow(
        "2026-04",
        0,
        root,
        sample_size=0,
    )
    assert changed_code.reused is False
    assert calls == 2
