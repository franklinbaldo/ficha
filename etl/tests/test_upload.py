from pathlib import Path

import pytest

from ficha_etl import upload


def _build_outputs(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "cnpjs.parquet").write_bytes(b"PAR1\x00")
    (output_dir / "raizes.parquet").write_bytes(b"PAR1\x00")
    (output_dir / "socios.parquet").write_bytes(b"PAR1\x00")
    (output_dir / "lookups.json").write_text("{}")


def _build_raw(raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "Empresas0.zip").write_bytes(b"PK\x03\x04")
    (raw_dir / "Cnaes.zip").write_bytes(b"PK\x03\x04")


def test_build_upload_plan_outputs_only(tmp_path):
    out = tmp_path / "out"
    _build_outputs(out)
    plan = upload.build_upload_plan("2026-04", output_dir=out)
    assert plan.item_id == "ficha-2026-04"
    assert set(plan.files.keys()) == {
        "cnpjs.parquet",
        "raizes.parquet",
        "socios.parquet",
        "lookups.json",
    }


def test_build_upload_plan_with_raw(tmp_path):
    out = tmp_path / "out"
    raw = tmp_path / "raw"
    _build_outputs(out)
    _build_raw(raw)
    plan = upload.build_upload_plan("2026-04", output_dir=out, raw_dir=raw)
    assert "raw/Empresas0.zip" in plan.files
    assert "raw/Cnaes.zip" in plan.files
    assert "cnpjs.parquet" in plan.files


def test_build_upload_plan_missing_output(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "cnpjs.parquet").write_bytes(b"x")  # só 1 dos 4
    with pytest.raises(FileNotFoundError, match="raizes.parquet"):
        upload.build_upload_plan("2026-04", output_dir=out)


def test_build_upload_plan_missing_raw(tmp_path):
    out = tmp_path / "out"
    _build_outputs(out)
    with pytest.raises(FileNotFoundError, match="raw dir not found"):
        upload.build_upload_plan(
            "2026-04", output_dir=out, raw_dir=tmp_path / "nonexistent"
        )


def test_upload_snapshot_requires_credentials(tmp_path, monkeypatch):
    monkeypatch.delenv("IA_ACCESS_KEY", raising=False)
    monkeypatch.delenv("IA_SECRET_KEY", raising=False)
    out = tmp_path / "out"
    _build_outputs(out)
    plan = upload.build_upload_plan("2026-04", output_dir=out)
    with pytest.raises(RuntimeError, match="IA_ACCESS_KEY"):
        upload.upload_snapshot(plan)


def test_upload_snapshot_calls_ia_per_file(tmp_path, monkeypatch):
    """Mocka session/item.upload_file e confirma que cada arquivo é chamado."""
    monkeypatch.setenv("IA_ACCESS_KEY", "fake")
    monkeypatch.setenv("IA_SECRET_KEY", "fake")
    out = tmp_path / "out"
    _build_outputs(out)

    calls: list[tuple[str, str]] = []

    class _FakeResp:
        status_code = 200

    class _FakeItem:
        def upload_file(self, local, key, **kwargs):
            calls.append((local, key))
            return [_FakeResp()]

    class _FakeSession:
        def get_item(self, item_id):
            assert item_id == "ficha-2026-04"
            return _FakeItem()

    monkeypatch.setattr(upload, "get_session", lambda **kwargs: _FakeSession())

    plan = upload.build_upload_plan("2026-04", output_dir=out)
    results = upload.upload_snapshot(plan, verbose=False)
    assert len(calls) == 4
    keys = {key for _, key in calls}
    assert keys == {
        "cnpjs.parquet",
        "raizes.parquet",
        "socios.parquet",
        "lookups.json",
    }
    assert all(status == "uploaded" for status in results.values())


def test_upload_snapshot_skipped_when_ia_returns_empty(tmp_path, monkeypatch):
    """Quando o arquivo já existe idêntico, internetarchive retorna [] — tratamos como skipped."""
    monkeypatch.setenv("IA_ACCESS_KEY", "fake")
    monkeypatch.setenv("IA_SECRET_KEY", "fake")
    out = tmp_path / "out"
    _build_outputs(out)

    class _FakeItem:
        def upload_file(self, *args, **kwargs):
            return []  # IA already has identical file

    class _FakeSession:
        def get_item(self, item_id):
            return _FakeItem()

    monkeypatch.setattr(upload, "get_session", lambda **kwargs: _FakeSession())

    plan = upload.build_upload_plan("2026-04", output_dir=out)
    results = upload.upload_snapshot(plan, verbose=False)
    assert all(status == "skipped" for status in results.values())


def test_upload_snapshot_reports_http_failure_status(tmp_path, monkeypatch):
    """Falha HTTP transitória (5xx) deve aparecer como 'http_503' no resultado.

    Crítico: `_cmd_run` precisa detectar isso e abortar antes de atualizar manifest.
    """
    monkeypatch.setenv("IA_ACCESS_KEY", "fake")
    monkeypatch.setenv("IA_SECRET_KEY", "fake")
    out = tmp_path / "out"
    _build_outputs(out)

    class _FakeRespFail:
        status_code = 503

    class _FakeItem:
        def upload_file(self, *args, **kwargs):
            return [_FakeRespFail()]

    class _FakeSession:
        def get_item(self, item_id):
            return _FakeItem()

    monkeypatch.setattr(upload, "get_session", lambda **kwargs: _FakeSession())

    plan = upload.build_upload_plan("2026-04", output_dir=out)
    results = upload.upload_snapshot(plan, verbose=False)
    assert all(status == "http_503" for status in results.values())
