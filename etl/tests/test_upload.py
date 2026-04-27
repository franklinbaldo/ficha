"""Testes para ficha_etl.upload (ia.upload mockado)."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ficha_etl import upload as upload_mod
from ficha_etl.mirror import item_id
from ficha_etl.sources import canonical_inventory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status: int = 200, url: str = "https://s3.us.archive.org/test") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.url = url
    return r


def _make_outputs(output_dir: Path) -> None:
    """Cria os 4 arquivos de output com conteúdo mínimo."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in ("cnpjs.parquet", "raizes.parquet", "socios.parquet"):
        (output_dir / name).write_bytes(b"PAR1" + b"\x00" * 20)  # header mínimo falso
    (output_dir / "lookups.json").write_text('{"schema_version":"1.0.0"}')


def _make_raw_zips(cache_dir: Path, month: str) -> list[str]:
    """Cria ZIPs stub no cache_dir/month/ para todos os 37 canônicos."""
    raw_dir = cache_dir / month
    raw_dir.mkdir(parents=True, exist_ok=True)
    names = []
    for spec in canonical_inventory():
        zp = raw_dir / spec.name
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr(f"{spec.name}.csv", b"data")
        names.append(spec.name)
    return names


# ---------------------------------------------------------------------------
# upload_outputs
# ---------------------------------------------------------------------------


@patch("ficha_etl.upload.ia.upload")
def test_upload_outputs_calls_ia_with_correct_files(mock_upload, tmp_path):
    output_dir = tmp_path / "output"
    _make_outputs(output_dir)

    mock_upload.return_value = [_make_response()]

    upload_mod.upload_outputs(
        "2026-04",
        output_dir,
        access_key="ACCESS",
        secret_key="SECRET",
    )

    assert mock_upload.called
    _, kwargs = mock_upload.call_args
    files_arg = mock_upload.call_args[1].get("files") or mock_upload.call_args[0][1]
    assert "cnpjs.parquet" in files_arg
    assert "raizes.parquet" in files_arg
    assert "socios.parquet" in files_arg
    assert "lookups.json" in files_arg


@patch("ficha_etl.upload.ia.upload")
def test_upload_outputs_uses_correct_identifier(mock_upload, tmp_path):
    output_dir = tmp_path / "output"
    _make_outputs(output_dir)
    mock_upload.return_value = [_make_response()]

    upload_mod.upload_outputs("2026-04", output_dir, access_key="A", secret_key="S")

    identifier_arg = mock_upload.call_args[0][0]
    assert identifier_arg == item_id("2026-04")


@patch("ficha_etl.upload.ia.upload")
def test_upload_outputs_raises_on_missing_file(mock_upload, tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    # Só cria 3 dos 4 arquivos
    for name in ("cnpjs.parquet", "raizes.parquet", "socios.parquet"):
        (output_dir / name).write_bytes(b"x")
    # lookups.json ausente

    with pytest.raises(FileNotFoundError, match="lookups.json"):
        upload_mod.upload_outputs("2026-04", output_dir, access_key="A", secret_key="S")

    mock_upload.assert_not_called()


@patch("ficha_etl.upload.ia.upload")
def test_upload_outputs_raises_on_http_error(mock_upload, tmp_path):
    output_dir = tmp_path / "output"
    _make_outputs(output_dir)
    mock_upload.return_value = [_make_response(status=503)]

    with pytest.raises(RuntimeError, match="503"):
        upload_mod.upload_outputs("2026-04", output_dir, access_key="A", secret_key="S")


@patch("ficha_etl.upload.ia.upload")
def test_upload_outputs_invalid_month(mock_upload, tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM"):
        upload_mod.upload_outputs("26-4", tmp_path, access_key="A", secret_key="S")
    mock_upload.assert_not_called()


# ---------------------------------------------------------------------------
# upload_raw_zips
# ---------------------------------------------------------------------------


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_uses_raw_prefix(mock_upload, tmp_path):
    month = "2026-04"
    _make_raw_zips(tmp_path, month)
    mock_upload.return_value = [_make_response()]

    upload_mod.upload_raw_zips(month, tmp_path, access_key="A", secret_key="S")

    files_arg = mock_upload.call_args[1].get("files") or mock_upload.call_args[0][1]
    # Todos os remote names devem começar com "raw/"
    assert all(k.startswith("raw/") for k in files_arg)


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_uploads_all_37(mock_upload, tmp_path):
    month = "2026-04"
    _make_raw_zips(tmp_path, month)
    mock_upload.return_value = [_make_response()]

    upload_mod.upload_raw_zips(month, tmp_path, access_key="A", secret_key="S")

    files_arg = mock_upload.call_args[1].get("files") or mock_upload.call_args[0][1]
    assert len(files_arg) == 37


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_raises_if_no_zips_found(mock_upload, tmp_path):
    """Se o cache estiver vazio, levanta FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="no raw ZIPs"):
        upload_mod.upload_raw_zips("2026-04", tmp_path, access_key="A", secret_key="S")
    mock_upload.assert_not_called()


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_skips_zero_byte_files(mock_upload, tmp_path):
    month = "2026-04"
    _make_raw_zips(tmp_path, month)
    # Torna um ZIP zero-byte
    first_zip = tmp_path / month / next(iter(canonical_inventory())).name
    first_zip.write_bytes(b"")

    mock_upload.return_value = [_make_response()]
    upload_mod.upload_raw_zips(month, tmp_path, access_key="A", secret_key="S")

    files_arg = mock_upload.call_args[1].get("files") or mock_upload.call_args[0][1]
    # Deve ter 36 (37 - 1 zero-byte)
    assert len(files_arg) == 36


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_raises_on_http_error(mock_upload, tmp_path):
    month = "2026-04"
    _make_raw_zips(tmp_path, month)
    mock_upload.return_value = [_make_response(status=500)]

    with pytest.raises(RuntimeError, match="500"):
        upload_mod.upload_raw_zips(month, tmp_path, access_key="A", secret_key="S")
