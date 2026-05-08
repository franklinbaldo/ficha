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


# ---------------------------------------------------------------------------
# Streaming retry path (zero-disk)
# ---------------------------------------------------------------------------


def test_ias3_error_message_is_ascii():
    """Error messages must be ASCII so they survive a runner with stderr=ascii.

    Production was hitting `'ascii' codec can't encode character '\\u2014'`
    because the original RuntimeError contained an em-dash.
    """
    exc = upload_mod._IAS3Error(500, "https://s3.us.archive.org/x/y", body="boom")
    str(exc).encode("ascii")  # raises if any non-ASCII char slipped in
    assert exc.status == 500


def test_ia_s3_put_metadata_headers_are_ascii(monkeypatch):
    """`x-archive-meta-*` headers go on the wire as HTTP headers, which must
    be ASCII. A single em-dash in the title crashes httpx with
    UnicodeEncodeError BEFORE the PUT goes out — see PR #24, run
    25502969568 where 36/37 ZIPs uploaded but the is_first worker died.
    """
    sent_headers: dict[str, str] = {}

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def put(self, url, content=None, headers=None):
            sent_headers.update(headers or {})
            return _FakeResp(200)

    class _FakeResp:
        def __init__(self, status):
            self.status_code = status
            self.text = ""

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)

    upload_mod._ia_s3_put(
        "ficha-2026-04",
        "raw/Empresas0.zip",
        iter([b"x"]),
        content_length="1",
        access_key="A",
        secret_key="S",
        is_first=True,
    )

    assert sent_headers, "headers should have been captured"
    for k, v in sent_headers.items():
        try:
            v.encode("ascii")
        except UnicodeEncodeError:
            pytest.fail(f"header {k!r} has non-ASCII value: {v!r}")


def test_stream_one_zip_with_retry_retries_transient(monkeypatch):
    """Transient IA S3 errors (5xx, 409) should be retried up to _RETRIES."""
    calls = {"n": 0}

    def fake_stream_one_zip(spec, **_kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise upload_mod._IAS3Error(500, "https://s3.us.archive.org/x/y")
        return spec.name

    monkeypatch.setattr(upload_mod, "_stream_one_zip", fake_stream_one_zip)
    monkeypatch.setattr(upload_mod.time, "sleep", lambda *_: None)

    spec = next(iter(canonical_inventory()))
    name = upload_mod._stream_one_zip_with_retry(
        spec,
        rfb_token="t",
        month="2026-04",
        identifier=item_id("2026-04"),
        access_key="A",
        secret_key="S",
        is_first=True,
    )
    assert name == spec.name
    assert calls["n"] == 3


def test_stream_one_zip_with_retry_does_not_retry_non_transient(monkeypatch):
    """Non-transient client errors (e.g. 401, 403) should fail immediately."""
    calls = {"n": 0}

    def fake_stream_one_zip(spec, **_kw):
        calls["n"] += 1
        raise upload_mod._IAS3Error(401, "https://s3.us.archive.org/x/y")

    monkeypatch.setattr(upload_mod, "_stream_one_zip", fake_stream_one_zip)
    monkeypatch.setattr(upload_mod.time, "sleep", lambda *_: None)

    spec = next(iter(canonical_inventory()))
    with pytest.raises(upload_mod._IAS3Error) as ei:
        upload_mod._stream_one_zip_with_retry(
            spec,
            rfb_token="t",
            month="2026-04",
            identifier=item_id("2026-04"),
            access_key="A",
            secret_key="S",
            is_first=False,
        )
    assert ei.value.status == 401
    assert calls["n"] == 1


def test_existing_raw_files_on_ia_parses_metadata(monkeypatch):
    """Existence check should return only `raw/*` names with non-zero size."""

    payload = {
        "files": [
            {"name": "raw/Empresas0.zip", "size": "494200000"},
            {"name": "raw/Empresas1.zip", "size": "74300000"},
            {"name": "raw/Cnaes.zip", "size": "0"},  # zero-size: not yet uploaded
            {"name": "cnpjs.parquet", "size": "3000000000"},  # not a raw zip
            {"name": "raw/Estabelecimentos0.zip"},  # missing size
        ]
    }

    class _FakeResp:
        status_code = 200

        def json(self):
            return payload

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            return _FakeResp()

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    out = upload_mod._existing_raw_files_on_ia("ficha-2026-04")
    assert out == {"raw/Empresas0.zip", "raw/Empresas1.zip"}


def test_existing_raw_files_on_ia_returns_empty_on_404(monkeypatch):
    class _FakeResp:
        status_code = 404

        def json(self):
            return {}

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            return _FakeResp()

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    assert upload_mod._existing_raw_files_on_ia("ficha-2099-01") == set()


def test_existing_raw_files_on_ia_returns_empty_on_network_error(monkeypatch):
    """A flaky IA metadata API shouldn't block streaming -- fall back to "stream all"."""

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            raise upload_mod.httpx.ConnectError("boom")

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    assert upload_mod._existing_raw_files_on_ia("ficha-2026-04") == set()


def test_stream_raw_zips_to_ia_skips_existing(monkeypatch):
    """When all ZIPs are already on IA, stream returns immediately AND does
    not contact RFB for a token. Recovery from a transform/upload failure
    must succeed even when RFB upstream is down or has rotated its token.
    """

    all_names = {f"raw/{spec.name}" for spec in canonical_inventory()}
    monkeypatch.setattr(upload_mod, "_existing_raw_files_on_ia", lambda _id: all_names)

    token_calls = {"n": 0}

    def fake_discover_token():
        token_calls["n"] += 1
        raise upload_mod.upstream.NoTokenError("RFB is hypothetically down")

    monkeypatch.setattr(upload_mod.upstream, "discover_token", fake_discover_token)

    streamed = {"n": 0}

    def fake_stream(*a, **kw):
        streamed["n"] += 1
        return "x"

    monkeypatch.setattr(upload_mod, "_stream_one_zip_with_retry", fake_stream)

    # Should be a no-op -- no streaming, no token lookup, no exception.
    upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")
    assert streamed["n"] == 0
    assert token_calls["n"] == 0, "should not contact RFB when nothing to stream"


def test_stream_one_zip_with_retry_gives_up_after_max_attempts(monkeypatch):
    calls = {"n": 0}

    def fake_stream_one_zip(spec, **_kw):
        calls["n"] += 1
        raise upload_mod._IAS3Error(503, "https://s3.us.archive.org/x/y")

    monkeypatch.setattr(upload_mod, "_stream_one_zip", fake_stream_one_zip)
    monkeypatch.setattr(upload_mod.time, "sleep", lambda *_: None)

    spec = next(iter(canonical_inventory()))
    with pytest.raises(upload_mod._IAS3Error) as ei:
        upload_mod._stream_one_zip_with_retry(
            spec,
            rfb_token="t",
            month="2026-04",
            identifier=item_id("2026-04"),
            access_key="A",
            secret_key="S",
            is_first=False,
        )
    assert ei.value.status == 503
    assert calls["n"] == upload_mod._RETRIES
