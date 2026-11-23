"""Testes para ficha_etl.upload (ia.upload mockado)."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
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
    """Cria todos os arquivos de output com conteúdo mínimo."""
    from ficha_etl.transform import _LOOKUP_KINDS

    output_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "cnpjs.parquet",
        "cnpj_contatos.parquet",
        "cnpj_cnaes.parquet",
        "raizes.parquet",
        "socios.parquet",
        "enderecos.parquet",
        "pessoas.parquet",
    ):
        (output_dir / name).write_bytes(b"PAR1" + b"\x00" * 20)
    (output_dir / "lookups.json").write_text('{"schema_version":"1.0.0"}')
    (output_dir / "lookups").mkdir(parents=True, exist_ok=True)
    for kind in _LOOKUP_KINDS:
        (output_dir / "lookups" / f"{kind}.parquet").write_bytes(b"PAR1" + b"\x00" * 20)


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
    files_arg = mock_upload.call_args[1].get("files") or mock_upload.call_args[0][1]
    for expected in (
        "cnpjs.parquet",
        "cnpj_contatos.parquet",
        "cnpj_cnaes.parquet",
        "raizes.parquet",
        "socios.parquet",
        "enderecos.parquet",
        "pessoas.parquet",
        "lookups.json",
    ):
        assert expected in files_arg, f"{expected!r} missing from upload files"


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
    # Só cria alguns parquets — lookups.json ausente
    for name in ("cnpjs.parquet", "raizes.parquet", "socios.parquet"):
        (output_dir / name).write_bytes(b"x")

    with pytest.raises(FileNotFoundError):
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
# upload_raw_zips (legacy raw cache mirror is deliberately disabled)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cache_state", ["empty", "one", "all", "zero_byte"])
@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_refuses_legacy_egress(mock_upload, tmp_path, cache_state):
    """A partial, empty or full cache cannot bypass raw custody admission."""
    month = "2026-04"
    if cache_state == "all":
        _make_raw_zips(tmp_path, month)
    elif cache_state in ("one", "zero_byte"):
        local = tmp_path / month / next(iter(canonical_inventory())).name
        local.parent.mkdir(parents=True)
        with zipfile.ZipFile(local, "w") as archive:
            archive.writestr("source.csv", b"cnpj;value\n1;A\n")
        if cache_state == "zero_byte":
            local.write_bytes(b"")

    with pytest.raises(RuntimeError, match="raw cache upload is disabled"):
        upload_mod.upload_raw_zips(month, tmp_path, access_key="A", secret_key="S")
    mock_upload.assert_not_called()


@patch("ficha_etl.upload.ia.upload")
def test_upload_raw_zips_invalid_month_remains_rejected(mock_upload, tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM"):
        upload_mod.upload_raw_zips("2026-13", tmp_path, access_key="A", secret_key="S")
    mock_upload.assert_not_called()


def test_upload_raw_zips_rejects_before_cache_or_remote_io(monkeypatch):
    """Refuse raw cache egress before accessing inventory, disk or IA."""

    class ForbiddenCache:
        def __truediv__(self, _part):
            pytest.fail("raw cache pathname inspected")

        def __fspath__(self):
            pytest.fail("raw cache pathname materialized")

    def forbid(*_args, **_kwargs):
        pytest.fail("raw helper crossed an I/O boundary")

    monkeypatch.setattr(upload_mod, "canonical_inventory", forbid)
    monkeypatch.setattr(upload_mod, "item_id", forbid)
    monkeypatch.setattr(upload_mod, "_existing_raw_files_on_ia", forbid)
    monkeypatch.setattr(upload_mod, "_ia_s3_put", forbid)
    monkeypatch.setattr(upload_mod.ia, "upload", forbid)
    monkeypatch.setattr(upload_mod.httpx, "Client", forbid)
    cache = ForbiddenCache()

    with pytest.raises(RuntimeError, match="raw cache upload is disabled"):
        upload_mod.upload_raw_zips("2026-04", cache, access_key="A", secret_key="S")


def test_legacy_raw_guard_does_not_cover_distinct_stream_entrypoint(monkeypatch, tmp_path):
    """Offline differential: legacy refusal is not a global raw writer fence."""
    metadata_lookups: list[str] = []

    def fake_inventory_lookup(identifier):
        metadata_lookups.append(identifier)
        return set()

    def forbid_external_io(*_args, **_kwargs):
        pytest.fail("external I/O crossed in differential writer test")

    monkeypatch.setattr(upload_mod, "_existing_raw_files_on_ia", fake_inventory_lookup)
    monkeypatch.setattr(upload_mod.upstream, "discover_token", forbid_external_io)
    monkeypatch.setattr(upload_mod, "_ia_s3_put", forbid_external_io)
    monkeypatch.setattr(upload_mod.ia, "upload", forbid_external_io)

    with pytest.raises(RuntimeError, match="raw cache upload is disabled"):
        upload_mod.upload_raw_zips("2026-04", tmp_path, access_key="A", secret_key="S")
    if metadata_lookups:
        raise AssertionError("legacy helper touched remote metadata before refusing")

    # The streaming path is separately callable. It consults a synthetic
    # catalog, then refuses without spool; this test never performs a PUT.
    with pytest.raises(RuntimeError, match="requires an existing explicit spool_dir"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S", spool_dir=None)
    if metadata_lookups != [item_id("2026-04")]:
        raise AssertionError("streaming entrypoint did not reach its independent admission")


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
            {"name": "raw/Estabelecimentos0.zip", "size": "0"},  # empty: not uploaded
        ]
    }

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            return httpx.Response(200, json=payload)

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    out = upload_mod._existing_raw_files_on_ia("ficha-2026-04")
    assert out == {"raw/Empresas0.zip", "raw/Empresas1.zip"}


@pytest.mark.parametrize(
    "files",
    [
        [{"name": "raw/Empresas0.zip", "size": True}],
        [{"name": "raw/Empresas0.zip", "size": 1.5}],
        [{"name": "raw/Empresas0.zip", "size": "１"}],
        [{"name": "raw/Empresas0.zip", "size": " 1 "}],
        [{"name": "raw/Empresas0.zip", "size": "-1"}],
        [{"name": "raw/Empresas0.zip"}],
        [
            {"name": "raw/Empresas0.zip", "size": "1"},
            {"name": "raw/Empresas0.zip", "size": "1"},
        ],
    ],
)
def test_stream_raw_rejects_ambiguous_catalog_before_get_or_put(monkeypatch, files):
    """A malformed IA inventory cannot authorize skipping or any remote write."""

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            return httpx.Response(200, json={"files": files})

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)

    def forbid_network(*args, **kwargs):
        pytest.fail("GET/PUT attempted before validating IA raw metadata")

    monkeypatch.setattr(upload_mod.upstream, "discover_token", forbid_network)
    monkeypatch.setattr(upload_mod, "_stream_one_zip_with_retry", forbid_network)

    with pytest.raises(RuntimeError, match="IA raw metadata"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")


def test_stream_raw_rejects_non_list_catalog_before_get_or_put(monkeypatch):
    """A 200 with a malformed catalog is not equivalent to a new IA item."""

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            return httpx.Response(200, json={"files": {"raw/Empresas0.zip": {"size": "1"}}})

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    monkeypatch.setattr(
        upload_mod.upstream,
        "discover_token",
        lambda: pytest.fail("upstream touched with malformed IA catalog"),
    )
    with pytest.raises(RuntimeError, match="IA raw metadata"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")


def test_stream_raw_rejects_http_200_invalid_json_before_get_or_put(monkeypatch):
    """HTTP 200 with broken JSON is not a transient metadata outage."""

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            return httpx.Response(200, content=b"<html>malformed JSON</html>")

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)

    def forbid_network(*args, **kwargs):
        pytest.fail("upstream GET or IA PUT attempted after invalid JSON")

    monkeypatch.setattr(upload_mod.upstream, "discover_token", forbid_network)
    monkeypatch.setattr(upload_mod, "_stream_one_zip_with_retry", forbid_network)

    with pytest.raises(RuntimeError, match="IA raw metadata: invalid JSON"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")


@pytest.mark.parametrize("status", [401, 403, 429, 500, 502, 503, 504])
def test_stream_raw_denies_metadata_errors_before_get_or_put(monkeypatch, status):
    """Denied or throttled metadata must not authorize RFB GET or IA PUT."""

    class _FakeResp:
        status_code = status

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            return _FakeResp()

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)

    def forbid_network(*args, **kwargs):
        pytest.fail("RFB GET or IA PUT after IA metadata access denial")

    monkeypatch.setattr(upload_mod.upstream, "discover_token", forbid_network)
    monkeypatch.setattr(upload_mod, "_stream_one_zip_with_retry", forbid_network)

    reason = (
        "access denied"
        if status in (401, 403)
        else ("rate limited" if status == 429 else "unavailable")
    )
    with pytest.raises(RuntimeError, match=f"{reason} HTTP {status}"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")


def test_existing_raw_files_on_ia_returns_empty_on_404(monkeypatch):
    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            return httpx.Response(404, json={})

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    assert upload_mod._existing_raw_files_on_ia("ficha-2099-01") == set()


@pytest.mark.parametrize(
    "error_type", [upload_mod.httpx.ConnectError, upload_mod.httpx.ReadTimeout]
)
def test_stream_raw_rejects_metadata_transport_failure(monkeypatch, error_type):
    """Transport failure gives no evidence that an IA item is absent."""

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, url):
            raise error_type("boom")

    def forbid(*args, **kwargs):
        pytest.fail("RFB GET or IA PUT after IA metadata transport failure")

    monkeypatch.setattr(upload_mod.httpx, "Client", _FakeClient)
    monkeypatch.setattr(upload_mod.upstream, "discover_token", forbid)
    monkeypatch.setattr(upload_mod, "_stream_one_zip_with_retry", forbid)
    with pytest.raises(RuntimeError, match="IA raw metadata: fetch failed"):
        upload_mod.stream_raw_zips_to_ia("2026-04", access_key="A", secret_key="S")


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
