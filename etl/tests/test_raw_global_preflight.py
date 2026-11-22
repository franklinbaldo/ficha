"""Pre-PUT 37/37 RFB raw admission: HTTP and S3 are strictly offline."""

from __future__ import annotations

import io
import struct
import zipfile
import zlib
from pathlib import Path

import httpx
import pytest

from ficha_etl import raw_preflight, upload
from ficha_etl.sources import canonical_inventory


def _equal(actual, expected) -> None:
    # Explicit failure remains live in python -O, unlike bare assert.
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def _source_zip(number: int) -> bytes:
    payload = f"DATA-{number:02d}".encode()
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(f"source-{number:02d}.csv", payload)
    return memory.getvalue()


def _offline(
    monkeypatch,
    *,
    bad_crc=False,
    status_37=None,
    bad_length=False,
    redirect_37=None,
    source_37=None,
    content_range_37=None,
    length_text_37=None,
):
    specs = canonical_inventory()
    _equal(len(specs), 37)
    streams = [_source_zip(i) for i in range(37)]
    if source_37 is not None:
        streams[-1] = source_37
    if bad_crc:
        source = bytearray(streams[-1])
        offset = source.index(b"DATA-36")
        source[offset] ^= 1  # same byte length and ZIP directory, invalid CRC
        streams[-1] = bytes(source)
    seen: dict[str, object] = {"gets": [], "puts": [], "received": []}
    original_client = httpx.Client
    names = {spec.name: i for i, spec in enumerate(specs)}

    def respond(request: httpx.Request):
        name = request.url.path.rsplit("/", 1)[-1]
        index = names[name]
        seen["gets"].append(name)
        if index == 36 and redirect_37 is not None and len(seen["gets"]) == 37:
            return httpx.Response(302, headers={"Location": redirect_37}, request=request)
        if index == 36 and status_37 is not None:
            return httpx.Response(status_37, request=request)
        source = streams[index]
        length = len(source) + (1 if bad_length and index == 36 else 0)
        headers = {"Content-Length": str(length)}
        if index == 36:
            if content_range_37 is not None:
                headers["Content-Range"] = content_range_37
            if length_text_37 is not None:
                headers["Content-Length"] = length_text_37
        return httpx.Response(
            200,
            headers=headers,
            stream=httpx.ByteStream(source),
            request=request,
        )

    transport = httpx.MockTransport(respond)

    def client_factory(**kwargs):
        return original_client(transport=transport, **kwargs)

    def put_spy(identifier, remote_name, chunks, *, content_length, **kwargs):
        data = b"".join(chunks)
        _equal(identifier, "ficha-2026-07")
        _equal(len(data), int(content_length))
        # A successful final count alone would not prove temporal ordering.
        _equal(len(seen["gets"]), 37)
        seen["puts"].append(remote_name)
        seen["received"].append(data)

    monkeypatch.setattr(upload.httpx, "Client", client_factory)
    monkeypatch.setattr(upload.upstream, "discover_token", lambda: "fake-token")
    monkeypatch.setattr(upload, "_existing_raw_files_on_ia", lambda _: set())
    monkeypatch.setattr(upload, "_ia_s3_put", put_spy)
    return seen, streams


def _run(tmp_path: Path):
    upload.stream_raw_zips_to_ia(
        "2026-07", access_key="fake", secret_key="fake", workers=1, spool_dir=tmp_path
    )


def test_preflight_admits_exact_37_before_any_put(monkeypatch, tmp_path):
    seen, sources = _offline(monkeypatch)
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [f"raw/{s.name}" for s in canonical_inventory()])
    _equal(seen["received"], sources)
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize(
    "case,reason",
    [("bad_crc", "CRC"), ("status_37", "404"), ("bad_length", "length mismatch")],
)
def test_final_zip_failure_cannot_partial_publish(monkeypatch, tmp_path, case, reason):
    options = {case: 404 if case == "status_37" else True}
    seen, _ = _offline(monkeypatch, **options)
    with pytest.raises((RuntimeError, httpx.HTTPStatusError), match=reason):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize(
    "origin",
    [
        "https://other.example",
        "http://arquivos.receitafederal.gov.br",
        "https://arquivos.receitafederal.gov.br",
    ],
)
def test_redirected_final_rfb_zip_is_not_admitted(monkeypatch, tmp_path, origin):
    # Last case covers even a same-origin HTTPS redirect. Every 3xx is denied.
    last_name = canonical_inventory()[-1].name
    target = f"{origin}/public.php/webdav/2026-07/{last_name}"
    seen, _ = _offline(monkeypatch, redirect_37=target)
    with pytest.raises(RuntimeError, match="redirected RFB source"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)  # The redirect target must not be requested.
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("content_range_37", "bytes 1-10/500", "unexpected RFB Content-Range"),
        ("length_text_37", "9" * 5000, "invalid RFB length"),
    ],
)
def test_final_zip_ambiguous_http_headers_cannot_publish(
    monkeypatch, tmp_path, field, value, reason
):
    # Mock HTTP 200 with abnormal source headers at ZIP 37.
    # Neither case should reach an S3 PUT; the length case must be contextual.
    seen, _ = _offline(monkeypatch, **{field: value})
    with pytest.raises(RuntimeError, match=reason):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def _oversized_source_37(
    compression: int, *, forge_header: bool, forge_compressed: bool = False
) -> bytes:
    """Synthetic malicious ZIP: 65536 expanded bytes but claims one byte."""
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=compression) as archive:
        archive.writestr("source-36.csv", b"A" * 65536)
    if not forge_header:
        return memory.getvalue()
    raw = bytearray(memory.getvalue())
    local = raw.index(b"PK\x03\x04")
    central = raw.index(b"PK\x01\x02")
    prefix_crc = zlib.crc32(b"A")
    struct.pack_into("<I", raw, local + 14, prefix_crc)
    struct.pack_into("<I", raw, local + 22, 1)
    struct.pack_into("<I", raw, central + 16, prefix_crc)
    struct.pack_into("<I", raw, central + 24, 1)
    if forge_compressed:
        struct.pack_into("<I", raw, local + 18, 1)
        struct.pack_into("<I", raw, central + 20, 1)
    return bytes(raw)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_underreported_final_zip_cannot_partial_publish(monkeypatch, tmp_path, method):
    forged = _oversized_source_37(method, forge_header=True)
    # The first 36 members are seven bytes each; allow just one more.
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 36 * 7 + 1)
    seen, _ = _offline(monkeypatch, source_37=forged)
    with pytest.raises(RuntimeError, match="global uncompressed limit"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_complete_final_zip_at_actual_expansion_limit(monkeypatch, tmp_path, method):
    valid = _oversized_source_37(method, forge_header=False)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 36 * 7 + 65536)
    seen, sources = _offline(monkeypatch, source_37=valid)
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(len(seen["puts"]), 37)
    _equal(seen["received"], sources)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_double_underreport_cannot_hide_unclaimed_zip_bytes(monkeypatch, tmp_path, method):
    forged = _oversized_source_37(method, forge_header=True, forge_compressed=True)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 36 * 7 + 1)
    seen, _ = _offline(monkeypatch, source_37=forged)
    with pytest.raises(RuntimeError, match="unclaimed ZIP payload bytes"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def test_deflate_data_descriptor_remains_valid(monkeypatch, tmp_path):
    # ZIP writers on unseekable streams use a signed data descriptor.
    class NonSeekable(io.BytesIO):
        def seekable(self):
            return False

        def seek(self, *args):
            raise OSError("unseekable")

    sink = NonSeekable()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("source-36.csv", b"DATA-36")
    seen, sources = _offline(monkeypatch, source_37=sink.getvalue())
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(len(seen["puts"]), 37)
    _equal(seen["received"], sources)


@pytest.mark.parametrize("method", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
@pytest.mark.parametrize(
    "local_offset",
    [14, 18, 22],
    ids=["local-crc", "local-compressed-size", "local-uncompressed-size"],
)
def test_final_zip_local_header_conflict_cannot_publish(
    monkeypatch, tmp_path, method, local_offset
):
    # Keep the central directory and payload intact. Only one local field
    # contradicts the authority used by zipfile.ZipInfo (Castor R133).
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=method) as archive:
        archive.writestr("source-36.csv", b"DATA-36")
    forged = bytearray(memory.getvalue())
    _equal(forged[:4], b"PK\x03\x04")
    _equal(struct.unpack_from("<H", forged, 6)[0] & 8, 0)
    struct.pack_into("<I", forged, local_offset, 0)
    seen, _ = _offline(monkeypatch, source_37=bytes(forged))
    with pytest.raises(RuntimeError, match="ZIP local CRC/size mismatch"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def test_valid_zip64_local_sizes_without_descriptor_remain_admissible(monkeypatch, tmp_path):
    # force_zip64 sets the local 32-bit sizes to 0xffffffff even for 7 bytes.
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        with archive.open("source-36.csv", "w", force_zip64=True) as writer:
            writer.write(b"DATA-36")
    source = memory.getvalue()
    _equal(struct.unpack_from("<I", source, 18)[0], 0xFFFFFFFF)
    _equal(struct.unpack_from("<I", source, 22)[0], 0xFFFFFFFF)
    seen, sources = _offline(monkeypatch, source_37=source)
    _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(len(seen["puts"]), 37)
    _equal(seen["received"], sources)


def test_duplicate_zip_bytes_under_two_source_names_cannot_publish(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch, source_37=_source_zip(0))
    with pytest.raises(RuntimeError, match="duplicate source SHA-256"):
        _run(tmp_path)
    _equal(len(seen["gets"]), 37)
    _equal(seen["puts"], [])
    _equal(list(tmp_path.iterdir()), [])


def test_expansion_cap_global_not_per_zip(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 35)
    with pytest.raises(RuntimeError, match="global uncompressed limit"):
        _run(tmp_path)
    _equal(seen["puts"], [])


def test_compressed_cap_global_not_per_zip(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_COMPRESSED_BYTES", 2 * len(_source_zip(0)))
    with pytest.raises(RuntimeError, match="global compressed limit"):
        _run(tmp_path)
    _equal(seen["puts"], [])


def test_canonical_duplicate_is_rejected_before_get(monkeypatch, tmp_path):
    seen, _ = _offline(monkeypatch)
    expected = canonical_inventory()
    monkeypatch.setattr(upload, "canonical_inventory", lambda: expected[:-1] + expected[:1])
    with pytest.raises(RuntimeError, match="canonical inventory"):
        _run(tmp_path)
    _equal(seen["gets"], [])
    _equal(seen["puts"], [])


def test_no_explicit_spool_denies_get_and_put(monkeypatch):
    seen, _ = _offline(monkeypatch)
    with pytest.raises(RuntimeError, match="explicit spool_dir"):
        upload.stream_raw_zips_to_ia("2026-07", access_key="x", secret_key="x")
    _equal(seen["gets"], [])
    _equal(seen["puts"], [])


def test_optimized_python_replays_zip37_local_guard():
    """Replay the six local-header negatives and positive controls with -O.

    A normal pytest run must not be mistaken for a test of optimized Python.
    Run the exact tests from this checkout in a child interpreter with -O,
    keeping the same fake HTTP transport and fake IA PUT assertions.
    """
    import subprocess
    import sys

    source = str(Path(__file__).resolve())
    selectors = [
        f"{source}::test_final_zip_local_header_conflict_cannot_publish",
        f"{source}::test_valid_zip64_local_sizes_without_descriptor_remain_admissible",
        f"{source}::test_deflate_data_descriptor_remains_valid",
        f"{source}::test_final_zip_ambiguous_http_headers_cannot_publish",
    ]
    replay = subprocess.run(
        [sys.executable, "-O", "-m", "pytest", "-q", *selectors],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    if replay.returncode != 0:
        raise AssertionError(replay.stdout + "\n" + replay.stderr)
