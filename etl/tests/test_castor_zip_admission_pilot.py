"""Offline A/B pilot only: do not select B from any production entrypoint."""

import hashlib
import json
import tempfile
import zipfile
from contextlib import ExitStack, closing
from pathlib import Path

import duckdb
import pytest

from ficha_etl import canonical_history, fetcher, raw_preflight, transform

from .test_canonical_history import _row, _write_zip
from .test_transform import (
    ESTABELECIMENTO_ROWS,
    _build_full_fixture_zips,
    _ZipDirFetcher,
    _zip_with_csv,
)


def _write_exact(handle, chunk):
    if handle.write(chunk) != len(chunk):
        raise OSError("short write in provisional ZIP sink")


def _extract_b(zip_path, dest_dir, *, open_sink=lambda path: path.open("xb")):
    """Pilot for flat, non-aliased fixture names, not a new name policy."""
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=dest_dir.parent) as temporary:
        staging = Path(temporary) / "admitted"
        staging.mkdir()
        names = []
        with ExitStack() as sinks, zipfile.ZipFile(zip_path) as archive:

            def sink_for(entry):
                # Close the previous member before opening the next one.
                # closing() invokes close(), never a suppressing __exit__.
                sinks.close()
                if entry.is_dir():
                    return None
                name = entry.filename
                if (
                    name != entry.orig_filename
                    or name in ("", ".", "..")
                    or any(char in name for char in ("/", "\\", ":", "\x00"))
                ):
                    raise ValueError("name outside pilot fixture domain")
                names.append(name)
                handle = sinks.enter_context(closing(open_sink(staging / name)))
                return lambda chunk: _write_exact(handle, chunk)

            raw_preflight._check_zip_members(archive, expanded=0, members=0, sink_for=sink_for)
        # No sink or archive handle remains open; close failure cannot publish.
        staging.rename(dest_dir)
        return [dest_dir / name for name in names]


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parquet_bag_equal(con, a, b):
    schema_a = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(a)]).fetchall()
    schema_b = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(b)]).fetchall()
    assert schema_a == schema_b
    for left, right in ((a, b), (b, a)):
        assert (
            con.execute(
                "SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM read_parquet(?)",
                [str(left), str(right)],
            ).fetchall()
            == []
        )


def test_monthly_real_transform_and_source_bag(tmp_path, monkeypatch):
    source = tmp_path / "zips"
    _build_full_fixture_zips(source)
    # Keep canonical parent names and distinct rows; exercise #116, including
    # identical internal filenames across independent ZIPs.
    for part in range(1, 10):
        row = list(ESTABELECIMENTO_ROWS[0])
        row[1] = f"{1000 + part:04}"
        row[15] = str(100 + part)
        _zip_with_csv(source / f"Estabelecimentos{part}.zip", "shared.csv", [tuple(row)])
    # Freeze identical bytes before both arms. Re-encode with fixed timestamp
    # and STORED to make fixture hashes independent of runtime zlib version.
    for path in sorted(source.glob("*.zip")):
        with zipfile.ZipFile(path) as archive:
            entries = [(entry.filename, archive.read(entry)) for entry in archive.infolist()]
        with zipfile.ZipFile(path, "w") as archive:
            for name, data in entries:
                info = zipfile.ZipInfo(name)
                info.create_system = 3
                archive.writestr(info, data)
    frozen = {p.name: _sha(p) for p in source.glob("*.zip")}
    assert len(frozen) == 37
    print("monthly fixture SHA256:", json.dumps(frozen, sort_keys=True))

    extract_a = transform.extract_zip
    for arm, extract in (("a", extract_a), ("b", _extract_b)):
        calls = []

        def traced(zip_path, dest_dir):
            calls.append(Path(zip_path).name)
            return extract(zip_path, dest_dir)

        monkeypatch.setattr(transform, "extract_zip", traced)
        root = tmp_path / arm
        transform.transform_snapshot(
            "2026-04",
            cache_dir=root / "cache",
            output_dir=root / "output",
            chain=fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(source)]),
            skip_unimplemented=False,
            reject_empty_estabelecimentos=True,
        )
        assert {p.name: _sha(p) for p in source.glob("*.zip")} == frozen
        assert sorted(calls) == sorted(frozen)

    a, b = (tmp_path / arm / "output" for arm in ("a", "b"))
    files = sorted(p.relative_to(a) for p in a.rglob("*.parquet"))
    required = {
        Path(name)
        for name in (
            "cnpjs.parquet",
            "cnpj_cnaes.parquet",
            "raizes.parquet",
            "socios.parquet",
            "cnpj_contatos.parquet",
            "enderecos.parquet",
        )
    }
    required.update(Path("lookups") / f"{kind}.parquet" for kind in transform._LOOKUP_KINDS)
    assert required <= set(files)
    assert files == sorted(p.relative_to(b) for p in b.rglob("*.parquet"))
    with duckdb.connect() as con:
        for relative in files:
            _parquet_bag_equal(con, a / relative, b / relative)
        for root in (a, b):
            rows = con.execute(
                "SELECT cnpj, numero FROM read_parquet(?)", [str(root / "enderecos.parquet")]
            ).fetchall()
            expected = {
                (
                    ESTABELECIMENTO_ROWS[0][0] + f"{1000 + part:04}" + ESTABELECIMENTO_ROWS[0][2],
                    str(100 + part),
                )
                for part in range(1, 10)
            }
            assert expected <= set(rows)
    assert json.loads((a / "lookups.json").read_text()) == json.loads(
        (b / "lookups.json").read_text()
    )
    receipts = [
        json.loads(
            (tmp_path / arm / "evidence" / "monthly-estabelecimento-source-bag.json").read_text()
        )
        for arm in ("a", "b")
    ]
    assert receipts[0] == receipts[1]
    assert receipts[0]["status"] == "ok"
    assert len(receipts[0]["parts"]) == 10


def test_historical_real_transform_fresh_roots(tmp_path, monkeypatch):
    source = tmp_path / "fixture.zip"
    _write_zip(source, [_row(nome_fantasia="Linha 1\nLinha 2")])
    with zipfile.ZipFile(source) as archive:
        entry = archive.infolist()[0]
        name = entry.filename
        payload = archive.read(entry)
    info = zipfile.ZipInfo(name)
    info.create_system = 3
    info.compress_type = zipfile.ZIP_DEFLATED
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(info, payload)
    frozen = _sha(source)
    print("historical fixture SHA256:", frozen)
    outputs = []
    for arm in ("a", "b"):
        if arm == "b":
            monkeypatch.setattr(transform, "extract_zip", _extract_b)
        result = canonical_history.run_historical_shadow(
            "2026-04", 0, tmp_path / arm, zip_override=source, sample_size=10
        )
        assert result.reused is False
        assert result.manifest["source"]["zip"]["sha256"] == frozen
        assert result.manifest["source"]["csv"]["sha256"] == hashlib.sha256(payload).hexdigest()
        assert result.manifest["source"]["csv"]["bytes"] == len(payload)
        assert _sha(source) == frozen
        outputs.append(result.output_path)
    with duckdb.connect() as con:
        _parquet_bag_equal(con, *outputs)
        for output in outputs:
            rows = con.execute(
                "SELECT cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, "
                "_source_file, _source_snapshot FROM read_parquet(?)",
                [str(output)],
            ).fetchall()
            expected = (
                "00000001",
                "0001",
                "91",
                "Linha 1\nLinha 2",
                "Estabelecimentos0.zip",
                "2026-04",
            )
            assert rows == [expected]


def test_late_crc_leaves_no_admitted_paths(tmp_path):
    source = tmp_path / "fixture.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("first.csv", b"first")
        archive.writestr("second.csv", b"x" * (2 * 1024 * 1024))
    with zipfile.ZipFile(source) as archive:
        member = archive.infolist()[1]
    data = bytearray(source.read_bytes())
    data[member.header_offset + 30 + len(member.filename) + member.file_size - 1] ^= 1
    source.write_bytes(data)
    with pytest.raises(RuntimeError, match="ZIP CRC failed"):
        _extract_b(source, tmp_path / "b")
    assert sorted(p.name for p in tmp_path.iterdir()) == [source.name]


@pytest.mark.parametrize("failure", ["short", "close"])
def test_operational_sink_failure_is_not_admitted(tmp_path, failure):
    source = tmp_path / "fixture.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("data.csv", b"123")

    class BrokenSink:
        def write(self, chunk):
            return len(chunk) - (1 if failure == "short" else 0)

        def close(self):
            if failure == "close":
                raise OSError("close failed")

    with pytest.raises(OSError, match="short write|close failed"):
        _extract_b(source, tmp_path / "b", open_sink=lambda path: BrokenSink())
    assert sorted(p.name for p in tmp_path.iterdir()) == [source.name]


def test_member_sinks_close_before_next_and_never_suppress_crc(tmp_path):
    source = tmp_path / "fixture.zip"
    with zipfile.ZipFile(source, "w") as archive:
        for i in range(5):
            archive.writestr(f"{i}.csv", bytes([i]) * 100)
    active = 0
    peak = 0

    class Sink:
        def __init__(self, path):
            nonlocal active, peak
            self.handle = path.open("xb")
            active += 1
            peak = max(peak, active)

        def write(self, chunk):
            return self.handle.write(chunk)

        def close(self):
            nonlocal active
            self.handle.close()
            active -= 1

        def __exit__(self, exc_type, exc, tb):
            return True

    paths = _extract_b(source, tmp_path / "valid", open_sink=Sink)
    assert [p.read_bytes() for p in paths] == [bytes([i]) * 100 for i in range(5)]
    assert (active, peak) == (0, 1)
    with zipfile.ZipFile(source) as archive:
        member = archive.infolist()[-1]
    data = bytearray(source.read_bytes())
    data[member.header_offset + 30 + len(member.filename)] ^= 1
    source.write_bytes(data)
    with pytest.raises(RuntimeError, match="ZIP CRC failed"):
        _extract_b(source, tmp_path / "invalid", open_sink=Sink)
    assert (active, peak) == (0, 1)
    assert not (tmp_path / "invalid").exists()
