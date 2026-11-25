"""Offline A/B pilot only: do not select B from any production entrypoint."""

import hashlib
import json
import tempfile
import zipfile
from contextlib import ExitStack, closing
from dataclasses import dataclass
from functools import partial
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


@dataclass
class _PilotBudget:
    expanded: int = 0
    members: int = 0


def _extract_b(zip_path, dest_dir, *, open_sink=lambda path: path.open("xb"), budget=None):
    """Pilot for flat, non-aliased fixture names, not a new name policy."""
    if budget is None:
        budget = _PilotBudget()
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

            totals = raw_preflight._check_zip_members(
                archive, expanded=budget.expanded, members=budget.members, sink_for=sink_for
            )
        # No sink or archive handle remains open; close failure cannot publish.
        staging.rename(dest_dir)
        budget.expanded, budget.members = totals
        return [dest_dir / name for name in names]


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _witness_extract(extract, expected, calls):
    def traced(zip_path, dest_dir):
        name = Path(zip_path).name
        calls.append(name)
        zip_sha, members = expected[name]
        assert _sha(zip_path) == zip_sha
        paths = extract(zip_path, dest_dir)
        assert paths == [dest_dir / name for name, _, _ in members]
        actual = [(p.name, p.stat().st_size, _sha(p)) for p in paths]
        assert actual == members
        return paths

    return traced


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
    expected = {}
    for path in sorted(source.glob("*.zip")):
        with zipfile.ZipFile(path) as archive:
            entries = [(entry.filename, archive.read(entry)) for entry in archive.infolist()]
        with zipfile.ZipFile(path, "w") as archive:
            for name, data in entries:
                info = zipfile.ZipInfo(name)
                info.create_system = 3
                archive.writestr(info, data)
        members = [(name, len(data), hashlib.sha256(data).hexdigest()) for name, data in entries]
        expected[path.name] = (_sha(path), members)
    frozen = {name: zip_sha for name, (zip_sha, _) in expected.items()}
    assert len(frozen) == 37
    print("monthly fixture SHA256:", json.dumps(frozen, sort_keys=True))

    def run(root):
        transform.transform_snapshot(
            "2026-04",
            cache_dir=root / "cache",
            output_dir=root / "output",
            chain=fetcher.ChainedFetcher(fetchers=[_ZipDirFetcher(source)]),
            skip_unimplemented=False,
            reject_empty_estabelecimentos=True,
        )

    def tree_sha(root):
        return {p.relative_to(root): _sha(p) for p in root.rglob("*") if p.is_file()}

    extract_a = transform.extract_zip
    calls_a = []
    monkeypatch.setattr(transform, "extract_zip", _witness_extract(extract_a, expected, calls_a))
    a_root = tmp_path / "a"
    run(a_root)
    assert sorted(calls_a) == sorted(frozen)
    assert {p.name: _sha(p) for p in source.glob("*.zip")} == frozen
    a_frozen = tree_sha(a_root)

    # The limit is reached across ZIPs, not inside the first ZIP. Keep this
    # failed attempt private and retry in a fresh root with a fresh budget.
    failed_root = tmp_path / "b-failed"
    failed_budget = _PilotBudget()
    failed_calls = []

    def forbidden_connect(*args, **kwargs):
        pytest.fail("DuckDB must not be opened after a failed admission")

    with monkeypatch.context() as scoped:
        scoped.setattr(raw_preflight, "_MAX_RAW_MEMBERS", 1)
        scoped.setattr(duckdb, "connect", forbidden_connect)
        scoped.setattr(
            transform,
            "extract_zip",
            _witness_extract(partial(_extract_b, budget=failed_budget), expected, failed_calls),
        )
        with pytest.raises(RuntimeError, match="invalid ZIP member count"):
            run(failed_root)
    assert len(failed_calls) == 2
    assert failed_budget.members == 1
    assert not (failed_root / "output").exists()
    assert not (failed_root / "evidence" / "monthly-estabelecimento-source-bag.json").exists()
    assert (failed_root / "cache" / "2026-04" / "metrics" / "transform_metrics.json").is_file()
    assert tree_sha(a_root) == a_frozen
    failed_frozen = tree_sha(failed_root)

    budget_b = _PilotBudget()
    calls_b = []
    monkeypatch.setattr(
        transform,
        "extract_zip",
        _witness_extract(partial(_extract_b, budget=budget_b), expected, calls_b),
    )
    run(tmp_path / "b")
    assert sorted(calls_b) == sorted(frozen)
    assert budget_b.members == 37
    assert budget_b.expanded == sum(
        size for _, entries in expected.values() for _, size, _ in entries
    )
    assert {p.name: _sha(p) for p in source.glob("*.zip")} == frozen
    assert tree_sha(a_root) == a_frozen
    assert tree_sha(failed_root) == failed_frozen

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
            expected_rows = [("".join(row[:3]), row[15]) for row in ESTABELECIMENTO_ROWS]
            expected_rows.extend(
                (
                    ESTABELECIMENTO_ROWS[0][0] + f"{1000 + part:04}" + ESTABELECIMENTO_ROWS[0][2],
                    str(100 + part),
                )
                for part in range(1, 10)
            )
            assert sorted(rows) == sorted(expected_rows)
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
    members = [(name, len(payload), hashlib.sha256(payload).hexdigest())]
    expected = {"Estabelecimentos0.zip": (frozen, members)}
    extract_a = transform.extract_zip
    for arm, extract in (("a", extract_a), ("b", _extract_b)):
        calls = []
        monkeypatch.setattr(transform, "extract_zip", _witness_extract(extract, expected, calls))
        result = canonical_history.run_historical_shadow(
            "2026-04", 0, tmp_path / arm, zip_override=source, sample_size=10
        )
        assert calls == ["Estabelecimentos0.zip"]
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


@pytest.mark.parametrize("with_sink", [False, True])
def test_physical_budget_accumulates_across_archives(tmp_path, monkeypatch, with_sink):
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    for path, data in ((first, b"abc"), (second, b"defg")):
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("data.csv", data)

    def check(path, expanded, members):
        if not with_sink:
            return raw_preflight._raw_zip_crc_and_budget(path, expanded=expanded, members=members)
        with zipfile.ZipFile(path) as archive:
            return raw_preflight._check_zip_members(
                archive, expanded=expanded, members=members, sink_for=lambda _: lambda _: None
            )

    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 7)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_MEMBERS", 2)
    first_totals = check(first, 0, 0)
    assert first_totals == (3, 1)
    assert check(second, *first_totals) == (7, 2)

    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 6)
    with pytest.raises(RuntimeError, match="global uncompressed limit"):
        check(second, *first_totals)

    monkeypatch.setattr(raw_preflight, "_MAX_RAW_UNCOMPRESSED_BYTES", 7)
    monkeypatch.setattr(raw_preflight, "_MAX_RAW_MEMBERS", 1)
    with pytest.raises(RuntimeError, match="invalid ZIP member count"):
        check(second, *first_totals)
