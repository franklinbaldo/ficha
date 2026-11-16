"""P0-B: independent, offline source-cardinality witness for the monthly gate.

Ten tiny source-part CSVs model the producer's input. Their counts and SHA-256
are measured from *source bytes*, independently of the produced Parquet and
its key projection. This is a test-only oracle, NOT a production gate. A match
of row counts does not prove identical keys, authentic RFB provenance, or
safe Internet Archive publication.

These are characterization tests: the existing uniqueness gate can accept
empty/truncated, otherwise unique output; the independent witness must not.
No RFB download, real ETL, network request or publication is performed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from ficha_etl.monthly_estabelecimento_gate import (
    MonthlyEstabelecimentoKeyGateError,
    run_monthly_gate,
)

ALPHA_CNPJ = "00000000E08G12"  # Official RFB example; check digits 12.
PARTS = tuple(range(10))


@dataclass(frozen=True)
class PartWitness:
    part: int
    path: Path
    sha256: str
    rows: int


def _check_digit(value: str, weights: tuple[int, ...]) -> str:
    remainder = sum((ord(char) - 48) * weight for char, weight in zip(
        value, weights, strict=True
    )) % 11
    return str(0 if remainder < 2 else 11 - remainder)


def _cnpj(base12: str) -> str:
    first = _check_digit(base12, (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    second = _check_digit(base12 + first, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))
    return base12 + first + second


def _source_parts(root: Path, *, duplicate_alpha: bool = False, empty: bool = False):
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    keys = []
    for part in PARTS:
        key = ALPHA_CNPJ if part == 0 or (duplicate_alpha and part == 1) else _cnpj(
            f"{part + 1:08d}0001"
        )
        part_keys = [] if empty else [key]
        path = root / f"Estabelecimentos{part}.csv"
        path.write_text("cnpj\n" + "".join(f"{k}\n" for k in part_keys), encoding="ascii")
        paths.append(path)
        keys.extend(part_keys)
    return paths, keys


def _capture_source_witnesses(paths: list[Path]) -> tuple[PartWitness, ...]:
    witnesses = []
    for part, path in enumerate(paths):
        raw = path.read_bytes()
        lines = raw.decode("ascii").splitlines()
        assert lines and lines[0] == "cnpj"
        witnesses.append(
            PartWitness(part, path, hashlib.sha256(raw).hexdigest(), len(lines) - 1)
        )
    return tuple(witnesses)


def _write_produced_parquet(path: Path, keys: list[str]) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE produced(cnpj VARCHAR)")
        if keys:
            con.executemany("INSERT INTO produced VALUES (?)", [(key,) for key in keys])
        con.execute("COPY produced TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])
    finally:
        con.close()


def _reconcile_with_independent_source(
    witnesses: tuple[PartWitness, ...], produced: Path
) -> int:
    """Check 10 distinct source receipts, immutable source bytes, and row counts."""
    if len(witnesses) != len(PARTS) or {w.part for w in witnesses} != set(PARTS):
        raise ValueError("missing or repeated source part receipt")
    for witness in witnesses:
        if hashlib.sha256(witness.path.read_bytes()).hexdigest() != witness.sha256:
            raise ValueError(f"source part {witness.part} changed after receipt")
    expected = sum(w.rows for w in witnesses)
    if expected == 0:
        raise ValueError("zero source rows cannot prove a complete monthly snapshot")
    con = duckdb.connect()
    try:
        actual = int(
            con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(produced)]).fetchone()[0]
        )
    finally:
        con.close()
    if actual != expected:
        raise ValueError(f"source/produced row mismatch: {expected} != {actual}")
    return expected


def _gate(root: Path, keys: list[str]) -> tuple[dict, Path, Path]:
    produced = root / "cnpjs.parquet"
    evidence = root / "evidence.json"
    _write_produced_parquet(produced, keys)
    payload = run_monthly_gate("2026-08", produced, evidence, root / "work")
    return payload, produced, evidence


def test_valid_alphanumeric_cnpj_and_ten_source_receipts(tmp_path: Path) -> None:
    paths, keys = _source_parts(tmp_path / "origin")
    witnesses = _capture_source_witnesses(paths)
    assert _cnpj("00000000E08G") == ALPHA_CNPJ
    payload, produced, _ = _gate(tmp_path, keys)

    assert payload["status"] == "ok"
    assert payload["invalid_cnpj_rows"] == 0
    assert payload["audit"]["distinct_valid_full_keys"] == len(PARTS)
    assert _reconcile_with_independent_source(witnesses, produced) == len(PARTS)


def test_alphanumeric_duplicate_across_distinct_parts_fails_gate(tmp_path: Path) -> None:
    paths, keys = _source_parts(tmp_path / "origin", duplicate_alpha=True)
    witnesses = _capture_source_witnesses(paths)
    produced = tmp_path / "cnpjs.parquet"
    evidence = tmp_path / "evidence.json"
    _write_produced_parquet(produced, keys)

    # Count completeness alone cannot establish key uniqueness.
    assert _reconcile_with_independent_source(witnesses, produced) == len(PARTS)
    with pytest.raises(MonthlyEstabelecimentoKeyGateError, match="duplicate full"):
        run_monthly_gate("2026-08", produced, evidence, tmp_path / "work")

    report = json.loads(evidence.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["audit"]["duplicate_key_count"] == 1
    assert report["audit"]["excess_duplicate_row_count"] == 1
    # The current adapter labels every row "cnpjs.parquet": lineage is lost.
    assert report["audit"]["cross_part_duplicate_key_count"] == 0


def test_zero_chunks_yield_empty_parquet_but_not_source_completeness(tmp_path: Path) -> None:
    paths, _ = _source_parts(tmp_path / "origin")
    witnesses = _capture_source_witnesses(paths)
    payload, produced, _ = _gate(tmp_path, [])

    # Characterize the existing uniqueness-only gate; this is NOT an E2E pass.
    assert payload["status"] == "ok"
    assert payload["source_rows"] == 0
    with pytest.raises(ValueError, match="source/produced row mismatch: 10 != 0"):
        _reconcile_with_independent_source(witnesses, produced)


def test_truncated_unique_output_is_not_a_complete_snapshot(tmp_path: Path) -> None:
    paths, keys = _source_parts(tmp_path / "origin")
    witnesses = _capture_source_witnesses(paths)
    payload, produced, _ = _gate(tmp_path, keys[:-1])

    assert payload["status"] == "ok"  # Uniqueness does not imply completeness.
    with pytest.raises(ValueError, match="source/produced row mismatch: 10 != 9"):
        _reconcile_with_independent_source(witnesses, produced)


def test_missing_part_and_tampered_receipt_fail_closed(tmp_path: Path) -> None:
    paths, keys = _source_parts(tmp_path / "origin")
    witnesses = _capture_source_witnesses(paths)
    produced = tmp_path / "cnpjs.parquet"
    _write_produced_parquet(produced, keys)

    with pytest.raises(ValueError, match="missing or repeated source part"):
        _reconcile_with_independent_source(witnesses[:-1], produced)

    paths[3].write_text("cnpj\n", encoding="ascii")
    with pytest.raises(ValueError, match="source part 3 changed"):
        _reconcile_with_independent_source(witnesses, produced)


def test_zero_source_rows_do_not_vacuously_reconcile(tmp_path: Path) -> None:
    paths, _ = _source_parts(tmp_path / "origin", empty=True)
    witnesses = _capture_source_witnesses(paths)
    produced = tmp_path / "cnpjs.parquet"
    _write_produced_parquet(produced, [])

    with pytest.raises(ValueError, match="zero source rows"):
        _reconcile_with_independent_source(witnesses, produced)
