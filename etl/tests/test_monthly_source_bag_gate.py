"""Offline adversarial controls of the monthly parsed-source bag gate."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from ficha_etl.monthly_source_bag_gate import (
    MonthlySourceBagError,
    reconcile_loaded_source_keys,
)


def _parts(root: Path) -> list[tuple[str, Path]]:
    out = []
    for i in range(10):
        name = f"Estabelecimentos{i}.zip"
        path = root / name / "source.csv"
        path.parent.mkdir(parents=True)
        path.write_text(f"{i}\n", encoding="utf-8")
        out.append((name, path))
    return out


def _tables(
    con: duckdb.DuckDBPyConnection, source: list[str | None], output: list[str | None], path: Path
) -> None:
    con.execute(
        "CREATE TABLE estabelecimento(cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR)"
    )
    if source:
        con.executemany(
            "INSERT INTO estabelecimento VALUES (?, ?, ?)",
            [
                (key[:8], key[8:12], key[12:14]) if key is not None else (None, None, None)
                for key in source
            ],
        )
    con.execute("CREATE TABLE produced(cnpj VARCHAR)")
    if output:
        con.executemany("INSERT INTO produced VALUES (?)", [(key,) for key in output])
    con.execute("COPY produced TO ? (FORMAT PARQUET)", [str(path)])


def _run(
    tmp_path: Path,
    source: list[str | None],
    output: list[str | None],
    *,
    parts: list[tuple[str, Path]] | None = None,
) -> dict:
    con = duckdb.connect()
    try:
        _tables(con, source, output, tmp_path / "output.parquet")
        return reconcile_loaded_source_keys(
            con,
            "2026-08",
            _parts(tmp_path / "sources") if parts is None else parts,
            tmp_path / "output.parquet",
            tmp_path / "evidence" / "receipt.json",
        )
    finally:
        con.close()


def test_equal_bag_accepts_permutations_and_alphanumeric_keys(tmp_path: Path) -> None:
    values = ["00000000E08G12", "12345678000195", "12345678000276"]
    receipt = _run(tmp_path, values, values[::-1])
    assert receipt["status"] == "ok"
    assert receipt["parsed_source_rows"] == receipt["produced_rows"] == 3
    assert len(receipt["parts"]) == 10
    assert receipt["identity_level"] == "parsed-duckdb-source-not-zip-custody"


@pytest.mark.parametrize(
    ("source", "output", "reason"),
    [
        (["12345678000195"], [], "cardinality"),
        (["12345678000195", "12345678000276"], ["12345678000195", "87654321000198"], "multiset"),
        (
            ["12345678000195", "12345678000195", "12345678000276"],
            ["12345678000195", "12345678000276", "12345678000276"],
            "multiset",
        ),
        (["12345678000195", "12345678000276"], ["12345678000195", "12345678000195"], "multiset"),
        ([], [], "cardinality"),
    ],
)
def test_fail_closed_for_loss_swap_and_multiplicity(
    tmp_path: Path,
    source: list[str],
    output: list[str],
    reason: str,
) -> None:
    with pytest.raises(MonthlySourceBagError, match=reason):
        _run(tmp_path, source, output)
    receipt = json.loads((tmp_path / "evidence" / "receipt.json").read_text())
    assert receipt["status"] == "failed"
    if reason == "multiset":
        assert receipt["parsed_source_rows"] == receipt["produced_rows"]
        assert receipt["source_only_key_exists"] is True
        assert receipt["output_only_key_exists"] is True


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "empty", "aliased", "wrong_parent"])
def test_fail_closed_for_missing_or_bad_part_inventory(tmp_path: Path, mutation: str) -> None:
    parts = _parts(tmp_path / "sources")
    if mutation == "missing":
        parts.pop()
    elif mutation == "duplicate":
        parts[9] = parts[0]
    elif mutation == "empty":
        parts[9][1].write_bytes(b"")
    elif mutation == "aliased":
        parts[9] = (parts[9][0], parts[0][1])
    else:
        parts[9] = (parts[9][0], tmp_path / "sources" / parts[0][0] / "source.csv")
    with pytest.raises(MonthlySourceBagError, match="noncanonical source part"):
        _run(tmp_path, ["12345678000195"], ["12345678000195"], parts=parts)
    receipt = json.loads((tmp_path / "evidence" / "receipt.json").read_text())
    assert receipt["status"] == "failed"


def test_preserves_null_multiplicity_without_claiming_key_validity(tmp_path: Path) -> None:
    receipt = _run(tmp_path, [None, "12345678000195"], ["12345678000195", None])
    assert receipt["status"] == "ok"
    # The independent monthly CNPJ/DV gate rejects nulls after this comparison.


def test_malformed_keys_are_not_falsely_declared_valid(tmp_path: Path) -> None:
    receipt = _run(tmp_path, ["bad-key"], ["bad-key"])
    assert receipt["status"] == "ok"
    assert receipt["identity_level"] == "parsed-duckdb-source-not-zip-custody"
    # This is only equality, NOT validity; the separate key gate still runs.


def test_same_duckdb_csv_dialect_as_writer_with_ten_real_reads(tmp_path: Path) -> None:
    """Witness must match lenient DuckDB rows, not a stricter Python CSV parser."""
    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    for i, (_, path) in enumerate(parts):
        fields = [""] * 30
        fields[0] = "12345678"
        fields[1] = f"{i + 1:04d}"
        fields[2] = "95"
        fields[14] = 'RUA"x' if i == 0 else "RUA"
        if i == 1:
            fields = fields[:20]  # padded to 30 by the production reader
        path.write_text(";".join(fields) + "\n", encoding="utf-8")

    con = duckdb.connect()
    try:
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
        )
        assert con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0] == 10
        target = tmp_path / "result.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(target)],
        )
        receipt = reconcile_loaded_source_keys(
            con, "2026-08", parts, target, tmp_path / "evidence.json"
        )
        assert receipt["status"] == "ok"
        assert receipt["parsed_source_rows"] == 10
    finally:
        con.close()
