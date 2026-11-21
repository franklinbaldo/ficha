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
        # All ten sources contribute one independent baseline key. Mutations
        # exercise the multiset without constructing impossible 1-of-10 cases.
        baseline = [f"{i:08d}000195" for i in range(10)] if source or output else []
        source_parts = _parts(tmp_path / "sources") if parts is None else parts
        counts = {path.resolve(): 1 if baseline else 0 for _, path in source_parts}
        if baseline:
            first = source_parts[0][1].resolve()
            counts[first] += len(source)
        _tables(con, baseline + source, baseline + output, tmp_path / "output.parquet")
        return reconcile_loaded_source_keys(
            con,
            "2026-08",
            source_parts,
            tmp_path / "output.parquet",
            tmp_path / "evidence" / "receipt.json",
            counts,
        )
    finally:
        con.close()


def test_equal_bag_accepts_permutations_and_alphanumeric_keys(tmp_path: Path) -> None:
    values = ["00000000E08G12", "12345678000195", "12345678000276"]
    receipt = _run(tmp_path, values, values[::-1])
    assert receipt["status"] == "ok"
    assert receipt["parsed_source_rows"] == receipt["produced_rows"] == 13
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
        ([], [], "zero logical rows"),
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
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0] == 10
        target = tmp_path / "result.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(target)],
        )
        receipt = reconcile_loaded_source_keys(
            con, "2026-08", parts, target, tmp_path / "evidence.json", counts
        )
        assert receipt["status"] == "ok"
        assert receipt["parsed_source_rows"] == 10
    finally:
        con.close()


def test_nonempty_part_with_zero_logical_rows_fails_with_receipt(tmp_path: Path) -> None:
    """A nonempty CSV can still contain zero logical records; never certify it."""
    parts = _parts(tmp_path / "sources")
    parts[9][1].write_bytes(b"\n")
    expected = [f"{i:08d}000195" for i in range(9)]
    con = duckdb.connect()
    try:
        _tables(con, expected, expected, tmp_path / "output.parquet")
        parsed_counts = {path.resolve(): 1 for _, path in parts}
        parsed_counts[parts[9][1].resolve()] = 0
        with pytest.raises(MonthlySourceBagError, match="zero logical rows"):
            reconcile_loaded_source_keys(
                con,
                "2026-08",
                parts,
                tmp_path / "output.parquet",
                tmp_path / "evidence" / "receipt.json",
                parsed_counts,
            )
        receipt = json.loads((tmp_path / "evidence" / "receipt.json").read_text())
        assert receipt["status"] == "failed"
    finally:
        con.close()


def test_per_part_counts_must_match_same_loaded_table(tmp_path: Path) -> None:
    parts = _parts(tmp_path / "sources")
    expected = [f"{i:08d}000195" for i in range(10)]
    con = duckdb.connect()
    try:
        _tables(con, expected, expected, tmp_path / "output.parquet")
        parsed_counts = {path.resolve(): 1 for _, path in parts}
        parsed_counts[parts[0][1].resolve()] = 2
        with pytest.raises(MonthlySourceBagError, match="row totals"):
            reconcile_loaded_source_keys(
                con,
                "2026-08",
                parts,
                tmp_path / "output.parquet",
                tmp_path / "evidence" / "receipt.json",
                parsed_counts,
            )
    finally:
        con.close()


def test_real_duckdb_loader_produces_per_part_row_counts(tmp_path: Path) -> None:
    """Do not infer row counts from ZIP size, newline count or global totals."""
    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    for i, (_, path) in enumerate(parts):
        columns = [""] * 30
        columns[0] = f"{i:08d}"
        columns[1] = "0001"
        columns[2] = "95"
        path.write_text(";".join(columns) + "\n", encoding="utf-8")

    con = duckdb.connect()
    try:
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [p for _, p in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert len(counts) == 10
        assert all(count == 1 for count in counts.values())
        assert (
            sum(counts.values())
            == con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
        )
    finally:
        con.close()


def test_real_duckdb_blank_line_part_blocks_equal_nine_part_output(tmp_path: Path) -> None:
    """One CSV has bytes but no logical record; the 9/9 multiset must not pass."""
    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    for i, (_, path) in enumerate(parts):
        fields = [""] * 30
        fields[0] = f"{i:08d}"
        fields[1] = "0001"
        fields[2] = "95"
        path.write_text(";".join(fields) + "\n", encoding="utf-8")
    parts[9][1].write_bytes(b"\n")

    con = duckdb.connect()
    try:
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert len(counts) == 10
        assert counts[parts[9][1].resolve()] == 0
        assert con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0] == 9

        target = tmp_path / "equal_nine_parts.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(target)],
        )
        with pytest.raises(MonthlySourceBagError, match="zero logical rows"):
            reconcile_loaded_source_keys(
                con, "2026-08", parts, target, tmp_path / "evidence.json", counts
            )
        receipt = json.loads((tmp_path / "evidence.json").read_text())
        assert receipt["status"] == "failed"
    finally:
        con.close()


def test_quoted_newline_counts_one_logical_record_per_source_part(tmp_path: Path) -> None:
    """The writer and witness must agree on a quoted physical newline."""
    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    for i, (_, path) in enumerate(parts):
        fields = [""] * 30
        fields[0] = f"{i:08d}"
        fields[1] = "0001"
        fields[2] = "95"
        fields[14] = '"AVENIDA\nCENTRAL"' if i == 4 else "RUA"
        path.write_bytes((";".join(fields) + "\n").encode("utf-8"))
    # The fifth CSV has two physical lines but one DuckDB logical row.
    assert parts[4][1].read_bytes().count(b"\n") == 2

    con = duckdb.connect()
    try:
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert len(counts) == 10
        assert set(counts.values()) == {1}
        row = con.execute("SELECT * FROM estabelecimento WHERE cnpj_basico = '00000004'").fetchone()
        assert row is not None
        assert row[14] == "AVENIDA\nCENTRAL"

        target = tmp_path / "result.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(target)],
        )
        receipt = reconcile_loaded_source_keys(
            con, "2026-08", parts, target, tmp_path / "receipt.json", counts
        )
        assert receipt["status"] == "ok"
        assert receipt["parsed_source_rows"] == 10
        assert all(part["parsed_csv_rows"] == 1 for part in receipt["parts"])
    finally:
        con.close()


def test_crlf_only_part_is_not_a_logical_record(tmp_path: Path) -> None:
    """A physically nonempty CRLF-only tenth CSV cannot certify ten parts."""
    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    for i, (_, path) in enumerate(parts):
        fields = [""] * 30
        fields[0] = f"{i:08d}"
        fields[1] = "0001"
        fields[2] = "95"
        path.write_text(";".join(fields) + "\n", encoding="utf-8")
    parts[9][1].write_bytes(b"\r\n")

    con = duckdb.connect()
    try:
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert counts[parts[9][1].resolve()] == 0
        assert con.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0] == 9
        target = tmp_path / "nine.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(target)],
        )
        with pytest.raises(MonthlySourceBagError, match="zero logical rows"):
            reconcile_loaded_source_keys(
                con, "2026-08", parts, target, tmp_path / "receipt.json", counts
            )
        assert json.loads((tmp_path / "receipt.json").read_text())["status"] == "failed"
    finally:
        con.close()


@pytest.mark.parametrize("bad_input", ["missing", "not_parquet"])
def test_duckdb_error_replaces_prior_success_receipt(tmp_path: Path, bad_input: str) -> None:
    """A failed second attempt must not leave a previous status=ok artifact."""
    parts = _parts(tmp_path / "sources")
    expected = [f"{i:08d}000195" for i in range(10)]
    con = duckdb.connect()
    try:
        good = tmp_path / "ok.parquet"
        _tables(con, expected, expected, good)
        counts = {path.resolve(): 1 for _, path in parts}
        receipt_path = tmp_path / "evidence" / "receipt.json"

        receipt = reconcile_loaded_source_keys(con, "2026-08", parts, good, receipt_path, counts)
        assert receipt["status"] == "ok"

        bad = tmp_path / "bad.parquet"
        if bad_input == "not_parquet":
            bad.write_bytes(b"not a Parquet file")
        with pytest.raises(duckdb.Error):
            reconcile_loaded_source_keys(con, "2026-08", parts, bad, receipt_path, counts)
        # The exception must propagate, but never preserve a stale success.
        after_error = json.loads(receipt_path.read_text())
        assert after_error["status"] == "failed"

        retry = reconcile_loaded_source_keys(con, "2026-08", parts, good, receipt_path, counts)
        assert retry["status"] == "ok"
        assert json.loads(receipt_path.read_text())["status"] == "ok"
    finally:
        con.close()


def test_hardlink_alias_of_ten_canonical_csv_paths_is_rejected(tmp_path: Path) -> None:
    """Ten canonical pathnames are not ten independent parsed source inodes."""
    import os

    from ficha_etl import registry
    from ficha_etl.transform import _create_table_from_csvs

    parts = _parts(tmp_path / "sources")
    columns = [""] * 30
    columns[0:3] = ["12345678", "0001", "95"]
    original = parts[0][1]
    original.write_text(";".join(columns) + "\n", encoding="utf-8")
    for _, path in parts[1:]:
        path.unlink()
        os.link(original, path)

    # Path.resolve() is distinct, even though all ten pathnames share an inode.
    assert len({p.resolve() for _, p in parts}) == 10
    assert len({(p.stat().st_dev, p.stat().st_ino) for _, p in parts}) == 1

    con = duckdb.connect()
    try:
        counts: dict[Path, int] = {}
        _create_table_from_csvs(
            con,
            "estabelecimento",
            [path for _, path in parts],
            registry.main_table("estabelecimento").source,
            per_file_counts=counts,
        )
        assert len(counts) == 10
        assert set(counts.values()) == {1}
        parquet = tmp_path / "equal.parquet"
        con.execute(
            "COPY (SELECT cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj "
            "FROM estabelecimento) TO ? (FORMAT PARQUET)",
            [str(parquet)],
        )
        receipt_path = tmp_path / "receipt.json"
        with pytest.raises(MonthlySourceBagError, match="noncanonical source part"):
            reconcile_loaded_source_keys(con, "2026-08", parts, parquet, receipt_path, counts)
        receipt = json.loads(receipt_path.read_text())
        assert receipt["status"] == "failed"
        assert "noncanonical" in receipt["reason"]
    finally:
        con.close()
