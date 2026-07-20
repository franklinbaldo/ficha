"""Phase 2/3 shadow canonical writer tests -- table-driven (#97 slice 2).

Two entry points, chosen by physical layout (see canonical_shadow.py's
module docstring): `write_canonical_part`/`run_canonical_shadow_part` for
single-physical-file validation (estabelecimento, via the
`write_estabelecimento_canonical_part`/`run_shadow_part` backward-compat
wrappers exercised unchanged below), and `write_canonical_dataset` for a
table with more than one physical source file and
`duplicate_policy="deterministic-collapse"` -- empresa, which RFB publishes
as ten `EmpresasN.zip` parts (`sources.py`'s `_BIG_TABLES`), not one CSV.
`write_canonical_part` refuses to run against such a table (a key duplicated
*across* two different physical ZIPs would never be visible to two separate
single-part calls); the empresa tests below exercise `write_canonical_dataset`
with the complete ten-part set.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from ficha_etl import canonical_shadow, registry


def _row(**overrides: str | None) -> dict[str, str | None]:
    values: dict[str, str | None] = {name: "" for name in registry.ESTABELECIMENTO_COLUMNS}
    values.update(
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "situacao_cadastral": "02",
            "data_situacao_cadastral": "20260719",
            "data_inicio_atividade": "19991231",
            "data_situacao_especial": "",
            "cep": "01001000",
            "municipio": "7107",
        }
    )
    values.update(overrides)
    return values


def _write_csv(path: Path, rows: list[dict[str, str | None]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter=";", quotechar='"', lineterminator="\n")
        for row in rows:
            writer.writerow([row[name] for name in registry.ESTABELECIMENTO_COLUMNS])


def _empresa_row(**overrides: str) -> dict[str, str]:
    values: dict[str, str] = dict.fromkeys(registry.EMPRESA_COLUMNS, "")
    values.update(
        {
            "cnpj_basico": "00000001",
            "razao_social": "ACME LTDA",
            "natureza_juridica": "2062",
            "qualificacao_responsavel": "49",
            "capital_social": "150000,00",
            "porte_empresa": "03",
        }
    )
    values.update(overrides)
    return values


def _write_empresa_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter=";", quotechar='"', lineterminator="\n")
        for row in rows:
            writer.writerow([row[name] for name in registry.EMPRESA_COLUMNS])


_EMPRESA_PARTS = tuple(f"Empresas{n}.zip" for n in range(10))


def _write_empresa_dataset(
    root: Path, rows_by_part: dict[str, list[dict[str, str]]]
) -> list[tuple[Path, str]]:
    """Build the complete ten-part physical empresa fixture set (per
    `sources.canonical_inventory()`): every `EmpresasN.zip` name gets a CSV,
    empty unless `rows_by_part` supplies rows for it. Returns the
    `(csv_path, source_file)` list `write_canonical_dataset` expects."""
    root.mkdir(parents=True, exist_ok=True)
    parts: list[tuple[Path, str]] = []
    for name in _EMPRESA_PARTS:
        csv_path = root / f"{name}.csv"
        _write_empresa_csv(csv_path, rows_by_part.get(name, []))
        parts.append((csv_path, name))
    return parts


def test_writer_emits_atomic_typed_part_and_raw_to_canonical_report(tmp_path):
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "canonical" / "part-0.parquet"
    _write_csv(
        csv_path,
        [
            _row(nome_fantasia="Linha 1\nLinha 2"),
            _row(
                cnpj_basico="00000002",
                data_situacao_cadastral="00000000",
                data_inicio_atividade="0",
                data_situacao_especial="not-a-date",
            ),
        ],
    )

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_estabelecimento_canonical_part(
            con,
            csv_path,
            output,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
            sample_size=100,
        )
        rows = con.execute(
            """
            SELECT
                cnpj_basico,
                data_situacao_cadastral,
                data_inicio_atividade,
                data_situacao_especial,
                nome_fantasia,
                _source_file,
                _source_snapshot
            FROM read_parquet(?)
            ORDER BY cnpj_basico
            """,
            [str(output)],
        ).fetchall()
        described = {
            name: duckdb_type
            for name, duckdb_type, *_ in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]
            ).fetchall()
        }
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_raw == 2
    assert report.rows_canonical == 2
    assert report.required_key_failures == {
        "cnpj_basico": 0,
        "cnpj_ordem": 0,
        "cnpj_dv": 0,
    }
    assert report.duplicate_key_rows == 0
    assert report.invalid_casts_by_column == {
        "data_situacao_cadastral": 1,
        "data_inicio_atividade": 1,
        "data_situacao_especial": 1,
    }
    assert report.invalid_casts_total == 3
    assert report.sample_size == 2
    assert len(report.sample_fingerprint) == 64
    assert report.sample_mismatches == 0
    assert report.schema_matches is True
    assert report.bytes_written == output.stat().st_size
    assert report.codec == "ZSTD"
    assert report.row_group_size == 200_000
    assert not list(output.parent.glob(f".{output.name}.*.partial"))

    assert str(rows[0][1]) == "2026-07-19"
    assert str(rows[0][2]) == "1999-12-31"
    assert rows[0][4] == "Linha 1\nLinha 2"
    assert rows[0][5:] == ("Estabelecimentos0.zip", "2026-07")
    assert rows[1][1:4] == (None, None, None)
    assert described["cnpj_basico"] == "VARCHAR"
    assert described["data_inicio_atividade"] == "DATE"
    assert described["_source_file"] == "VARCHAR"


def test_required_key_gate_fails_before_replacing_existing_output(tmp_path):
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "part-0.parquet"
    output.write_bytes(b"existing-good-output")
    _write_csv(csv_path, [_row(cnpj_dv="")])

    con = duckdb.connect()
    try:
        with pytest.raises(canonical_shadow.CanonicalValidationError) as caught:
            canonical_shadow.write_estabelecimento_canonical_part(
                con,
                csv_path,
                output,
                source_file="Estabelecimentos0.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()

    report = caught.value.report
    assert report.status == "failed"
    assert report.required_key_failures["cnpj_dv"] == 1
    assert report.rows_canonical is None
    assert report.error is not None
    assert "required key failures" in report.error
    assert output.read_bytes() == b"existing-good-output"
    assert not list(output.parent.glob(f".{output.name}.*.partial"))


def test_duplicate_full_cnpj_gate_fails_with_excess_count(tmp_path):
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "part-0.parquet"
    _write_csv(csv_path, [_row(), _row(nome_fantasia="payload conflitante")])

    con = duckdb.connect()
    try:
        with pytest.raises(canonical_shadow.CanonicalValidationError) as caught:
            canonical_shadow.write_estabelecimento_canonical_part(
                con,
                csv_path,
                output,
                source_file="Estabelecimentos0.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()

    assert caught.value.report.duplicate_key_rows == 1
    assert "duplicate full-CNPJ" in str(caught.value)
    assert not output.exists()


def test_file_backed_run_writes_quality_and_resource_evidence(tmp_path):
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "canonical" / "part-0.parquet"
    report_path = tmp_path / "evidence" / "quality.json"
    metrics_path = tmp_path / "evidence" / "metrics.json"
    work_dir = tmp_path / "work"
    _write_csv(csv_path, [_row()])

    report = canonical_shadow.run_shadow_part(
        csv_path,
        output,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
        work_dir=work_dir,
        report_path=report_path,
        metrics_path=metrics_path,
        sample_size=10,
    )

    quality = json.loads(report_path.read_text())
    envelope = json.loads(metrics_path.read_text())
    stage = envelope["stages"][0]

    assert report.status == "ok"
    assert quality["status"] == "ok"
    assert quality["rows_raw"] == quality["rows_canonical"] == 1
    assert envelope["schema_version"] == "1"
    assert envelope["month"] == "2026-07"
    assert stage["stage"] == "canonical_estabelecimento_part"
    assert stage["rows_read"] == stage["rows_written"] == 1
    assert stage["files_read"] == 1
    assert stage["casts_invalid"] == 0
    assert stage["duplicate_rows"] == 0
    assert stage["extra"]["sample_mismatches"] == 0
    assert stage["extra"]["codec"] == "ZSTD"
    assert not (work_dir / "canonical-estabelecimento.duckdb").exists()
    assert output.exists()


def test_failed_file_backed_run_still_preserves_quality_and_metrics(tmp_path):
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "canonical" / "part-0.parquet"
    report_path = tmp_path / "evidence" / "quality.json"
    metrics_path = tmp_path / "evidence" / "metrics.json"
    _write_csv(csv_path, [_row(cnpj_basico="")])

    with pytest.raises(canonical_shadow.CanonicalValidationError):
        canonical_shadow.run_shadow_part(
            csv_path,
            output,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
            work_dir=tmp_path / "work",
            report_path=report_path,
            metrics_path=metrics_path,
        )

    quality = json.loads(report_path.read_text())
    stage = json.loads(metrics_path.read_text())["stages"][0]
    assert quality["status"] == "failed"
    assert quality["required_key_failures"]["cnpj_basico"] == 1
    assert stage["extra"]["status"] == "failed"
    assert stage["rows_written"] is None
    assert not output.exists()


def test_cli_rejects_invalid_snapshot_before_opening_duckdb(tmp_path, capsys):
    result = canonical_shadow.main(
        [
            "--csv",
            str(tmp_path / "missing.csv"),
            "--source-file",
            "Estabelecimentos0.zip",
            "--snapshot",
            "2026-99",
            "--output",
            str(tmp_path / "part.parquet"),
        ]
    )

    assert result == 2
    assert "snapshot must be YYYY-MM" in capsys.readouterr().err


# -----------------------------------------------------------------------------
# empresa (#97 slice 2) -- ten physical parts (Empresas0.zip..Empresas9.zip
# per sources.canonical_inventory()), duplicate_policy="deterministic-collapse"
# (registry.EMPRESA_CANONICAL). Exercises write_canonical_dataset, the entry
# point required for any multi-part deterministic-collapse table -- see
# canonical_shadow.py's module docstring for why write_canonical_part (single
# CSV) cannot be used here.
# -----------------------------------------------------------------------------


def test_empresa_via_write_canonical_part_fails_closed_single_zip(tmp_path):
    """A call attempting canonical empresa from only one physical ZIP must
    fail closed rather than silently producing an apparently-valid but
    globally-incomplete part."""
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    _write_empresa_csv(csv_path, [_empresa_row()])

    con = duckdb.connect()
    try:
        with pytest.raises(ValueError, match="write_canonical_dataset"):
            canonical_shadow.write_canonical_part(
                con,
                "empresa",
                csv_path,
                output,
                source_file="Empresas0.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()
    assert not output.exists()


def test_empresa_dataset_missing_expected_part_fails_closed(tmp_path):
    parts = _write_empresa_dataset(tmp_path / "dataset", {"Empresas0.zip": [_empresa_row()]})
    incomplete = parts[:-1]  # drop Empresas9.zip
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        with pytest.raises(ValueError, match="incomplete physical part set"):
            canonical_shadow.write_canonical_dataset(
                con, "empresa", incomplete, output, source_snapshot="2026-07"
            )
    finally:
        con.close()
    assert not output.exists()


def test_empresa_projection_casts_capital_social_decimal_comma(tmp_path):
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {
            "Empresas0.zip": [
                _empresa_row(cnpj_basico="00000001", capital_social="150000,50"),
                _empresa_row(cnpj_basico="00000002", capital_social=""),
                _empresa_row(cnpj_basico="00000003", capital_social="não-é-decimal"),
            ]
        },
    )
    output = tmp_path / "part-0.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07", sample_size=10
        )
        rows = con.execute(
            "SELECT cnpj_basico, capital_social FROM read_parquet(?) ORDER BY cnpj_basico",
            [str(output)],
        ).fetchall()
        described = {
            name: duckdb_type
            for name, duckdb_type, *_ in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(output)]
            ).fetchall()
        }
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_raw == report.rows_canonical == 3
    assert report.duplicate_key_rows == 0
    assert report.duplicate_key_count == 0
    assert report.conflicting_key_count == 0
    assert report.invalid_casts_by_column == {"capital_social": 1}  # only the malformed one
    assert report.schema_matches is True
    assert report.sample_mismatches == 0  # the DECIMAL sample-mismatch fix
    assert described["capital_social"] == "DECIMAL(18,2)"

    from decimal import Decimal

    assert rows[0] == ("00000001", Decimal("150000.50"))
    assert rows[1] == ("00000002", None)  # blank
    assert rows[2] == ("00000003", None)  # malformed nonblank


def test_empresa_duplicate_key_entirely_inside_one_part_still_works(tmp_path):
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {"Empresas0.zip": [_empresa_row(), _empresa_row(), _empresa_row()]},
    )
    output = tmp_path / "part-0.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07"
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()[0]
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_raw == 3
    assert report.rows_canonical == 1
    assert report.duplicate_key_rows == 2  # excess rows
    assert report.duplicate_key_count == 1  # one distinct key duplicated
    assert report.conflicting_key_count == 0  # exact duplicates, no conflict
    assert report.conflicting_sample == []
    assert row_count == 1


def test_empresa_exact_duplicate_across_two_parts_collapses_globally(tmp_path):
    """The same exact empresa row published in two different physical ZIPs
    (e.g. republished across releases) must collapse to one row, not survive
    once per part -- the core bug this fix addresses."""
    row = _empresa_row()
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {"Empresas0.zip": [row], "Empresas5.zip": [row]},
    )
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07"
        )
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()[0]
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_raw == 2
    assert report.rows_canonical == 1
    assert row_count == 1
    assert report.duplicate_key_count == 1
    assert report.duplicate_key_rows == 1  # one excess row, counted across parts
    assert report.conflicting_key_count == 0  # identical payload, not a conflict


def test_empresa_conflicting_duplicates_across_parts_collapse_deterministically(tmp_path):
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {
            "Empresas0.zip": [_empresa_row(razao_social="CONFLICT A")],
            "Empresas5.zip": [_empresa_row(razao_social="CONFLICT B")],
        },
    )
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07"
        )
        survivor_razao_social, survivor_source_file = con.execute(
            f"SELECT razao_social, _source_file FROM read_parquet('{output}')"
        ).fetchone()
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_canonical == 1
    assert report.duplicate_key_count == 1
    assert report.conflicting_key_count == 1
    assert len(report.conflicting_sample) == 1
    sample = report.conflicting_sample[0]
    assert sample["cnpj_basico"] == "00000001"
    # Conflict evidence identifies not just the key but which physical parts
    # contributed to it.
    assert sample["source_files"] == ["Empresas0.zip", "Empresas5.zip"]
    # Deterministic full-row tiebreak: lexicographically smaller row survives
    # ("CONFLICT A" < "CONFLICT B") -- reproducible, not "whatever DuckDB kept".
    assert survivor_razao_social == "CONFLICT A"
    assert survivor_source_file == "Empresas0.zip"


def test_empresa_dataset_order_independent_across_parts_and_rows(tmp_path):
    """Not just the same SET of records after sorting in the test query --
    the writer itself must emit the same physical row SEQUENCE (RFC 0001
    requires the same records, ordering and deduplication decisions), so
    this reads the Parquet with no ORDER BY at all and compares the raw
    fetch order. Byte-identical Parquet files are not claimed or compared
    (Parquet writer metadata, e.g. timestamps, can differ) -- instead a
    SHA256 digest of the logical row sequence is compared, which is exactly
    what would catch a silent reordering without over-claiming file-level
    identity.
    """
    exact_row = _empresa_row(cnpj_basico="00000002")
    conflict_a = _empresa_row(cnpj_basico="00000003", razao_social="CONFLICT A")
    conflict_b = _empresa_row(cnpj_basico="00000003", razao_social="CONFLICT B")

    def run(tag: str, *, reverse_parts: bool, reverse_rows: bool):
        rows_by_part = {
            "Empresas0.zip": [_empresa_row(cnpj_basico="00000001"), exact_row, conflict_a],
            "Empresas5.zip": [exact_row, conflict_b],
        }
        if reverse_rows:
            rows_by_part = {name: list(reversed(rows)) for name, rows in rows_by_part.items()}
        parts = _write_empresa_dataset(tmp_path / f"dataset-{tag}", rows_by_part)
        if reverse_parts:
            parts = list(reversed(parts))
        output = tmp_path / f"part-{tag}.parquet"
        con = duckdb.connect()
        try:
            report = canonical_shadow.write_canonical_dataset(
                con, "empresa", parts, output, source_snapshot="2026-07", sample_size=10
            )
            # No ORDER BY here on purpose -- this must reflect the order the
            # writer itself emitted, not an order imposed by the test query.
            rows = con.execute(
                "SELECT cnpj_basico, razao_social, _source_file FROM read_parquet(?)",
                [str(output)],
            ).fetchall()
        finally:
            con.close()
        return report, rows

    report_fwd, rows_fwd = run("fwd", reverse_parts=False, reverse_rows=False)
    report_rev, rows_rev = run("rev", reverse_parts=True, reverse_rows=True)

    # Emitted physical row sequence is identical, not just the same set.
    assert rows_fwd == rows_rev
    # cnpj_basico is the primary key, so ordering by it is the writer's
    # documented explicit contract -- pin it directly, not just "whatever
    # order came out of both runs happened to match".
    assert [row[0] for row in rows_fwd] == sorted(row[0] for row in rows_fwd)

    digest_fwd = hashlib.sha256(
        json.dumps(rows_fwd, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    digest_rev = hashlib.sha256(
        json.dumps(rows_rev, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    assert digest_fwd == digest_rev

    assert report_fwd.conflicting_sample == report_rev.conflicting_sample
    assert report_fwd.duplicate_key_count == report_rev.duplicate_key_count == 2
    assert report_fwd.duplicate_key_rows == report_rev.duplicate_key_rows == 2
    assert report_fwd.sample_fingerprint == report_rev.sample_fingerprint


def test_empresa_dataset_sample_fingerprint_independent_of_scan_order(tmp_path):
    """_sample()'s selection must be a function of key VALUES, not scan
    order -- with more canonical rows than sample_size, reservoir sampling
    (even with a repeatable seed) could select a different subset of rows
    depending on part/row order, silently changing sample_fingerprint for
    logically identical input. Six distinct keys, sample_size=3, forces real
    sub-selection (not "sample_size >= available rows, so everything is
    selected regardless of algorithm", which the order-independence test
    above doesn't exercise since it only has 3 canonical rows)."""
    rows_by_part = {
        "Empresas0.zip": [_empresa_row(cnpj_basico=f"{n:08d}") for n in range(1, 4)],
        "Empresas5.zip": [_empresa_row(cnpj_basico=f"{n:08d}") for n in range(4, 7)],
    }

    def run(tag: str, *, reverse_parts: bool, reverse_rows: bool):
        parts_rows = rows_by_part
        if reverse_rows:
            parts_rows = {name: list(reversed(rows)) for name, rows in parts_rows.items()}
        parts = _write_empresa_dataset(tmp_path / f"dataset-{tag}", parts_rows)
        if reverse_parts:
            parts = list(reversed(parts))
        output = tmp_path / f"part-{tag}.parquet"
        con = duckdb.connect()
        try:
            report = canonical_shadow.write_canonical_dataset(
                con, "empresa", parts, output, source_snapshot="2026-07", sample_size=3
            )
        finally:
            con.close()
        return report

    report_fwd = run("fwd", reverse_parts=False, reverse_rows=False)
    report_rev = run("rev", reverse_parts=True, reverse_rows=True)

    assert report_fwd.rows_canonical == 6
    assert report_fwd.sample_size == 3  # genuine sub-selection, not "everything"
    assert report_fwd.sample_fingerprint == report_rev.sample_fingerprint


def test_sample_mismatches_validates_the_same_keys_sample_fingerprinted(tmp_path):
    """_sample_mismatches must join through the SAME deterministic sample-key
    selection _sample() fingerprinted, not an independently reservoir-sampled
    (and therefore potentially different) set of rows. Six distinct keys,
    sample_size=3, reversed part+row order in the second run. Corrupts the
    WRITTEN canonical Parquet directly (post-write, since the writer's own
    round-trip check would otherwise fail closed on a real mismatch) for one
    key inside the deterministic sample and one key outside it: only the
    in-sample corruption may be detected, proving fingerprint and validation
    share one bounded subset rather than two unrelated samples. Which key
    lands "inside" vs "outside" is discovered via _selected_sample_keys, not
    hardcoded, since that depends on DuckDB's hash() ranking.
    """
    table = registry.main_table("empresa")
    spec = table.canonical
    sample_size = 3
    all_keys = [f"{n:08d}" for n in range(1, 7)]

    def run(tag: str, *, reverse_parts: bool, reverse_rows: bool):
        rows_by_part = {
            "Empresas0.zip": [_empresa_row(cnpj_basico=k) for k in all_keys[:3]],
            "Empresas5.zip": [_empresa_row(cnpj_basico=k) for k in all_keys[3:]],
        }
        if reverse_rows:
            rows_by_part = {name: list(reversed(rows)) for name, rows in rows_by_part.items()}
        parts = _write_empresa_dataset(tmp_path / f"dataset-{tag}", rows_by_part)
        if reverse_parts:
            parts = list(reversed(parts))
        output = tmp_path / f"part-{tag}.parquet"

        con = duckdb.connect()
        try:
            report = canonical_shadow.write_canonical_dataset(
                con, "empresa", parts, output, source_snapshot="2026-07", sample_size=sample_size
            )
            assert report.status == "ok"
            assert report.sample_mismatches == 0  # writer's own round-trip check passed

            raw_table = canonical_shadow._raw_table_name("empresa")
            selected = canonical_shadow._selected_sample_keys(con, raw_table, spec, sample_size)
            selected_keys = {row[0] for row in selected}
            outside_keys = set(all_keys) - selected_keys
            assert len(selected_keys) == sample_size
            assert outside_keys  # sanity: sample_size < total distinct keys

            in_sample_key = sorted(selected_keys)[0]
            outside_key = sorted(outside_keys)[0]

            con.execute(
                "CREATE OR REPLACE TABLE _corrupt_stage AS SELECT * FROM read_parquet(?)",
                [str(output)],
            )
            con.execute(
                "UPDATE _corrupt_stage SET razao_social = 'CORRUPTED' WHERE cnpj_basico = ?",
                [in_sample_key],
            )
            con.execute(
                "UPDATE _corrupt_stage SET razao_social = 'CORRUPTED' WHERE cnpj_basico = ?",
                [outside_key],
            )
            corrupted_path = tmp_path / f"corrupted-{tag}.parquet"
            con.execute(
                f"COPY _corrupt_stage TO "
                f"{canonical_shadow._literal(str(corrupted_path))} (FORMAT PARQUET)"
            )

            source_file_sql = f"src.{canonical_shadow._SOURCE_FILE_TAG}"
            mismatches = canonical_shadow._sample_mismatches(
                con,
                raw_table,
                spec,
                corrupted_path,
                sample_size,
                source_file_sql,
                "2026-07",
            )
        finally:
            con.close()
        return mismatches

    mismatches_fwd = run("fwd", reverse_parts=False, reverse_rows=False)
    mismatches_rev = run("rev", reverse_parts=True, reverse_rows=True)

    # Only the in-sample corruption is counted -- the outside-sample one is
    # invisible to validation, proving the bounded subset is respected.
    assert mismatches_fwd == 1
    assert mismatches_rev == 1
    assert mismatches_fwd == mismatches_rev


def test_empresa_dataset_counts_include_cross_part_occurrences(tmp_path):
    """duplicate_key_count/duplicate_key_rows must reflect the FULL logical
    dataset, not just within-part duplicates -- a key repeated across three
    different parts is still one duplicate key with two excess rows."""
    row = _empresa_row()
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {"Empresas0.zip": [row], "Empresas3.zip": [row], "Empresas7.zip": [row]},
    )
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07"
        )
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_raw == 3
    assert report.rows_canonical == 1
    assert report.duplicate_key_count == 1
    assert report.duplicate_key_rows == 2  # 3 occurrences - 1 survivor


def test_empresa_null_blank_key_fails_separately_from_duplicates(tmp_path):
    # A genuine duplicate (cnpj_basico repeated, across two parts) AND a
    # blank-keyed row -- the blank-key failure must fire, not the duplicate
    # path.
    parts = _write_empresa_dataset(
        tmp_path / "dataset",
        {
            "Empresas0.zip": [_empresa_row(), _empresa_row(cnpj_basico="")],
            "Empresas5.zip": [_empresa_row()],
        },
    )
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        with pytest.raises(canonical_shadow.CanonicalValidationError) as caught:
            canonical_shadow.write_canonical_dataset(
                con, "empresa", parts, output, source_snapshot="2026-07"
            )
    finally:
        con.close()

    report = caught.value.report
    assert report.status == "failed"
    assert report.required_key_failures["cnpj_basico"] == 1
    assert "required key failures" in report.error
    assert not output.exists()


def test_empresa_discarded_duplicate_malformed_capital_social_still_counted(tmp_path):
    """_invalid_casts must run BEFORE the deterministic collapse -- a
    malformed value on the row that LOSES the tiebreak (and is discarded)
    must still be counted, not silently lost along with the row."""
    survivor = _empresa_row(capital_social="100000,00")  # sorts first, survives
    discarded = _empresa_row(capital_social="not-a-decimal")  # sorts after, discarded
    parts = _write_empresa_dataset(tmp_path / "dataset", {"Empresas0.zip": [survivor, discarded]})
    output = tmp_path / "part.parquet"

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_dataset(
            con, "empresa", parts, output, source_snapshot="2026-07"
        )
        remaining = con.execute(f"SELECT capital_social FROM read_parquet('{output}')").fetchone()[
            0
        ]
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_canonical == 1
    from decimal import Decimal

    assert remaining == Decimal("100000.00")  # the well-formed row survived
    # ...but the malformed value on the DISCARDED row is still in the raw
    # invalid-cast evidence.
    assert report.invalid_casts_by_column.get("capital_social") == 1


def test_policy_validation_fires_before_data_inspection_on_unique_input(tmp_path, monkeypatch):
    """An unsupported/inconsistent duplicate_policy must fail even when the
    input contains zero duplicate keys -- the old code only checked policy
    validity inside an `if duplicate_keys:` gate, so a unique-only input
    would have silently proceeded despite a tampered/unsupported policy."""
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "part-0.parquet"
    _write_csv(csv_path, [_row()])  # single row, no duplicates at all

    tampered = copy.deepcopy(registry.ESTABELECIMENTO_CANONICAL)
    object.__setattr__(tampered, "duplicate_policy", "quarantine")
    table = registry.main_table("estabelecimento")
    monkeypatch.setattr(canonical_shadow, "_spec", lambda table_name: (table, tampered))

    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="unsupported duplicate_policy"):
            canonical_shadow.write_canonical_part(
                con,
                "estabelecimento",
                csv_path,
                output,
                source_file="Estabelecimentos0.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()
    assert not output.exists()


def test_estabelecimento_single_part_wrapper_unaffected_by_multi_part_guard(tmp_path):
    """estabelecimento has ten physical parts too (sources.py's
    _BIG_TABLES), but duplicate_policy="fail" -- the write_canonical_part
    multi-part guard must only block deterministic-collapse tables, not
    every multi-part table."""
    csv_path = tmp_path / "estabelecimentos.csv"
    output = tmp_path / "part-0.parquet"
    _write_csv(csv_path, [_row()])

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_estabelecimento_canonical_part(
            con,
            csv_path,
            output,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
    finally:
        con.close()

    assert report.status == "ok"
    assert output.exists()


def test_writer_fails_closed_on_unsupported_duplicate_policy(tmp_path, monkeypatch):
    """ParquetSpec.__post_init__ already restricts duplicate_policy to
    "fail"/"deterministic-collapse", so this is normally unreachable --
    force an unrecognized value onto a COPY of the frozen dataclass (never
    the shared module-level constant) via object.__setattr__, bypassing
    __post_init__ (which only runs at construction, not on later mutation),
    to prove the writer itself fails closed instead of silently defaulting
    to one branch.
    """
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    _write_empresa_csv(csv_path, [_empresa_row(), _empresa_row()])

    tampered = copy.deepcopy(registry.EMPRESA_CANONICAL)
    object.__setattr__(tampered, "duplicate_policy", "quarantine")
    table = registry.main_table("empresa")

    monkeypatch.setattr(canonical_shadow, "_spec", lambda table_name: (table, tampered))

    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="unsupported duplicate_policy"):
            canonical_shadow.write_canonical_part(
                con,
                "empresa",
                csv_path,
                output,
                source_file="Empresas0.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()
    assert not output.exists()


def test_table_driven_invocation_rejects_table_without_canonical_contract(tmp_path):
    csv_path = tmp_path / "simples.csv"
    csv_path.write_text("", encoding="utf-8")
    output = tmp_path / "part-0.parquet"

    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="no canonical contract"):
            canonical_shadow.write_canonical_part(
                con,
                "simples",
                csv_path,
                output,
                source_file="Simples.zip",
                source_snapshot="2026-07",
            )
    finally:
        con.close()


def test_empresa_single_part_file_backed_run_fails_closed(tmp_path):
    """run_canonical_shadow_part (the metrics-wrapped single-part run) must
    refuse empresa too -- it calls write_canonical_part internally, so the
    same multi-part guard applies. The metrics-wrapped dataset-level run is
    deferred to the slice that actually needs it (#97 slice 3)."""
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "canonical" / "part-0.parquet"
    _write_empresa_csv(csv_path, [_empresa_row()])

    with pytest.raises(ValueError, match="write_canonical_dataset"):
        canonical_shadow.run_canonical_shadow_part(
            "empresa",
            csv_path,
            output,
            source_file="Empresas0.zip",
            source_snapshot="2026-07",
            work_dir=tmp_path / "work",
            report_path=tmp_path / "evidence" / "quality.json",
            metrics_path=tmp_path / "evidence" / "metrics.json",
            sample_size=10,
        )
    assert not output.exists()
