"""Phase 2/3 shadow canonical writer tests -- table-driven (#97 slice 2).

`canonical_shadow.write_canonical_part`/`run_canonical_shadow_part` serve
any main table with a registry canonical contract; entity-specific behavior
comes only from the table's declared `duplicate_policy`. Estabelecimento
tests below exercise the `write_estabelecimento_canonical_part`/
`run_shadow_part` backward-compat wrappers unchanged (they must keep passing
exactly as before this generalization); empresa tests exercise the generic
entry points directly.
"""

from __future__ import annotations

import copy
import csv
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
# empresa (#97 slice 2) -- exercises write_canonical_part directly, the
# generic table-driven entry point. duplicate_policy="deterministic-collapse"
# (registry.EMPRESA_CANONICAL), unlike estabelecimento's "fail" above.
# -----------------------------------------------------------------------------


def test_empresa_projection_casts_capital_social_decimal_comma(tmp_path):
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    _write_empresa_csv(
        csv_path,
        [
            _empresa_row(cnpj_basico="00000001", capital_social="150000,50"),
            _empresa_row(cnpj_basico="00000002", capital_social=""),
            _empresa_row(cnpj_basico="00000003", capital_social="não-é-decimal"),
        ],
    )

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_part(
            con,
            "empresa",
            csv_path,
            output,
            source_file="Empresas0.zip",
            source_snapshot="2026-07",
            sample_size=10,
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


def test_empresa_exact_duplicates_collapse_to_one_row(tmp_path):
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    _write_empresa_csv(csv_path, [_empresa_row(), _empresa_row(), _empresa_row()])

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_part(
            con,
            "empresa",
            csv_path,
            output,
            source_file="Empresas0.zip",
            source_snapshot="2026-07",
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


def test_empresa_conflicting_duplicates_collapse_deterministically(tmp_path):
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    _write_empresa_csv(
        csv_path,
        [
            _empresa_row(razao_social="CONFLICT A"),
            _empresa_row(razao_social="CONFLICT B"),
        ],
    )

    con = duckdb.connect()
    try:
        report = canonical_shadow.write_canonical_part(
            con,
            "empresa",
            csv_path,
            output,
            source_file="Empresas0.zip",
            source_snapshot="2026-07",
        )
        survivor = con.execute(f"SELECT razao_social FROM read_parquet('{output}')").fetchone()[0]
    finally:
        con.close()

    assert report.status == "ok"
    assert report.rows_canonical == 1
    assert report.duplicate_key_count == 1
    assert report.conflicting_key_count == 1
    assert report.conflicting_sample == [{"cnpj_basico": "00000001"}]
    # Deterministic full-row tiebreak: lexicographically smaller row survives
    # ("CONFLICT A" < "CONFLICT B") -- reproducible, not "whatever DuckDB kept".
    assert survivor == "CONFLICT A"


def test_empresa_output_independent_of_input_row_order(tmp_path):
    rows = [
        _empresa_row(),
        _empresa_row(),
        _empresa_row(cnpj_basico="00000002", razao_social="CONFLICT A"),
        _empresa_row(cnpj_basico="00000002", razao_social="CONFLICT B"),
        _empresa_row(cnpj_basico="00000003", capital_social="999999,99"),
    ]

    def run(
        order: list[dict[str, str]], tag: str
    ) -> tuple[canonical_shadow.CanonicalPartReport, list]:
        csv_path = tmp_path / f"empresa-{tag}.csv"
        output = tmp_path / f"part-{tag}.parquet"
        _write_empresa_csv(csv_path, order)
        con = duckdb.connect()
        try:
            report = canonical_shadow.write_canonical_part(
                con,
                "empresa",
                csv_path,
                output,
                source_file="Empresas0.zip",
                source_snapshot="2026-07",
                sample_size=10,
            )
            out_rows = con.execute(
                "SELECT cnpj_basico, razao_social, capital_social "
                f"FROM read_parquet('{output}') ORDER BY cnpj_basico"
            ).fetchall()
        finally:
            con.close()
        return report, out_rows

    report_forward, rows_forward = run(rows, "forward")
    report_reversed, rows_reversed = run(list(reversed(rows)), "reversed")

    assert rows_forward == rows_reversed
    assert report_forward.conflicting_sample == report_reversed.conflicting_sample
    assert report_forward.duplicate_key_count == report_reversed.duplicate_key_count
    assert report_forward.sample_fingerprint == report_reversed.sample_fingerprint


def test_empresa_null_blank_key_fails_separately_from_duplicates(tmp_path):
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "part-0.parquet"
    # A genuine duplicate (cnpj_basico repeated) AND a blank-keyed row in the
    # same fixture -- the blank-key failure must fire, not the duplicate path.
    _write_empresa_csv(
        csv_path,
        [_empresa_row(), _empresa_row(), _empresa_row(cnpj_basico="")],
    )

    con = duckdb.connect()
    try:
        with pytest.raises(canonical_shadow.CanonicalValidationError) as caught:
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

    report = caught.value.report
    assert report.status == "failed"
    assert report.required_key_failures["cnpj_basico"] == 1
    assert "required key failures" in report.error
    assert not output.exists()


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


def test_empresa_file_backed_run_uses_table_specific_stage_and_database_names(tmp_path):
    csv_path = tmp_path / "empresa.csv"
    output = tmp_path / "canonical" / "part-0.parquet"
    report_path = tmp_path / "evidence" / "quality.json"
    metrics_path = tmp_path / "evidence" / "metrics.json"
    work_dir = tmp_path / "work"
    _write_empresa_csv(csv_path, [_empresa_row(), _empresa_row()])  # exact dup, collapses

    report = canonical_shadow.run_canonical_shadow_part(
        "empresa",
        csv_path,
        output,
        source_file="Empresas0.zip",
        source_snapshot="2026-07",
        work_dir=work_dir,
        report_path=report_path,
        metrics_path=metrics_path,
        sample_size=10,
    )

    envelope = json.loads(metrics_path.read_text())
    stage = envelope["stages"][0]

    assert report.status == "ok"
    assert report.rows_raw == 2
    assert report.rows_canonical == 1
    assert stage["stage"] == "canonical_empresa_part"
    assert stage["duplicate_rows"] == 1
    assert stage["extra"]["duplicate_key_count"] == 1
    assert stage["extra"]["conflicting_key_count"] == 0
    assert not (work_dir / "canonical-empresa.duckdb").exists()
    assert output.exists()
