"""Phase 2 shadow canonical establishment writer tests."""

from __future__ import annotations

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
        "data_inicio_atividade": 0,
        "data_situacao_especial": 1,
    }
    assert report.invalid_casts_total == 2
    assert report.sample_size == 2
    assert len(report.sample_fingerprint) == 64
    assert report.sample_mismatches == 0
    assert report.schema_matches is True
    assert report.bytes_written == output.stat().st_size
    assert report.codec == "ZSTD"
    assert report.row_group_size == 200_000
    assert not output.with_name(f".{output.name}.partial").exists()

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
    assert not output.with_name(f".{output.name}.partial").exists()


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
