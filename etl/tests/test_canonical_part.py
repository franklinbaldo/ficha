"""Shadow canonical estabelecimento checkpoint tests (RFC 0001 Phase 2)."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from ficha_etl import canonical, registry


def _row(**overrides: str | None) -> dict[str, str | None]:
    row: dict[str, str | None] = {column: "" for column in registry.ESTABELECIMENTO_COLUMNS}
    row.update(
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "identificador_matriz_filial": "1",
            "situacao_cadastral": "02",
        }
    )
    row.update(overrides)
    return row


def _write_csv(path: Path, rows: list[dict[str, str | None]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(
            handle,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )
        writer.writerows(
            [[row[column] for column in registry.ESTABELECIMENTO_COLUMNS] for row in rows]
        )


def test_write_part_types_dates_samples_and_persists_manifest(tmp_path):
    csv_path = tmp_path / "Estabelecimentos0.csv"
    output_path = tmp_path / "canonical" / "estabelecimento-0.parquet"
    work_dir = tmp_path / "work"
    _write_csv(
        csv_path,
        [
            _row(
                data_situacao_cadastral="20240229",
                data_inicio_atividade="19991231",
                data_situacao_especial="",
                nome_fantasia="Linha 1\nLinha 2",
                cep="01001000",
                municipio="7107",
            ),
            _row(
                cnpj_basico="00000002",
                cnpj_ordem="0002",
                cnpj_dv="72",
                data_situacao_cadastral="00000000",
                data_inicio_atividade="nao-e-data",
                data_situacao_especial="20260719",
                cep="00000000",
                municipio="0001",
            ),
        ],
    )

    result = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos'0.zip",
        source_snapshot="2026-07",
        work_dir=work_dir,
    )

    assert result.reused is False
    assert output_path.exists()
    assert result.manifest_path.exists()
    assert not work_dir.exists()

    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT cnpj_basico, cnpj_ordem, cnpj_dv,
                   data_situacao_cadastral, data_inicio_atividade,
                   data_situacao_especial, nome_fantasia, cep, municipio,
                   _source_file, _source_snapshot
            FROM read_parquet(?)
            ORDER BY cnpj_basico
            """,
            [str(output_path)],
        ).fetchall()
        described = {
            name: duckdb_type
            for name, duckdb_type, *_ in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(output_path)]
            ).fetchall()
        }
    finally:
        con.close()

    assert rows == [
        (
            "00000001",
            "0001",
            "91",
            date(2024, 2, 29),
            date(1999, 12, 31),
            None,
            "Linha 1\nLinha 2",
            "01001000",
            "7107",
            "Estabelecimentos'0.zip",
            "2026-07",
        ),
        (
            "00000002",
            "0002",
            "72",
            None,
            None,
            date(2026, 7, 19),
            "",
            "00000000",
            "0001",
            "Estabelecimentos'0.zip",
            "2026-07",
        ),
    ]
    assert described["cnpj_basico"] == "VARCHAR"
    assert described["data_inicio_atividade"] == "DATE"

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest == result.manifest
    assert manifest["validation"]["status"] == "passed"
    assert manifest["validation"]["source_rows"] == 2
    assert manifest["validation"]["output_rows"] == 2
    assert manifest["validation"]["sample_seed"] == 42
    assert manifest["validation"]["sample_size"] == 2
    assert len(manifest["validation"]["sample_fingerprint"]) == 64
    assert manifest["validation"]["sample_mismatches"] == 0
    assert manifest["casts_invalid"] == {
        "data_situacao_cadastral": 0,
        "data_inicio_atividade": 1,
        "data_situacao_especial": 0,
    }
    stage = manifest["metrics"]["stages"][0]
    assert stage["casts_invalid"] == 1
    assert stage["quarantine_rows"] == 0
    assert stage["extra"]["sample_mismatches"] == 0
    assert manifest["output"]["sha256"] == canonical._sha256(output_path)  # noqa: SLF001
    assert not output_path.with_suffix(".parquet.failure.json").exists()


def test_resume_reuses_only_matching_checksummed_checkpoint(tmp_path):
    csv_path = tmp_path / "Estabelecimentos0.csv"
    output_path = tmp_path / "estabelecimento-0.parquet"
    _write_csv(csv_path, [_row(data_inicio_atividade="20260719")])

    first = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
    )
    first_mtime = output_path.stat().st_mtime_ns
    second = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
    )
    assert second.reused is True
    assert second.manifest == first.manifest
    assert output_path.stat().st_mtime_ns == first_mtime

    third = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
        sample_size=0,
    )
    assert third.reused is False
    assert third.manifest["validation"]["sample_size"] == 0


def test_identical_and_conflicting_duplicates_are_distinguished(tmp_path):
    identical_csv = tmp_path / "identical.csv"
    conflicting_csv = tmp_path / "conflicting.csv"
    _write_csv(identical_csv, [_row(), _row()])
    _write_csv(
        conflicting_csv,
        [_row(nome_fantasia="A"), _row(nome_fantasia="B")],
    )

    with pytest.raises(canonical.CanonicalValidationError) as identical:
        canonical.write_estabelecimento_part(
            identical_csv,
            tmp_path / "identical.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
    assert identical.value.evidence["validation"]["identical_duplicate_rows"] == 1
    assert identical.value.evidence["validation"]["conflicting_duplicate_rows"] == 0

    with pytest.raises(canonical.CanonicalValidationError) as conflicting:
        canonical.write_estabelecimento_part(
            conflicting_csv,
            tmp_path / "conflicting.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
    assert conflicting.value.evidence["validation"]["identical_duplicate_rows"] == 0
    assert conflicting.value.evidence["validation"]["conflicting_duplicate_rows"] == 1
    assert "1 conflicting" in str(conflicting.value)


def test_failed_gate_preserves_previous_checkpoint_and_writes_failure_evidence(tmp_path):
    csv_path = tmp_path / "source.csv"
    output_path = tmp_path / "canonical.parquet"
    _write_csv(csv_path, [_row()])
    first = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
    )
    output_before = output_path.read_bytes()
    manifest_before = first.manifest_path.read_bytes()

    _write_csv(csv_path, [_row(cnpj_basico="")])
    with pytest.raises(canonical.CanonicalValidationError) as caught:
        canonical.write_estabelecimento_part(
            csv_path,
            output_path,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )

    failure_path = output_path.with_suffix(".parquet.failure.json")
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert caught.value.evidence == failure
    assert failure["validation"]["status"] == "failed"
    assert failure["validation"]["primary_key_missing_rows"] == 1
    assert failure["output"]["published"] is False
    assert output_path.read_bytes() == output_before
    assert first.manifest_path.read_bytes() == manifest_before
    assert not list(tmp_path.glob(".*.tmp*"))


def test_independent_reversible_sample_rejects_corrupted_projection(tmp_path, monkeypatch):
    csv_path = tmp_path / "source.csv"
    output_path = tmp_path / "canonical.parquet"
    _write_csv(csv_path, [_row(cep="01001000")])
    original = canonical.registry.canonical_projection_sql

    def corrupted_projection(spec, *, source_alias="src"):
        sql = original(spec, source_alias=source_alias)
        return sql.replace(
            f'"{source_alias}"."cep" AS "cep"',
            'NULL::VARCHAR AS "cep"',
        )

    monkeypatch.setattr(
        canonical.registry,
        "canonical_projection_sql",
        corrupted_projection,
    )
    with pytest.raises(
        canonical.CanonicalValidationError,
        match="deterministic raw/canonical sample mismatches",
    ):
        canonical.write_estabelecimento_part(
            csv_path,
            output_path,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )
    assert not output_path.exists()
    assert output_path.with_suffix(".parquet.failure.json").exists()


def test_publish_failure_rolls_back_previous_output_and_manifest(tmp_path, monkeypatch):
    csv_path = tmp_path / "source.csv"
    output_path = tmp_path / "canonical.parquet"
    _write_csv(csv_path, [_row()])
    first = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
    )
    output_before = output_path.read_bytes()
    manifest_before = first.manifest_path.read_bytes()

    _write_csv(csv_path, [_row(nome_fantasia="changed")])
    real_replace = canonical.os.replace

    def fail_manifest_promotion(source, destination):
        if Path(destination) == first.manifest_path and str(source).endswith(".tmp"):
            raise OSError("simulated manifest promotion failure")
        real_replace(source, destination)

    monkeypatch.setattr(canonical.os, "replace", fail_manifest_promotion)
    with pytest.raises(OSError, match="simulated"):
        canonical.write_estabelecimento_part(
            csv_path,
            output_path,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )

    assert output_path.read_bytes() == output_before
    assert first.manifest_path.read_bytes() == manifest_before


def test_input_contract_and_cli_reject_invalid_values(tmp_path, capsys):
    csv_path = tmp_path / "source.csv"
    _write_csv(csv_path, [_row()])

    with pytest.raises(ValueError, match="unsupported Parquet codec"):
        canonical.write_estabelecimento_part(
            csv_path,
            tmp_path / "out.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
            codec="brotli",
        )
    with pytest.raises(ValueError, match="positive"):
        canonical.write_estabelecimento_part(
            csv_path,
            tmp_path / "out.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
            row_group_size=0,
        )
    with pytest.raises(ValueError, match="negative"):
        canonical.write_estabelecimento_part(
            csv_path,
            tmp_path / "out.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
            sample_size=-1,
        )

    result = canonical.main(
        [
            "--csv",
            str(csv_path),
            "--source-file",
            "Estabelecimentos0.zip",
            "--snapshot",
            "July-2026",
            "--output",
            str(tmp_path / "out.parquet"),
        ]
    )
    assert result == 1
    assert "YYYY-MM" in capsys.readouterr().err
