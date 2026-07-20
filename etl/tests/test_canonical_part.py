"""Shadow canonical estabelecimento part tests (RFC 0001 Phase 2)."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from ficha_etl import canonical, registry


def _row(**overrides: str | None) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        column: "" for column in registry.ESTABELECIMENTO_COLUMNS
    }
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


def test_write_part_types_dates_preserves_strings_and_persists_evidence(tmp_path):
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
                   data_situacao_especial, cep, municipio,
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
    assert manifest["validation"]["duplicate_primary_key_rows"] == 0
    assert manifest["casts_invalid"] == {
        "data_situacao_cadastral": 1,
        "data_inicio_atividade": 1,
        "data_situacao_especial": 0,
    }
    assert manifest["metrics"]["stages"][0]["casts_invalid"] == 2
    assert manifest["metrics"]["stages"][0]["quarantine_rows"] == 0
    assert manifest["output"]["rows"] == 2
    assert manifest["output"]["codec"] == "ZSTD"
    assert manifest["output"]["row_group_size"] == 200_000


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

    _write_csv(
        csv_path,
        [
            _row(data_inicio_atividade="20260719"),
            _row(cnpj_basico="00000002", cnpj_dv="72"),
        ],
    )
    third = canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
    )
    assert third.reused is False
    assert third.manifest["validation"]["output_rows"] == 2
    assert third.manifest["source"]["sha256"] != first.manifest["source"]["sha256"]


def test_duplicate_primary_key_fails_before_publishing_any_checkpoint(tmp_path):
    csv_path = tmp_path / "duplicate.csv"
    output_path = tmp_path / "canonical.parquet"
    duplicate = _row(data_inicio_atividade="20260719")
    _write_csv(csv_path, [duplicate, duplicate])

    with pytest.raises(canonical.CanonicalValidationError, match="duplicate primary-key"):
        canonical.write_estabelecimento_part(
            csv_path,
            output_path,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )

    assert not output_path.exists()
    assert not output_path.with_suffix(".parquet.manifest.json").exists()
    assert not list(tmp_path.glob(".*.tmp*"))


def test_missing_primary_key_fails_and_temporary_files_are_cleaned(tmp_path):
    csv_path = tmp_path / "missing-key.csv"
    output_path = tmp_path / "canonical.parquet"
    _write_csv(csv_path, [_row(cnpj_basico="")])

    with pytest.raises(canonical.CanonicalValidationError, match="missing primary key"):
        canonical.write_estabelecimento_part(
            csv_path,
            output_path,
            source_file="Estabelecimentos0.zip",
            source_snapshot="2026-07",
        )

    assert not output_path.exists()
    assert not output_path.with_suffix(".parquet.manifest.json").exists()
    assert not list(tmp_path.glob(".*.tmp*"))


def test_temporary_files_share_destination_filesystem_for_atomic_rename(
    tmp_path, monkeypatch
):
    csv_path = tmp_path / "source.csv"
    output_path = tmp_path / "out" / "canonical.parquet"
    work_dir = tmp_path / "separate-work-tree"
    _write_csv(csv_path, [_row()])

    original_replace = canonical.os.replace
    replacements: list[tuple[Path, Path]] = []

    def recording_replace(source, destination):
        source_path = Path(source)
        destination_path = Path(destination)
        replacements.append((source_path, destination_path))
        assert source_path.parent.stat().st_dev == destination_path.parent.stat().st_dev
        original_replace(source, destination)

    monkeypatch.setattr(canonical.os, "replace", recording_replace)
    canonical.write_estabelecimento_part(
        csv_path,
        output_path,
        source_file="Estabelecimentos0.zip",
        source_snapshot="2026-07",
        work_dir=work_dir,
    )

    assert len(replacements) == 2
    assert all(source.parent == destination.parent for source, destination in replacements)


def test_input_contract_rejects_invalid_snapshot_codec_and_row_group(tmp_path):
    csv_path = tmp_path / "source.csv"
    _write_csv(csv_path, [_row()])

    with pytest.raises(ValueError, match="YYYY-MM"):
        canonical.write_estabelecimento_part(
            csv_path,
            tmp_path / "out.parquet",
            source_file="Estabelecimentos0.zip",
            source_snapshot="July-2026",
        )
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
