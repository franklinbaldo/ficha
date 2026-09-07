from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from ficha_etl.monthly_estabelecimento_gate import (
    MonthlyEstabelecimentoKeyGateError,
    run_monthly_gate,
)


def _write_cnpjs(path: Path, values: list[str]) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE rows(cnpj VARCHAR)")
        con.executemany("INSERT INTO rows VALUES (?)", [(value,) for value in values])
        con.execute(
            "COPY rows TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(path)],
        )
    finally:
        con.close()


def test_monthly_gate_accepts_unique_production_bytes_and_records_identity(tmp_path: Path) -> None:
    cnpjs = tmp_path / "cnpjs.parquet"
    evidence = tmp_path / "evidence.json"
    _write_cnpjs(cnpjs, ["12345678000190", "12345678000270", "87654321000110"])

    payload = run_monthly_gate("2026-08", cnpjs, evidence, tmp_path / "work")

    assert payload["status"] == "ok"
    assert payload["source_rows"] == 3
    assert payload["audit"]["distinct_valid_full_keys"] == 3
    assert payload["audit"]["duplicate_key_count"] == 0
    assert payload["adapter"]["audit_engine"] == "estabelecimento_key_audit.run_global_key_audit"
    assert payload["adapter"]["network_reads"] == 0
    assert payload["input"]["sha256"]
    assert json.loads(evidence.read_text(encoding="utf-8"))["status"] == "ok"


def test_monthly_gate_rejects_duplicate_that_survives_cross_part_merge(tmp_path: Path) -> None:
    """Two source parts with the same full key become two equal produced CNPJs.

    The source-part smoke already lives in test_estabelecimento_key_audit; this
    integration test proves that the monthly adapter keeps that duplicate
    observable after the producer has merged away part lineage and fails before
    publication.
    """
    cnpjs = tmp_path / "cnpjs.parquet"
    evidence = tmp_path / "evidence.json"
    duplicated = "12345678000190"
    _write_cnpjs(cnpjs, [duplicated, "87654321000110", duplicated])

    with pytest.raises(MonthlyEstabelecimentoKeyGateError, match="1 duplicate full"):
        run_monthly_gate("2026-08", cnpjs, evidence, tmp_path / "work")

    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["audit"]["duplicate_key_count"] == 1
    assert payload["audit"]["excess_duplicate_row_count"] == 1


def test_monthly_gate_rejects_malformed_published_cnpj(tmp_path: Path) -> None:
    cnpjs = tmp_path / "cnpjs.parquet"
    evidence = tmp_path / "evidence.json"
    _write_cnpjs(cnpjs, ["12345678000190", "short"])

    with pytest.raises(MonthlyEstabelecimentoKeyGateError, match="malformed published CNPJ"):
        run_monthly_gate("2026-08", cnpjs, evidence, tmp_path / "work")

    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["invalid_cnpj_rows"] == 1
