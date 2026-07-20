"""Cross-part establishment key uniqueness audit tests (issue #100).

Two groups:
  a) global aggregation logic -- built directly against small key-only
     Parquets via DuckDB, no ZIP/CSV/download involved;
  b) per-part checkpoint orchestration -- tiny synthetic ZIPs, mirroring
     test_canonical_history.py's fixture pattern, covering reuse/tampering/
     failure evidence and the full ten-part offline flow.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import estabelecimento_key_audit as key_audit
from ficha_etl import registry

# -----------------------------------------------------------------------------
# a) global aggregation
# -----------------------------------------------------------------------------


def _write_key_parquet(
    con: duckdb.DuckDBPyConnection, path: Path, rows: list[tuple[str, str, str, str]]
) -> None:
    con.execute(
        'CREATE OR REPLACE TABLE _fixture ("cnpj_basico" VARCHAR, "cnpj_ordem" VARCHAR, '
        '"cnpj_dv" VARCHAR, "_source_file" VARCHAR)'
    )
    con.executemany("INSERT INTO _fixture VALUES (?, ?, ?, ?)", rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY _fixture TO '{path}' (FORMAT PARQUET)")
    con.execute("DROP TABLE _fixture")


def test_no_duplicates_across_parts(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        p1 = tmp_path / "part-1.parquet"
        _write_key_parquet(con, p0, [("00000001", "0001", "91", "Estabelecimentos0.zip")])
        _write_key_parquet(con, p1, [("00000002", "0001", "92", "Estabelecimentos1.zip")])

        report = key_audit.run_global_key_audit(con, [p0, p1])
    finally:
        con.close()

    assert report.total_rows_scanned == 2
    assert report.distinct_valid_full_keys == 2
    assert report.duplicate_key_count == 0
    assert report.excess_duplicate_row_count == 0
    assert report.cross_part_duplicate_key_count == 0
    assert report.evidence_sample == []


def test_same_key_in_two_different_parts_is_cross_part(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        p1 = tmp_path / "part-1.parquet"
        _write_key_parquet(con, p0, [("00000001", "0001", "91", "Estabelecimentos0.zip")])
        _write_key_parquet(con, p1, [("00000001", "0001", "91", "Estabelecimentos1.zip")])

        report = key_audit.run_global_key_audit(con, [p0, p1])
    finally:
        con.close()

    assert report.distinct_valid_full_keys == 1
    assert report.duplicate_key_count == 1
    assert report.excess_duplicate_row_count == 1
    assert report.cross_part_duplicate_key_count == 1
    assert report.evidence_sample == [
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "count": 2,
            "source_files": ["Estabelecimentos0.zip", "Estabelecimentos1.zip"],
        }
    ]


def test_duplicate_within_one_part_is_not_cross_part(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_key_parquet(
            con,
            p0,
            [
                ("00000001", "0001", "91", "Estabelecimentos0.zip"),
                ("00000001", "0001", "91", "Estabelecimentos0.zip"),
            ],
        )

        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    assert report.duplicate_key_count == 1
    assert report.excess_duplicate_row_count == 1
    assert report.cross_part_duplicate_key_count == 0
    assert report.evidence_sample[0]["source_files"] == ["Estabelecimentos0.zip"]


def test_triplicate_spanning_multiple_parts(tmp_path):
    """Two occurrences in part 0, one in part 2: n=3, n_parts=2 -- still one
    cross-part duplicate KEY, but excess rows counts every row beyond the
    first (2), not the part count."""
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        p2 = tmp_path / "part-2.parquet"
        _write_key_parquet(
            con,
            p0,
            [
                ("00000001", "0001", "91", "Estabelecimentos0.zip"),
                ("00000001", "0001", "91", "Estabelecimentos0.zip"),
            ],
        )
        _write_key_parquet(con, p2, [("00000001", "0001", "91", "Estabelecimentos2.zip")])

        report = key_audit.run_global_key_audit(con, [p0, p2])
    finally:
        con.close()

    assert report.duplicate_key_count == 1
    assert report.excess_duplicate_row_count == 2
    assert report.cross_part_duplicate_key_count == 1
    assert report.evidence_sample[0]["count"] == 3
    assert report.evidence_sample[0]["source_files"] == [
        "Estabelecimentos0.zip",
        "Estabelecimentos2.zip",
    ]


def test_blank_and_null_key_components_excluded_and_counted_separately(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_key_parquet(
            con,
            p0,
            [
                ("00000001", "0001", "91", "Estabelecimentos0.zip"),
                ("", "0001", "91", "Estabelecimentos0.zip"),
                ("00000002", "", "91", "Estabelecimentos0.zip"),
                ("00000003", "0001", None, "Estabelecimentos0.zip"),  # NULL, not blank string
            ],
        )

        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    # Blank/null-keyed rows never enter the duplicate analysis at all.
    assert report.distinct_valid_full_keys == 1
    assert report.duplicate_key_count == 0
    assert report.blank_or_null_counts_by_component == {
        "cnpj_basico": 1,
        "cnpj_ordem": 1,
        "cnpj_dv": 1,
    }


def test_duplicate_key_count_and_excess_row_count_are_distinct(tmp_path):
    """Three keys duplicated -- one 2x, one 3x, one 4x -- must report the
    distinct COUNT of duplicated keys (3) separately from the total excess
    rows (1 + 2 + 3 = 6), never conflating the two."""
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        rows = []
        rows += [("00000001", "0001", "91", "Estabelecimentos0.zip")] * 2
        rows += [("00000002", "0001", "92", "Estabelecimentos0.zip")] * 3
        rows += [("00000003", "0001", "93", "Estabelecimentos0.zip")] * 4
        rows += [("00000004", "0001", "94", "Estabelecimentos0.zip")]  # not a duplicate
        _write_key_parquet(con, p0, rows)

        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    assert report.distinct_valid_full_keys == 4
    assert report.duplicate_key_count == 3
    assert report.excess_duplicate_row_count == 6


# -----------------------------------------------------------------------------
# b) per-part checkpoint orchestration -- synthetic ZIPs
# -----------------------------------------------------------------------------


def _row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(registry.ESTABELECIMENTO_COLUMNS, "")
    row.update(
        {
            "cnpj_basico": "00000001",
            "cnpj_ordem": "0001",
            "cnpj_dv": "91",
            "identificador_matriz_filial": "1",
            "situacao_cadastral": "02",
            "data_situacao_cadastral": "20260719",
            "data_inicio_atividade": "19991231",
        }
    )
    row.update(overrides)
    return row


def _zip_bytes(rows: list[dict[str, str]], *, extra_file: bool = False) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    writer.writerows([[row[name] for name in registry.ESTABELECIMENTO_COLUMNS] for row in rows])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.ESTABELE", buffer.getvalue().encode("latin-1"))
        if extra_file:
            archive.writestr("unexpected.txt", b"extra")
    return output.getvalue()


def _write_zip(path: Path, rows: list[dict[str, str]], *, extra_file: bool = False) -> None:
    path.write_bytes(_zip_bytes(rows, extra_file=extra_file))


def test_part_checkpoint_reuse(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row(), _row(cnpj_ordem="0002")])

    first = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    assert first.reused is False
    manifest = json.loads(first.manifest_path.read_text())
    assert manifest["status"] == "ok"
    assert manifest["source"]["name"] == "Estabelecimentos0.zip"
    report = json.loads(first.report_path.read_text())
    assert report["rows_raw"] == 2
    assert report["within_part_duplicate_keys"] == 0
    assert not (root / "extracted").exists()

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("part audit ran despite a valid checksummed checkpoint")

    monkeypatch.setattr(key_audit, "run_part_key_audit_with_metrics", should_not_run)
    second = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    assert second.reused is True
    assert second.manifest == manifest


def test_tampered_checkpoint_output_invalidates_resume(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()])
    first = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    first.output_path.write_bytes(b"tampered")

    calls = 0
    original = key_audit.run_part_key_audit_with_metrics

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(key_audit, "run_part_key_audit_with_metrics", counted)
    second = key_audit.run_part_checkpoint("2026-04", 0, root)

    assert calls == 1
    assert second.reused is False
    assert second.manifest["source"]["acquisition"] == "local-cache"
    assert second.output_path.read_bytes() != b"tampered"


def test_code_fingerprint_change_invalidates_resume(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()])
    key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)

    calls = 0
    original = key_audit.run_part_key_audit_with_metrics

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(key_audit, "run_part_key_audit_with_metrics", counted)
    original_fingerprints = key_audit._code_fingerprints  # noqa: SLF001
    monkeypatch.setattr(
        key_audit,
        "_code_fingerprints",
        lambda: {**original_fingerprints(), "estabelecimento_key_audit": "changed"},
    )
    result = key_audit.run_part_checkpoint("2026-04", 0, root)

    assert result.reused is False
    assert calls == 1


def test_malformed_zip_fails_with_durable_evidence(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row()], extra_file=True)

    with pytest.raises(RuntimeError, match="expected exactly one extracted CSV"):
        key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)

    failure = json.loads((root / "evidence" / "part-0.key-audit.failure.json").read_text())
    assert failure["status"] == "failed"
    assert failure["source"]["name"] == "Estabelecimentos0.zip"
    assert len(failure["source"]["zip"]["sha256"]) == 64
    assert "expected exactly one" in failure["error"]


def test_missing_zip_fails_with_durable_evidence(tmp_path):
    root = tmp_path / "run"
    missing = tmp_path / "does-not-exist.zip"

    with pytest.raises(FileNotFoundError):
        key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=missing)

    failure = json.loads((root / "evidence" / "part-0.key-audit.failure.json").read_text())
    assert failure["status"] == "failed"


def test_full_ten_part_offline_run_reports_a_real_cross_part_duplicate(tmp_path):
    """End-to-end offline correctness check: ten tiny synthetic ZIPs, one
    key deliberately duplicated between part 0 and part 5, everything else
    unique. This is a synthetic-fixture correctness test for the tool
    itself -- not a substitute for the real snapshot run.
    """
    root = tmp_path / "run"
    overrides: dict[int, Path] = {}
    for part in range(10):
        zip_path = tmp_path / f"Estabelecimentos{part}.zip"
        if part in (0, 5):
            # Same full key in both part 0 and part 5 -- the deliberate
            # cross-part duplicate this test exists to catch.
            rows = [_row(cnpj_basico="00000001", cnpj_ordem="0001", cnpj_dv="91")]
        else:
            # Offset by 100 so no other part's key can collide with the
            # deliberate "00000001" duplicate above (part=1 would otherwise
            # also produce "00000001").
            rows = [_row(cnpj_basico=f"{part + 100:08d}", cnpj_ordem="0001", cnpj_dv="91")]
        _write_zip(zip_path, rows)
        overrides[part] = zip_path

    result = key_audit.run_key_audit("2026-04", root, zip_overrides=overrides)

    assert result.report["total_rows_scanned"] == 10
    assert result.report["distinct_valid_full_keys"] == 9
    assert result.report["duplicate_key_count"] == 1
    assert result.report["excess_duplicate_row_count"] == 1
    assert result.report["cross_part_duplicate_key_count"] == 1
    assert result.report["evidence_sample"][0]["source_files"] == [
        "Estabelecimentos0.zip",
        "Estabelecimentos5.zip",
    ]
    assert len(result.part_results) == 10
    assert result.report_path.exists()
    assert result.report["snapshot_month"] == "2026-04"
    assert result.report["duckdb_version"]
