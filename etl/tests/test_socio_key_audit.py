"""Socio row-identity/cardinality investigation tests (#97 slice 5).

Two groups, same split as test_estabelecimento_key_audit.py:
  a) global aggregation logic -- built directly against small synthetic
     all-columns Parquets via DuckDB, no ZIP/CSV/download involved;
  b) per-part checkpoint orchestration -- tiny synthetic ZIPs, covering
     reuse/tampering/failure evidence and a full ten-part offline run.
"""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from ficha_etl import registry
from ficha_etl import socio_key_audit as key_audit

# -----------------------------------------------------------------------------
# a) global aggregation
# -----------------------------------------------------------------------------


def _write_socio_parquet(
    con: duckdb.DuckDBPyConnection, path: Path, rows: list[dict[str, str]]
) -> None:
    columns_sql = ", ".join(
        f"{registry.quote_identifier(name)} VARCHAR" for name in registry.SOCIO_COLUMNS
    )
    con.execute(f'CREATE OR REPLACE TABLE _fixture ({columns_sql}, "_source_file" VARCHAR)')
    placeholders = ", ".join("?" for _ in range(len(registry.SOCIO_COLUMNS) + 1))
    con.executemany(
        f"INSERT INTO _fixture VALUES ({placeholders})",
        [[row[name] for name in registry.SOCIO_COLUMNS] + [row["_source_file"]] for row in rows],
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY _fixture TO '{path}' (FORMAT PARQUET)")
    con.execute("DROP TABLE _fixture")


def _socio_row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(registry.SOCIO_COLUMNS, "")
    row.update(
        cnpj_basico="00000001",
        identificador_socio="2",
        nome_socio_razao_social="FULANO DE TAL",
        cnpj_cpf_socio="***111111**",
        qualificacao_socio="49",
        data_entrada_sociedade="20200101",
        pais="",
        representante_legal="",
        nome_representante_legal="",
        qualificacao_representante_legal="00",
        faixa_etaria="5",
    )
    row.update(overrides)
    row.setdefault("_source_file", "Socios0.zip")
    return row


def test_no_duplicates_for_any_candidate(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(cnpj_basico="00000001", cnpj_cpf_socio="***111111**"),
                _socio_row(cnpj_basico="00000002", cnpj_cpf_socio="***222222**"),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    assert report["total_rows_scanned"] == 2
    for name in key_audit._CANDIDATE_KEYS:  # noqa: SLF001
        candidate = report["candidate_keys"][name]
        assert candidate["distinct_valid_key_count"] == 2, name
        assert candidate["duplicate_key_count"] == 0, name
        assert candidate["conflicting_key_count"] == 0, name


def test_exact_duplicate_is_duplicate_not_conflicting(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row()
        _write_socio_parquet(con, p0, [row, dict(row)])  # byte-identical duplicate
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    narrow = report["candidate_keys"]["cnpj_basico_socio"]
    assert narrow["distinct_valid_key_count"] == 1
    assert narrow["duplicate_key_count"] == 1
    assert narrow["excess_duplicate_row_count"] == 1
    assert narrow["conflicting_key_count"] == 0  # same payload, not a conflict


def test_conflicting_duplicate_across_parts_is_cross_part_and_conflicting(tmp_path):
    """Same (cnpj_basico, cnpj_cpf_socio) in two different parts, but a
    different qualificacao_socio -- a real conflict at the narrow candidate,
    which the wider candidates (that include qualificacao_socio in the key
    itself) resolve by simply treating them as different keys instead."""
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        p5 = tmp_path / "part-5.parquet"
        _write_socio_parquet(
            con, p0, [_socio_row(qualificacao_socio="49", _source_file="Socios0.zip")]
        )
        _write_socio_parquet(
            con, p5, [_socio_row(qualificacao_socio="99", _source_file="Socios5.zip")]
        )
        report = key_audit.run_global_key_audit(con, [p0, p5])
    finally:
        con.close()

    narrow = report["candidate_keys"]["cnpj_basico_socio"]
    assert narrow["distinct_valid_key_count"] == 1
    assert narrow["duplicate_key_count"] == 1
    assert narrow["cross_part_duplicate_key_count"] == 1
    assert narrow["conflicting_key_count"] == 1
    assert narrow["evidence_sample"] == [
        {
            "cnpj_basico": "00000001",
            "cnpj_cpf_socio": "***111111**",
            "count": 2,
            "source_files": ["Socios0.zip", "Socios5.zip"],
        }
    ]

    wider = report["candidate_keys"]["cnpj_basico_socio_identificador_qualificacao"]
    assert wider["distinct_valid_key_count"] == 2  # qualificacao_socio differs -> different keys
    assert wider["duplicate_key_count"] == 0
    assert wider["conflicting_key_count"] == 0


def test_blank_cnpj_cpf_socio_excluded_as_key_integrity_failure(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(cnpj_basico="00000001", cnpj_cpf_socio="***111111**"),
                _socio_row(cnpj_basico="00000002", cnpj_cpf_socio=""),  # blank -- integrity failure
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    narrow = report["candidate_keys"]["cnpj_basico_socio"]
    assert narrow["blank_or_null_counts_by_component"]["cnpj_cpf_socio"] == 1
    assert narrow["distinct_valid_key_count"] == 1  # the blank row is excluded entirely


def test_optional_columns_blank_does_not_exclude_wide_candidate(tmp_path):
    """`pais`/`representante_legal` are legitimately blank for most real
    rows (domestic partners, no legal representative) -- a wide candidate
    that includes them must NOT treat that as a key-integrity failure the
    way a blank cnpj_basico/cnpj_cpf_socio would. Only the two identity
    columns gate validity; pais/representante_legal blank is just a normal
    value that participates in the comparison.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [_socio_row(pais="", representante_legal="")],  # both blank, as in most real rows
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    wide = report["candidate_keys"]["full_row_minus_names"]
    assert wide["distinct_valid_key_count"] == 1  # NOT zero
    assert wide["blank_or_null_counts_by_component"]["pais"] == 1  # still reported as a diagnostic
    assert wide["blank_or_null_counts_by_component"]["representante_legal"] == 1


def test_evidence_sample_only_collected_for_sampled_candidates(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row()
        _write_socio_parquet(con, p0, [row, dict(row)])
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    for name, candidate in report["candidate_keys"].items():
        if name in key_audit._SAMPLED_CANDIDATE_KEYS:  # noqa: SLF001
            assert candidate["evidence_sample"], name
        else:
            assert candidate["evidence_sample"] == [], name


# -----------------------------------------------------------------------------
# b) per-part checkpoint orchestration
# -----------------------------------------------------------------------------


def _row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(registry.SOCIO_COLUMNS, "")
    row.update(
        cnpj_basico="00000001",
        identificador_socio="2",
        nome_socio_razao_social="FULANO DE TAL",
        cnpj_cpf_socio="***111111**",
        qualificacao_socio="49",
        data_entrada_sociedade="20200101",
        qualificacao_representante_legal="00",
        faixa_etaria="5",
    )
    row.update(overrides)
    return row


def _zip_bytes(rows: list[dict[str, str]], *, extra_file: bool = False) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator="\n"
    )
    writer.writerows([[row[name] for name in registry.SOCIO_COLUMNS] for row in rows])
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("K3241.K03200Y0.D60719.SOCIOCSV", buffer.getvalue().encode("latin-1"))
        if extra_file:
            archive.writestr("unexpected.txt", b"extra")
    return output.getvalue()


def _write_zip(path: Path, rows: list[dict[str, str]], *, extra_file: bool = False) -> None:
    path.write_bytes(_zip_bytes(rows, extra_file=extra_file))


def test_socio_remote_uses_historical_ia_raw_path():
    remote = key_audit.socio_remote("2026-04", 3)
    assert remote.name == "Socios3.zip"
    assert remote.kind == "socios"
    assert remote.url.endswith("/ficha-2026-04/raw/Socios3.zip")

    with pytest.raises(ValueError, match="YYYY-MM"):
        key_audit.socio_remote("April-2026", 0)
    with pytest.raises(ValueError, match="between 0 and 9"):
        key_audit.socio_remote("2026-04", 10)


def test_part_checkpoint_reuse(tmp_path, monkeypatch):
    zip_path = tmp_path / "fixture.zip"
    root = tmp_path / "run"
    _write_zip(zip_path, [_row(), _row(cnpj_cpf_socio="***222222**")])

    first = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    assert first.reused is False
    manifest = json.loads(first.manifest_path.read_text())
    assert manifest["status"] == "ok"
    assert manifest["source"]["name"] == "Socios0.zip"
    report = json.loads(first.report_path.read_text())
    assert report["rows_raw"] == 2
    assert not (root / "extracted").exists()  # big extracted CSV cleaned up
    assert any((root / "raw").glob("*.zip"))  # ZIP retained -- needed for checkpoint reuse

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("part audit ran despite a valid checksummed checkpoint")

    monkeypatch.setattr(key_audit, "run_part_key_audit_with_metrics", should_not_run)
    second = key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=zip_path)
    assert second.reused is True
    assert second.manifest == manifest


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
        lambda: {**original_fingerprints(), "socio_key_audit": "changed"},
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
    assert failure["source"]["name"] == "Socios0.zip"
    assert len(failure["source"]["zip"]["sha256"]) == 64
    assert "expected exactly one" in failure["error"]


def test_missing_zip_fails_with_durable_evidence(tmp_path):
    root = tmp_path / "run"
    missing = tmp_path / "does-not-exist.zip"

    with pytest.raises(FileNotFoundError):
        key_audit.run_part_checkpoint("2026-04", 0, root, zip_override=missing)

    failure = json.loads((root / "evidence" / "part-0.key-audit.failure.json").read_text())
    assert failure["status"] == "failed"


def test_full_ten_part_offline_run_differentiates_candidate_keys(tmp_path):
    """End-to-end offline correctness check mirroring the real investigation
    shape: part 0 has an exact within-part duplicate, part 0 and part 5
    share a cross-part CONFLICTING duplicate (same narrow key, different
    qualificacao_socio) that a wider candidate resolves by treating as
    different keys, and one row has a blank cnpj_cpf_socio. Not a
    substitute for the real snapshot run.
    """
    root = tmp_path / "run"
    overrides: dict[int, Path] = {}
    for part in range(10):
        zip_path = tmp_path / f"Socios{part}.zip"
        if part == 0:
            _write_zip(
                zip_path,
                [
                    _row(
                        cnpj_basico="00000001",
                        cnpj_cpf_socio="***111111**",
                        qualificacao_socio="49",
                    ),
                    _row(cnpj_basico="00000002", cnpj_cpf_socio="***222222**"),
                    _row(cnpj_basico="00000002", cnpj_cpf_socio="***222222**"),  # exact dup
                ],
            )
        elif part == 5:
            _write_zip(
                zip_path,
                [
                    _row(
                        cnpj_basico="00000001",
                        cnpj_cpf_socio="***111111**",
                        qualificacao_socio="99",
                    ),
                    _row(cnpj_basico="00000003", cnpj_cpf_socio=""),  # blank key component
                ],
            )
        else:
            _write_zip(zip_path, [])
        overrides[part] = zip_path

    result = key_audit.run_key_audit("2026-04", root, zip_overrides=overrides)
    report = result.report

    assert report["total_rows_scanned"] == 5
    assert len(report["parts"]) == 10

    narrow = report["candidate_keys"]["cnpj_basico_socio"]
    assert narrow["distinct_valid_key_count"] == 2
    assert narrow["duplicate_key_count"] == 2  # both (1,111111) and (2,222222) recur
    assert narrow["conflicting_key_count"] == 1  # only (1,111111): differing qualificacao_socio
    assert narrow["cross_part_duplicate_key_count"] == 1
    assert narrow["blank_or_null_counts_by_component"]["cnpj_cpf_socio"] == 1

    with_qualificacao = report["candidate_keys"]["cnpj_basico_socio_identificador_qualificacao"]
    assert with_qualificacao["distinct_valid_key_count"] == 3
    assert with_qualificacao["duplicate_key_count"] == 1  # only the exact (2,222222) pair remains
    assert with_qualificacao["conflicting_key_count"] == 0

    # Disk lifecycle: ZIPs retained (checkpoint reuse), extracted CSVs cleaned up.
    assert len(list((root / "raw").glob("*.zip"))) == 10
    assert not (root / "extracted").exists()


def test_cli_rejects_malformed_zip_override(tmp_path):
    result = key_audit.main(
        ["--month", "2026-04", "--root", str(tmp_path / "run"), "--zip", "not-a-valid-entry"]
    )
    assert result == 2


def test_cli_rejects_duplicate_part_override(tmp_path):
    zip_path = tmp_path / "fixture.zip"
    _write_zip(zip_path, [_row()])
    result = key_audit.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(tmp_path / "run"),
            "--zip",
            f"0={zip_path}",
            "--zip",
            f"0={zip_path}",
        ]
    )
    assert result == 2


def test_cli_rejects_missing_override_file(tmp_path):
    result = key_audit.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(tmp_path / "run"),
            "--zip",
            f"0={tmp_path / 'does-not-exist.zip'}",
        ]
    )
    assert result == 2
