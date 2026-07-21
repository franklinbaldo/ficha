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


def test_category_row_counts_sum_to_total(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(identificador_socio="1", cnpj_cpf_socio="12345678000199"),
                _socio_row(identificador_socio="2", cnpj_cpf_socio="***111111**"),
                _socio_row(identificador_socio="3", cnpj_cpf_socio="", pais="105"),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    assert report["total_rows_scanned"] == 3
    categories = report["categories"]
    assert categories["pessoa_juridica"]["row_count"] == 1
    assert categories["pessoa_fisica"]["row_count"] == 1
    assert categories["socio_estrangeiro"]["row_count"] == 1
    assert sum(c["row_count"] for c in categories.values()) == 3


def test_pf_no_duplicates_for_any_candidate(tmp_path):
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

    pf = report["categories"]["pessoa_fisica"]
    for candidates in (pf["identity_candidates"], pf["relationship_candidates"]):
        for name, candidate in candidates.items():
            assert candidate["distinct_valid_key_count"] == 2, name
            assert candidate["duplicate_key_count"] == 0, name
            assert candidate["conflicting_key_count"] == 0, name


def test_pf_exact_duplicate_is_duplicate_not_conflicting(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row()
        _write_socio_parquet(con, p0, [row, dict(row)])  # byte-identical duplicate
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    narrow = report["categories"]["pessoa_fisica"]["identity_candidates"]["pf:cpf_nome"]
    assert narrow["distinct_valid_key_count"] == 1
    assert narrow["duplicate_key_count"] == 1
    assert narrow["excess_duplicate_row_count"] == 1
    assert narrow["conflicting_key_count"] == 0  # same payload, not a conflict


def test_pf_conflicting_duplicate_across_parts_resolved_by_qualificacao(tmp_path):
    """Same company + masked CPF + name in two different parts, but a
    different qualificacao_socio -- a real conflict at `pf:company_partner`
    (company + category identity, no role yet), which
    `pf:company_partner_qualificacao` resolves by treating them as
    different keys once the role is part of the key."""
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

    relationship = report["categories"]["pessoa_fisica"]["relationship_candidates"]
    narrow = relationship["pf:company_partner"]
    assert narrow["distinct_valid_key_count"] == 1
    assert narrow["duplicate_key_count"] == 1
    assert narrow["cross_part_duplicate_key_count"] == 1
    assert narrow["conflicting_key_count"] == 1

    wider = relationship["pf:company_partner_qualificacao"]
    assert wider["distinct_valid_key_count"] == 2  # qualificacao_socio differs -> different keys
    assert wider["duplicate_key_count"] == 0
    assert wider["conflicting_key_count"] == 0


def test_pf_masked_cpf_collision_between_different_people_resolved_by_name(tmp_path):
    """RFB masks cnpj_cpf_socio down to its middle six digits -- two
    genuinely DIFFERENT people can share the same masked value. Simulated
    here as two rows with the same masked CPF but different names and a
    different qualificacao_socio: the CPF-only candidate wrongly reports
    this as one conflicting key (as if it were the same partner switching
    roles); the name-augmented candidate correctly recognizes two distinct
    people and reports zero conflict.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    cnpj_cpf_socio="***111111**",
                    nome_socio_razao_social="FULANO DE TAL",
                    qualificacao_socio="49",
                ),
                _socio_row(
                    cnpj_cpf_socio="***111111**",
                    nome_socio_razao_social="BELTRANO OUTRO",  # different person, same masked CPF
                    qualificacao_socio="99",
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    identity = report["categories"]["pessoa_fisica"]["identity_candidates"]
    cpf_only = identity["pf:cpf"]
    assert cpf_only["distinct_valid_key_count"] == 1
    assert cpf_only["duplicate_key_count"] == 1
    assert cpf_only["conflicting_key_count"] == 1  # wrongly looks like one conflicting partner

    with_name = identity["pf:cpf_nome"]
    assert with_name["distinct_valid_key_count"] == 2  # correctly two different people
    assert with_name["duplicate_key_count"] == 0
    assert with_name["conflicting_key_count"] == 0


def test_pf_blank_cnpj_cpf_socio_excluded_as_key_integrity_failure(tmp_path):
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

    narrow = report["categories"]["pessoa_fisica"]["identity_candidates"]["pf:cpf"]
    assert narrow["blank_or_null_counts_by_component"]["cnpj_cpf_socio"] == 1
    assert narrow["distinct_valid_key_count"] == 1  # the blank row is excluded entirely


def test_pf_diagnostics_same_cpf_different_name_and_faixa(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    cnpj_cpf_socio="***111111**",
                    nome_socio_razao_social="FULANO DE TAL",
                    faixa_etaria="5",
                ),
                # same masked CPF, different normalized name
                _socio_row(
                    cnpj_cpf_socio="***111111**",
                    nome_socio_razao_social="BELTRANO OUTRO",
                    faixa_etaria="5",
                ),
                # same masked CPF and name, different faixa_etaria
                _socio_row(
                    cnpj_cpf_socio="***111111**",
                    nome_socio_razao_social="FULANO DE TAL",
                    faixa_etaria="6",
                    cnpj_basico="00000002",
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    diagnostics = report["categories"]["pessoa_fisica"]["diagnostics"]
    assert diagnostics["same_masked_cpf_different_normalized_name_count"] == 1
    assert diagnostics["same_masked_cpf_and_name_different_faixa_etaria_count"] == 1


def test_pj_cnpj_alone_matches_cnpj_plus_name_when_names_are_consistent(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    identificador_socio="1",
                    cnpj_cpf_socio="12345678000199",
                    nome_socio_razao_social="EMPRESA A LTDA",
                ),
                _socio_row(
                    identificador_socio="1",
                    cnpj_cpf_socio="98765432000155",
                    nome_socio_razao_social="EMPRESA B LTDA",
                    cnpj_basico="00000002",
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    pj = report["categories"]["pessoa_juridica"]
    cnpj_only = pj["identity_candidates"]["pj:cnpj"]
    cnpj_and_name = pj["identity_candidates"]["pj:cnpj_nome"]
    assert cnpj_only["distinct_valid_key_count"] == 2
    assert cnpj_and_name["distinct_valid_key_count"] == 2  # name adds nothing here
    assert pj["diagnostics"]["name_resolves_collision_beyond_valid_cnpj"] is False


def test_pj_diagnostics_cnpj_format_and_same_cnpj_different_name(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    identificador_socio="1",
                    cnpj_cpf_socio="12345678000199",  # valid: 14 digits, all numeric
                    nome_socio_razao_social="EMPRESA A LTDA",
                ),
                # same CNPJ republished with a different normalized name -- a
                # consistency diagnostic, not evidence name belongs in the key
                _socio_row(
                    identificador_socio="1",
                    cnpj_cpf_socio="12345678000199",
                    nome_socio_razao_social="EMPRESA A LTDA - RENOMEADA",
                ),
                # malformed: not 14 digits
                _socio_row(
                    identificador_socio="1",
                    cnpj_cpf_socio="123",
                    nome_socio_razao_social="EMPRESA C",
                    cnpj_basico="00000003",
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    diagnostics = report["categories"]["pessoa_juridica"]["diagnostics"]
    assert diagnostics["cnpj_format_valid_count"] == 2
    assert diagnostics["cnpj_format_invalid_count"] == 1
    assert diagnostics["same_cnpj_different_normalized_name_count"] == 1


def test_foreign_partner_has_no_cnpj_cpf_and_is_not_excluded(tmp_path):
    """identificador_socio='3' rows always have a blank cnpj_cpf_socio --
    this is the entire foreign-partner category, not a key-integrity
    failure, and must not exclude the row from that category's candidates
    (which never reference cnpj_cpf_socio in the first place)."""
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    identificador_socio="3",
                    cnpj_cpf_socio="",
                    nome_socio_razao_social="JOHN SMITH",
                    pais="249",
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    foreign = report["categories"]["socio_estrangeiro"]
    assert foreign["row_count"] == 1
    nome_pais = foreign["identity_candidates"]["foreign:nome_pais"]
    assert nome_pais["distinct_valid_key_count"] == 1  # NOT excluded/zero
    # cnpj_cpf_socio isn't part of this candidate's columns at all, so it's
    # never measured (let alone treated as a key-integrity failure).
    assert "cnpj_cpf_socio" not in nome_pais["blank_or_null_counts_by_component"]


def test_foreign_partner_null_pais_evidence_sample_not_dropped_by_null_safe_join(tmp_path):
    """Regression: `pais` is blank/NULL for ~0.6% of real foreign-partner
    rows. A plain `=` join between the duplicate-key aggregate and the raw
    rows silently drops NULL-valued groups (`NULL = NULL` is not TRUE),
    producing an empty evidence_sample even though duplicate_key_count is
    correctly non-zero. `IS NOT DISTINCT FROM` must be used instead.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row(
            identificador_socio="3",
            cnpj_cpf_socio="",
            nome_socio_razao_social="JOHN SMITH",
            pais="",  # blank -> NULL after CSV load in the real pipeline; NULL here too
        )
        _write_socio_parquet(con, p0, [row, dict(row)])
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    nome_pais = report["categories"]["socio_estrangeiro"]["identity_candidates"][
        "foreign:nome_pais"
    ]
    assert nome_pais["duplicate_key_count"] == 1
    assert nome_pais["evidence_sample"], "NULL-valued pais silently dropped this duplicate group"
    assert nome_pais["evidence_sample"][0]["count"] == 2


def test_representante_independence_flags_variation_within_relationship_group(tmp_path):
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(representante_legal="11111111111", nome_representante_legal="REP A"),
                # same relationship key (cnpj_basico, cpf, name, qualificacao, data_entrada)
                # but a different legal representative
                _socio_row(representante_legal="22222222222", nome_representante_legal="REP B"),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    independence = report["categories"]["pessoa_fisica"]["diagnostics"][
        "representante_independence"
    ]
    assert independence["duplicate_relationship_groups"] == 1
    assert independence["groups_with_representante_variation"] == 1


def test_optional_wide_column_blank_does_not_exclude_rows_from_candidate(tmp_path):
    """Direct unit test of `_audit_one_candidate`'s gating logic: a
    candidate column outside `_KEY_INTEGRITY_COLUMNS` (here `pais`) must
    not be treated as a key-integrity failure when blank -- only reported
    as a diagnostic. Guards the bug this module's docstring describes
    (a wide candidate wrongly zeroing out distinct_valid_key_count because
    every row's optional column happened to be blank). `source` must carry
    every raw SOCIO_COLUMNS plus `_row_hash` -- `_audit_one_candidate`'s
    conflict check always compares it -- so this goes through
    `_build_socio_base` rather than a minimal ad hoc table.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(con, p0, [_socio_row(pais="")])  # blank, as in most real rows
        source = key_audit._build_socio_base(  # noqa: SLF001
            con, registry.paths_literal([p0])
        )
        report = key_audit._audit_one_candidate(  # noqa: SLF001
            con,
            source,
            "probe",
            ("cnpj_basico", "cnpj_cpf_socio", "pais"),
            collect_sample=False,
        )
    finally:
        con.close()

    assert report.distinct_valid_key_count == 1  # NOT zero
    assert report.blank_or_null_counts_by_component["pais"] == 1  # still reported as a diagnostic


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

    # All five rows use identificador_socio="2" (the _row() default) -> pessoa_fisica.
    pf = report["categories"]["pessoa_fisica"]
    assert pf["row_count"] == 5

    narrow = pf["identity_candidates"]["pf:cpf"]
    assert narrow["distinct_valid_key_count"] == 2
    assert narrow["duplicate_key_count"] == 2  # both ***111111** and ***222222** recur
    assert narrow["conflicting_key_count"] == 1  # only ***111111**: differing qualificacao_socio
    assert narrow["cross_part_duplicate_key_count"] == 1
    assert narrow["blank_or_null_counts_by_component"]["cnpj_cpf_socio"] == 1

    # Both rows sharing ***111111** use the SAME name (the _row() default) --
    # unlike the masked-CPF-collision test, this genuinely is one person
    # switching roles, so adding the name must NOT spuriously resolve this
    # real conflict at the company_partner level.
    company_partner = pf["relationship_candidates"]["pf:company_partner"]
    assert company_partner["distinct_valid_key_count"] == 2
    assert company_partner["conflicting_key_count"] == 1

    with_qualificacao = pf["relationship_candidates"]["pf:company_partner_qualificacao"]
    assert with_qualificacao["distinct_valid_key_count"] == 3
    assert with_qualificacao["duplicate_key_count"] == 1  # only the exact ***222222** pair remains
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
