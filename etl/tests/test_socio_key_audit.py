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
    for name, candidate in pf["identity_candidates"].items():
        assert candidate["distinct_valid_key_count"] == 2, name
        assert candidate["duplicate_key_count"] == 0, name
        assert candidate["conflicting_key_count"] is None, name  # not computed at identity level
    for name, candidate in pf["relationship_candidates"].items():
        assert candidate["distinct_valid_key_count"] == 2, name
        assert candidate["duplicate_key_count"] == 0, name
        assert candidate["conflicting_key_count"] == 0, name


def test_pf_exact_duplicate_is_duplicate_not_conflicting(tmp_path):
    """Real full-row comparison at the relationship level (company-scoped,
    where conflicting_key_count is actually computed -- see
    `_audit_one_candidate`'s `compute_conflicting`), not a hash: a
    byte-identical duplicate must report zero conflicts, proven by
    comparing every column, not by a probabilistic hash match."""
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row()
        _write_socio_parquet(con, p0, [row, dict(row)])  # byte-identical duplicate
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    identity = report["categories"]["pessoa_fisica"]["identity_candidates"]["pf:cpf_nome"]
    assert identity["distinct_valid_key_count"] == 1
    assert identity["duplicate_key_count"] == 1
    assert identity["conflicting_key_count"] is None  # not computed at identity level

    relationship = report["categories"]["pessoa_fisica"]["relationship_candidates"][
        "pf:company_partner"
    ]
    assert relationship["distinct_valid_key_count"] == 1
    assert relationship["duplicate_key_count"] == 1
    assert relationship["excess_duplicate_row_count"] == 1
    assert relationship["conflicting_key_count"] == 0  # same payload, not a conflict


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
    here as two rows with the same masked CPF but different names: the
    CPF-only identity candidate wrongly groups them into one duplicate
    key, and the dedicated `same_masked_cpf_different_normalized_name_count`
    diagnostic (not `conflicting_key_count`, which identity-level
    candidates don't compute -- see `_audit_one_candidate`) confirms the
    collision is exactly this masked-CPF-plus-different-name case; the
    name-augmented candidate correctly recognizes two distinct people.
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

    pf = report["categories"]["pessoa_fisica"]
    cpf_only = pf["identity_candidates"]["pf:cpf"]
    assert cpf_only["distinct_valid_key_count"] == 1
    assert cpf_only["duplicate_key_count"] == 1  # wrongly looks like one partner
    assert cpf_only["conflicting_key_count"] is None  # not computed at identity level

    with_name = pf["identity_candidates"]["pf:cpf_nome"]
    assert with_name["distinct_valid_key_count"] == 2  # correctly two different people
    assert with_name["duplicate_key_count"] == 0

    assert pf["diagnostics"]["same_masked_cpf_different_normalized_name_count"] == 1


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


def test_name_normalization_strips_accents(tmp_path):
    """RFB free text is not consistently accented across records -- the
    same real person's name can appear with and without diacritics.
    Without accent removal, `\"JOSÉ\"` and `\"JOSE\"` would wrongly compare
    as two different people; `_normalized_name_expr` uses `strip_accents()`
    so both map to the same normalized identity.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(nome_socio_razao_social="JOSÉ DA SILVA"),
                _socio_row(nome_socio_razao_social="JOSE DA SILVA", cnpj_basico="00000002"),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    with_name = report["categories"]["pessoa_fisica"]["identity_candidates"]["pf:cpf_nome"]
    assert with_name["distinct_valid_key_count"] == 1  # accented/unaccented spellings collapse
    assert with_name["duplicate_key_count"] == 1


def test_pf_relationship_with_faixa_is_measured_but_not_recommended(tmp_path):
    """`faixa_etaria` is measured at the relationship level for
    comparison (`pf:relationship_with_faixa`) but is NOT part of the
    recommended `pf:relationship` candidate: two rows sharing the exact
    recommended relationship key but differing only in age bracket must
    still collapse to one duplicate/conflicting pair at `pf:relationship`
    (age bracket does not define a separate relationship), while
    `pf:relationship_with_faixa` splits them into two distinct keys
    purely as a measurement of what including it WOULD do.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(faixa_etaria="5"),
                _socio_row(faixa_etaria="6"),  # same relationship key, different age bracket
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    relationship = report["categories"]["pessoa_fisica"]["relationship_candidates"]
    recommended = relationship["pf:relationship"]
    assert recommended["distinct_valid_key_count"] == 1
    assert recommended["duplicate_key_count"] == 1
    assert recommended["conflicting_key_count"] == 1  # faixa_etaria genuinely differs

    with_faixa = relationship["pf:relationship_with_faixa"]
    assert (
        with_faixa["distinct_valid_key_count"] == 2
    )  # measured, not folded into the recommendation
    assert with_faixa["duplicate_key_count"] == 0


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


def test_foreign_partner_conflict_is_preserved_not_resolved_by_representante(tmp_path):
    """Two foreign-partner rows sharing the full recommended
    `foreign:relationship` key (company + normalized name + pais +
    qualificacao_socio + data_entrada_sociedade) but with different
    `representante_legal` must still be reported as duplicate AND
    conflicting -- the conflict is measured and preserved, not silently
    resolved by folding `representante_legal` into the identity. Only
    `representante_independence` (a separate diagnostic) surfaces that the
    representative varies; it never changes what `foreign:relationship`'s
    own columns are.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    identificador_socio="3",
                    cnpj_cpf_socio=None,
                    nome_socio_razao_social="JOHN SMITH",
                    pais="249",
                    representante_legal="11111111111",
                ),
                _socio_row(
                    identificador_socio="3",
                    cnpj_cpf_socio=None,
                    nome_socio_razao_social="JOHN SMITH",
                    pais="249",
                    representante_legal="22222222222",  # only this differs
                ),
            ],
        )
        report = key_audit.run_global_key_audit(con, [p0])
    finally:
        con.close()

    foreign = report["categories"]["socio_estrangeiro"]
    relationship = foreign["relationship_candidates"]["foreign:relationship"]
    assert "representante_legal" not in relationship["columns"]
    assert relationship["distinct_valid_key_count"] == 1
    assert relationship["duplicate_key_count"] == 1
    assert relationship["conflicting_key_count"] == 1  # preserved, not silently resolved

    independence = foreign["diagnostics"]["representante_independence"]
    assert independence["duplicate_relationship_groups"] == 1
    assert independence["groups_with_representante_variation"] == 1


def test_foreign_partner_null_pais_evidence_sample_not_dropped_by_null_safe_join(tmp_path):
    """Regression, with a REAL SQL NULL (not an empty string): `pais` is
    NULL for ~0.6% of real foreign-partner rows (loaded as SQL NULL by
    `CsvSpec.null_padding=True` in the real pipeline, not empty string). A
    plain `=` join between the duplicate-key aggregate and the raw rows
    silently drops NULL-valued groups (`NULL = NULL` is not TRUE in SQL),
    producing an empty evidence_sample even though duplicate_key_count is
    correctly non-zero. `IS NOT DISTINCT FROM` must be used instead.
    `_write_socio_parquet` inserts a Python `None` as an actual SQL NULL
    cell (via parameterized INSERT), not the literal string `""` -- this is
    the distinction that matters: `TRIM(NULL) = ''` is NULL/unknown, not
    TRUE, so a test using `pais=""` would not actually exercise this path.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        row = _socio_row(
            identificador_socio="3",
            cnpj_cpf_socio=None,
            nome_socio_razao_social="JOHN SMITH",
            pais=None,  # real SQL NULL, not ""
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
    assert nome_pais["evidence_sample"][0]["pais"] is None


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


def test_representante_independence_is_null_aware(tmp_path):
    """Regression: `COUNT(DISTINCT col) > 1` alone silently ignores NULL
    rows, so a group with one row that has a legal representative on file
    (a real value) and one that doesn't (real SQL NULL, not an empty
    string) would be reported as "consistent" -- COUNT(DISTINCT) over
    (NULL, 'value') is 1, not 2. That is a genuine variation and must be
    flagged; `_null_aware_varies_expr` exists specifically for this case.
    """
    con = duckdb.connect()
    try:
        p0 = tmp_path / "part-0.parquet"
        _write_socio_parquet(
            con,
            p0,
            [
                _socio_row(
                    representante_legal=None,
                    nome_representante_legal=None,
                    qualificacao_representante_legal=None,
                ),
                # same relationship key, but this occurrence HAS a legal
                # representative on file -- real NULL vs real value, no
                # second DISTINCT non-NULL value involved
                _socio_row(
                    representante_legal="11111111111",
                    nome_representante_legal="REP A",
                    qualificacao_representante_legal="05",
                ),
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
    assert narrow["conflicting_key_count"] is None  # not computed at identity level
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


# -----------------------------------------------------------------------------
# c) aggregation-only mode -- re-runs the current code's global aggregation
#    against checkpoints that already exist on disk (e.g. restored from a
#    prior run's artifact), with no network access at all.
# -----------------------------------------------------------------------------


def _build_offline_checkpoints(root: Path, tmp_path: Path) -> None:
    """Populate `root/columns/part-N.parquet` (+ manifests) for all ten
    parts via the normal offline `run_part_checkpoint` path (tiny
    synthetic ZIPs), simulating what a restored artifact would already
    contain on disk before `run_aggregation_only` is called."""
    for part in range(10):
        zip_path = tmp_path / f"Socios{part}.zip"
        if part == 0:
            _write_zip(zip_path, [_row(cnpj_basico="00000001", cnpj_cpf_socio="***111111**")])
        else:
            _write_zip(zip_path, [])
        key_audit.run_part_checkpoint("2026-04", part, root, zip_override=zip_path)


def _run_aggregation_only(root, month="2026-04", **overrides):
    kwargs = {
        "source_workflow_run_id": "111111",
        "source_artifact_name": "socio-key-audit-2026-04-111111",
    }
    kwargs.update(overrides)
    return key_audit.run_aggregation_only(root, month, **kwargs)


def test_run_aggregation_only_requires_all_parts_present(tmp_path):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)
    (root / "columns" / "part-3.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="part 3"):
        _run_aggregation_only(root)


def test_run_aggregation_only_requires_source_provenance(tmp_path):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)

    with pytest.raises(ValueError, match="source_workflow_run_id"):
        key_audit.run_aggregation_only(
            root, "2026-04", source_workflow_run_id="", source_artifact_name="x"
        )
    with pytest.raises(ValueError, match="source_artifact_name"):
        key_audit.run_aggregation_only(
            root, "2026-04", source_workflow_run_id="111111", source_artifact_name=""
        )


def test_run_aggregation_only_requires_manifest_for_every_part(tmp_path):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)
    (root / "evidence" / "part-3.key-audit.manifest.json").unlink()

    with pytest.raises(FileNotFoundError, match="manifest for part 3"):
        _run_aggregation_only(root)


def test_run_aggregation_only_requires_manifest_checksum(tmp_path):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)
    manifest_path = root / "evidence" / "part-3.key-audit.manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["output"]["sha256"] = ""  # present but empty -- not a usable checksum
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(RuntimeError, match="no recorded output checksum"):
        _run_aggregation_only(root)


def test_run_aggregation_only_rejects_checksum_mismatch(tmp_path):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)
    # Corrupt one checkpoint's bytes without updating its manifest --
    # aggregation-only must refuse to run over data that doesn't match
    # its own recorded checksum instead of silently aggregating over it.
    tampered = root / "columns" / "part-0.parquet"
    tampered.write_bytes(tampered.read_bytes() + b"\x00")

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        _run_aggregation_only(root)


def test_run_aggregation_only_succeeds_against_existing_checkpoints_no_network(
    tmp_path, monkeypatch
):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)

    def should_not_be_called(*_args, **_kwargs):
        raise AssertionError("aggregation-only mode must not touch the network")

    monkeypatch.setattr(key_audit, "preflight_parts", should_not_be_called)
    monkeypatch.setattr(key_audit, "run_part_checkpoint", should_not_be_called)

    result = _run_aggregation_only(
        root,
        source_workflow_run_id="29789142307",
        source_artifact_name="socio-key-audit-2026-04-29789142307",
        source_artifact_id="8479703886",
        source_checkpoint_commit="29fa59921c492021ec8cb52b22d8ae2c8e7c5805",
    )

    assert result.report["mode"] == "aggregation-only"
    assert result.report["total_rows_scanned"] == 1
    assert len(result.verified_checksums) == 10
    assert result.report["categories"]["pessoa_fisica"]["row_count"] == 1


def test_run_aggregation_only_report_contains_source_provenance(tmp_path):
    """Machine-readable provenance in the generated report: which prior
    run/artifact the checkpoints came from, which commit produced this
    reanalysis, and every part's expected+actual checksum -- not just a
    pass/fail boolean."""
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)

    result = _run_aggregation_only(
        root,
        source_workflow_run_id="29789142307",
        source_artifact_name="socio-key-audit-2026-04-29789142307",
        source_artifact_id="8479703886",
        source_checkpoint_commit="29fa59921c492021ec8cb52b22d8ae2c8e7c5805",
    )

    provenance = result.report["source_provenance"]
    assert provenance["workflow_run_id"] == "29789142307"
    assert provenance["artifact_name"] == "socio-key-audit-2026-04-29789142307"
    assert provenance["artifact_id"] == "8479703886"
    assert provenance["checkpoint_commit"] == "29fa59921c492021ec8cb52b22d8ae2c8e7c5805"
    assert "reanalysis_workflow_run_id" in result.report
    assert "reanalysis_source_commit" in result.report

    assert len(result.report["verified_checkpoint_checksums"]) == 10
    for part_key, checksums in result.report["verified_checkpoint_checksums"].items():
        assert checksums["expected"] == checksums["actual"], part_key
        assert len(checksums["expected"]) == 64, part_key  # sha256 hex digest


def test_cli_aggregate_only_rejects_zip_override(tmp_path):
    result = key_audit.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(tmp_path / "run"),
            "--aggregate-only",
            "--source-run-id",
            "111111",
            "--source-artifact-name",
            "socio-key-audit-2026-04-111111",
            "--zip",
            f"0={tmp_path / 'does-not-exist.zip'}",
        ]
    )
    assert result == 2


def test_cli_aggregate_only_requires_source_provenance(tmp_path):
    result = key_audit.main(
        ["--month", "2026-04", "--root", str(tmp_path / "run"), "--aggregate-only"]
    )
    assert result == 2


def test_cli_aggregate_only_end_to_end(tmp_path, capsys):
    root = tmp_path / "run"
    _build_offline_checkpoints(root, tmp_path)

    result = key_audit.main(
        [
            "--month",
            "2026-04",
            "--root",
            str(root),
            "--aggregate-only",
            "--source-run-id",
            "29789142307",
            "--source-artifact-name",
            "socio-key-audit-2026-04-29789142307",
            "--source-artifact-id",
            "8479703886",
            "--source-checkpoint-commit",
            "29fa59921c492021ec8cb52b22d8ae2c8e7c5805",
        ]
    )

    assert result == 0
    out = capsys.readouterr().out
    assert "1 rows scanned" in out
    report = json.loads((root / "evidence" / "global.socio-key-audit.json").read_text())
    assert report["mode"] == "aggregation-only"
    assert report["source_provenance"]["workflow_run_id"] == "29789142307"
    assert report["source_provenance"]["artifact_name"] == "socio-key-audit-2026-04-29789142307"
