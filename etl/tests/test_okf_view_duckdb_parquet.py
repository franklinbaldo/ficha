"""Confront generated OKF view SQL with small, synthetic DuckDB Parquet inputs.

The fixtures deliberately contain a duplicated active Raiz. This is not
evidence of duplicated published RFB rows or a test of the SSG site.
"""

from collections import Counter
import json
from pathlib import Path

import duckdb
import pytest


GENERATED_VIEWS = Path(__file__).resolve().parents[2] / "web/src/generated/ficha-okf.views.ts"


def generated_sql():
    content = GENERATED_VIEWS.read_text(encoding="utf-8")
    marker = "export const okfConvenienceViews = "
    assert content.count(marker) == 1
    payload = content.split(marker, 1)[1].split(" as const satisfies", 1)[0]
    views = json.loads(payload)
    assert isinstance(views, list) and len(views) == 2
    names = [view["name"] for view in views]
    assert names == ["empresas_ativas", "socios_de_raizes_ativas"]
    return {view["name"]: view["sql"] for view in views}


@pytest.fixture
def parquet_views(tmp_path: Path):
    connection = duckdb.connect(":memory:")
    try:
        fixture_sql = """
            CREATE TABLE source_cnpjs (cnpj VARCHAR, situacao_cadastral VARCHAR);
            INSERT INTO source_cnpjs VALUES
                ('A', '02'), ('B', '08'), ('C', NULL);
            CREATE TABLE source_socios (
                cnpj_base VARCHAR, nome_socio_razao_social VARCHAR
            );
            INSERT INTO source_socios VALUES
                ('11111111', 'one-active-parent'),
                ('22222222', 'inactive-parent'),
                ('33333333', 'missing-parent'),
                ('44444444', 'first-duplicate-parent'),
                ('44444444', 'second-duplicate-parent'),
                ('55555555', 'null-active-count');
            CREATE TABLE source_raizes (
                cnpj_base VARCHAR, qtd_estabelecimentos_ativos INTEGER
            );
            INSERT INTO source_raizes VALUES
                ('11111111', 1), ('22222222', 0),
                ('44444444', 1), ('44444444', 2), ('55555555', NULL);
        """
        connection.execute(fixture_sql)
        for relation in ("cnpjs", "socios", "raizes"):
            path = tmp_path / f"{relation}.parquet"
            parquet_path = str(path).replace("\\", "/").replace("'", "''")
            connection.execute(f"COPY source_{relation} TO '{parquet_path}' (FORMAT PARQUET)")
            connection.execute(
                f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{parquet_path}')"
            )
        yield connection
    finally:
        connection.close()


def test_duckdb_executes_generated_views_over_parquet(parquet_views):
    sql = generated_sql()
    assert parquet_views.execute(sql["empresas_ativas"]).fetchall() == [("A", "02")]
    rows = parquet_views.execute(sql["socios_de_raizes_ativas"]).fetchall()
    expected = [
        ("11111111", "one-active-parent"),
        ("44444444", "first-duplicate-parent"),
        ("44444444", "first-duplicate-parent"),
        ("44444444", "second-duplicate-parent"),
        ("44444444", "second-duplicate-parent"),
    ]
    assert Counter(rows) == Counter(expected)


def test_independent_exists_oracle_exposes_join_cardinality(parquet_views):
    # "At least one active parent" is a semijoin; it must not multiply
    # distinct Socio input rows when Raiz has two active rows for one key.
    oracle = """
        SELECT s.* FROM socios AS s
        WHERE EXISTS (
            SELECT 1 FROM raizes AS r
            WHERE r.cnpj_base = s.cnpj_base
              AND r.qtd_estabelecimentos_ativos > 0
        )
    """
    expected = parquet_views.execute(oracle).fetchall()
    actual = parquet_views.execute(generated_sql()["socios_de_raizes_ativas"]).fetchall()
    expected_rows = [
        ("11111111", "one-active-parent"),
        ("44444444", "first-duplicate-parent"),
        ("44444444", "second-duplicate-parent"),
    ]
    assert Counter(expected) == Counter(expected_rows)
    assert len(actual) == 5
    assert Counter(actual) != Counter(expected)


def test_parquet_duplicate_root_fails_explicit_primary_key(parquet_views):
    # Reading Parquet as a relation does not confer a primary-key constraint.
    guarded_schema = """
        CREATE TABLE guarded_raizes (
            cnpj_base VARCHAR PRIMARY KEY,
            qtd_estabelecimentos_ativos INTEGER
        )
    """
    parquet_views.execute(guarded_schema)
    with pytest.raises(duckdb.ConstraintException):
        parquet_views.execute("INSERT INTO guarded_raizes SELECT * FROM raizes")


def test_explicit_primary_and_foreign_keys_restore_semijoin_parity():
    with duckdb.connect(":memory:") as connection:
        guarded_schema = """
            CREATE TABLE raizes (
                cnpj_base VARCHAR PRIMARY KEY,
                qtd_estabelecimentos_ativos INTEGER
            );
            CREATE TABLE socios (
                cnpj_base VARCHAR REFERENCES raizes(cnpj_base),
                nome_socio_razao_social VARCHAR
            );
            INSERT INTO raizes VALUES ('11111111', 1), ('44444444', 2);
            INSERT INTO socios VALUES
                ('11111111', 'one-active-parent'),
                ('44444444', 'first-duplicate-parent'),
                ('44444444', 'second-duplicate-parent');
        """
        connection.execute(guarded_schema)
        with pytest.raises(duckdb.ConstraintException):
            connection.execute("INSERT INTO raizes VALUES ('44444444', 1)")
        with pytest.raises(duckdb.ConstraintException):
            connection.execute("INSERT INTO socios VALUES ('99999999', 'orphan-partner')")

        actual = connection.execute(generated_sql()["socios_de_raizes_ativas"]).fetchall()
        expected = [
            ("11111111", "one-active-parent"),
            ("44444444", "first-duplicate-parent"),
            ("44444444", "second-duplicate-parent"),
        ]
        assert Counter(actual) == Counter(expected)


# These competence labels are fixture identifiers, not real RFB snapshot metadata.
# Different active parents in each fixture make a cross-snapshot JOIN observable.
COMPETENCIA_INPUTS = {
    "2026-07": {
        "cnpjs": [("A", "02"), ("B", "08")],
        "socios": [("11111111", "partner-one"), ("22222222", "partner-two")],
        "raizes": [("11111111", 1), ("22222222", 0)],
    },
    "2026-08": {
        "cnpjs": [("A", "08"), ("B", "02")],
        "socios": [("11111111", "partner-one"), ("22222222", "partner-two")],
        "raizes": [("11111111", 0), ("22222222", 1)],
    },
}


@pytest.fixture
def competencia_parquets(tmp_path: Path):
    schemas = {
        "cnpjs": "cnpj VARCHAR, situacao_cadastral VARCHAR",
        "socios": "cnpj_base VARCHAR, nome_socio_razao_social VARCHAR",
        "raizes": "cnpj_base VARCHAR, qtd_estabelecimentos_ativos INTEGER",
    }
    connection = duckdb.connect(":memory:")
    paths = {}
    try:
        for competencia, relations in COMPETENCIA_INPUTS.items():
            for relation, rows in relations.items():
                connection.execute(
                    f"CREATE TABLE source_{relation} ({schemas[relation]})"
                )
                connection.executemany(
                    f"INSERT INTO source_{relation} VALUES (?, ?)", rows
                )
                target = tmp_path / f"{competencia}-{relation}.parquet"
                escaped = target.as_posix().replace("'", "''")
                connection.execute(
                    f"COPY source_{relation} TO '{escaped}' (FORMAT PARQUET)"
                )
                connection.execute(f"DROP TABLE source_{relation}")
                paths[competencia, relation] = escaped
        yield connection, paths
    finally:
        connection.close()


def bind_competencia(connection, paths, competencia, raiz_competencia=None):
    for relation in ("cnpjs", "socios", "raizes"):
        origin = (
            raiz_competencia
            if relation == "raizes" and raiz_competencia is not None
            else competencia
        )
        connection.execute(
            f"CREATE OR REPLACE VIEW {relation} AS "
            f"SELECT * FROM read_parquet('{paths[origin, relation]}')"
        )


def raw_competencia_oracle(competencia):
    """Independent Python selection, not generated SQL or a Zod acceptance test."""
    source = COMPETENCIA_INPUTS[competencia]
    active_roots = {
        base for base, count in source["raizes"] if count is not None and count > 0
    }
    return {
        "empresas_ativas": [row for row in source["cnpjs"] if row[1] == "02"],
        "socios_de_raizes_ativas": [
            row for row in source["socios"] if row[0] in active_roots
        ],
    }


def test_competencia_scoped_generated_sql_matches_raw_oracle(competencia_parquets):
    connection, paths = competencia_parquets
    sql = generated_sql()
    for competencia in COMPETENCIA_INPUTS:
        bind_competencia(connection, paths, competencia)
        expected = raw_competencia_oracle(competencia)

        # This audit is scoped to the loaded fixture, not a property of Parquet.
        duplicate_roots = connection.execute(
            "SELECT cnpj_base FROM raizes "
            "GROUP BY cnpj_base HAVING COUNT(*) > 1"
        ).fetchall()
        orphan_socios = connection.execute(
            "SELECT s.cnpj_base FROM socios AS s "
            "WHERE NOT EXISTS (SELECT 1 FROM raizes AS r "
            "WHERE r.cnpj_base = s.cnpj_base)"
        ).fetchall()
        assert duplicate_roots == []
        assert orphan_socios == []

        for name in ("empresas_ativas", "socios_de_raizes_ativas"):
            actual = connection.execute(sql[name]).fetchall()
            assert Counter(actual) == Counter(expected[name]), (competencia, name)


def test_cross_competencia_root_swap_fails_raw_oracle(competencia_parquets):
    connection, paths = competencia_parquets
    sql = generated_sql()["socios_de_raizes_ativas"]
    for competencia, other in (("2026-07", "2026-08"), ("2026-08", "2026-07")):
        bind_competencia(connection, paths, competencia)
        expected = raw_competencia_oracle(competencia)["socios_de_raizes_ativas"]
        assert Counter(connection.execute(sql).fetchall()) == Counter(expected)

        # A physically valid root Parquet from another competence still has
        # unique keys and no orphans, but changes this snapshot's result.
        bind_competencia(connection, paths, competencia, raiz_competencia=other)
        swapped = connection.execute(sql).fetchall()
        assert Counter(swapped) != Counter(expected)
