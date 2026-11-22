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
