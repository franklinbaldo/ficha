"""Regressoes de unicidade das seis lookups antes dos joins do ETL (#291)."""

from pathlib import Path

import duckdb
import pytest

from ficha_etl import transform


KINDS = ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")


def _write_lookup_csv(path: Path, rows: list[tuple[str, str]]) -> Path:
    body = "".join(f'"{codigo}";"{descricao}"\n' for codigo, descricao in rows)
    path.write_bytes(body.encode("latin-1"))
    return path


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("other_description", "conflicting_keys"),
    [("Mesmo", 0), ("Diferente", 1)],
    ids=("identical-duplicate", "conflicting-duplicate"),
)
def test_reject_duplicate_lookup_codigo_before_writers(
    tmp_path: Path,
    kind: str,
    other_description: str,
    conflicting_keys: int,
) -> None:
    """Mesmo codigo nao pode multiplicar joins, ainda que descricao seja igual."""
    csv_path = _write_lookup_csv(
        tmp_path / f"{kind}.csv",
        [("0100", "Mesmo"), ("0100", other_description)],
    )
    con = duckdb.connect()
    try:
        with pytest.raises(ValueError, match=f"lookup_{kind}: duplicate codigo") as exc:
            transform.load_lookup_into_duckdb(con, kind, csv_path)
        assert "keys=1" in str(exc.value)
        assert "excess_rows=1" in str(exc.value)
        assert f"conflicting_keys={conflicting_keys}" in str(exc.value)
        assert "0100" in str(exc.value)
        # Caller catching the error must not see a dirty join input.
        n = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [f"lookup_{kind}"],
        ).fetchone()[0]
        assert n == 0
    finally:
        con.close()


def test_distinct_codes_and_latin1_descriptions_remain_valid(tmp_path: Path) -> None:
    con = duckdb.connect()
    try:
        path = _write_lookup_csv(
            tmp_path / "municipios.csv",
            [("0001", "São Paulo"), ("0002", "São Paulo")],
        )
        transform.load_lookup_into_duckdb(con, "municipios", path)
        assert transform.lookups_dict(con, "municipios") == {
            "0001": "São Paulo",
            "0002": "São Paulo",
        }
    finally:
        con.close()


def test_valid_cnae_lookup_keeps_one_association_per_position(tmp_path: Path) -> None:
    """Controle DuckDB/Parquet real: CNAE principal e secundario sem fan-out."""
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE estabelecimento AS SELECT
                'A1234567'::VARCHAR AS cnpj_basico,
                '0001'::VARCHAR AS cnpj_ordem,
                '90'::VARCHAR AS cnpj_dv,
                '4711301'::VARCHAR AS cnae_fiscal_principal,
                '6201500'::VARCHAR AS cnae_fiscal_secundaria
            """
        )
        csv_path = _write_lookup_csv(
            tmp_path / "cnaes.csv",
            [("4711301", "Comercio"), ("6201500", "Serviços de software")],
        )
        transform.load_lookup_into_duckdb(con, "cnaes", csv_path)
        output = tmp_path / "cnpj_cnaes.parquet"
        transform.write_cnpj_cnaes_parquet(con, output)
        rows = con.execute(
            """
            SELECT cnpj, cnae_codigo, posicao, cnae_descricao
            FROM read_parquet(?)
            ORDER BY posicao
            """,
            [str(output)],
        ).fetchall()
        assert rows == [
            ("A1234567000190", "4711301", 0, "Comercio"),
            ("A1234567000190", "6201500", 1, "Serviços de software"),
        ]
        assert len({row[:3] for row in rows}) == len(rows)
    finally:
        con.close()
