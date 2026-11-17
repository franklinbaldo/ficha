"""Offline, synthetic contract test for the production address Parquet writer."""

from pathlib import Path

import duckdb

from ficha_etl.transform import write_enderecos_parquet


def test_enderecos_writer_preserves_alpha_base_and_order_and_rfb(
    tmp_path: Path,
) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE estabelecimento ("
            "cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR, "
            "uf VARCHAR, municipio VARCHAR, logradouro VARCHAR, "
            "numero VARCHAR, bairro VARCHAR, cep VARCHAR)"
        )
        con.executemany(
            "INSERT INTO estabelecimento VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "00000000",
                    "E08G",
                    "12",
                    "SP",
                    "7107",
                    "R. das Flores",
                    "10",
                    "CENTRO",
                    "01000000",
                ),
                (
                    "00AB12C3",
                    "0A01",
                    "91",
                    "SP",
                    "7108",
                    "R. das Flores",
                    "11",
                    "CENTRO",
                    "01000001",
                ),
                (
                    "00AB12C3",
                    "00B2",
                    "76",
                    "RJ",
                    "7107",
                    "R. das Flores",
                    "12",
                    "CENTRO",
                    "22000000",
                ),
                (
                    "00000001",
                    "0001",
                    "91",
                    "SP",
                    "7107",
                    "Rua das Flores",
                    "13",
                    "CENTRO",
                    "01000002",
                ),
                (
                    "00000002",
                    "0001",
                    "00",
                    "SP",
                    "7107",
                    "",
                    "14",
                    "CENTRO",
                    "01000003",
                ),
            ],
        )
        con.execute(
            "CREATE TABLE lookup_municipios (codigo VARCHAR, descricao VARCHAR)"
        )
        con.executemany(
            "INSERT INTO lookup_municipios VALUES (?, ?)",
            [
                ("7107", "RFB TESTE 7107"),
                ("7108", "RFB TESTE 7108"),
                ("3550308", "IBGE NAO EQUIVALE A RFB"),
            ],
        )

        path = tmp_path / "enderecos.parquet"
        write_enderecos_parquet(con, path)
        assert path.is_file()
        assert 0 < path.stat().st_size < 100_000

        rows = con.execute(
            "SELECT cnpj, uf, municipio_codigo, municipio_nome, "
            "logradouro_normalizado, typeof(cnpj) AS tipo "
            "FROM read_parquet(?) ORDER BY cnpj",
            [str(path)],
        ).fetchall()
        assert rows == [
            (
                "00000000E08G12",
                "SP",
                "7107",
                "RFB TESTE 7107",
                "RUA DAS FLORES",
                "VARCHAR",
            ),
            (
                "00000001000191",
                "SP",
                "7107",
                "RFB TESTE 7107",
                "RUA DAS FLORES",
                "VARCHAR",
            ),
            (
                "00AB12C300B276",
                "RJ",
                "7107",
                "RFB TESTE 7107",
                "RUA DAS FLORES",
                "VARCHAR",
            ),
            (
                "00AB12C30A0191",
                "SP",
                "7108",
                "RFB TESTE 7108",
                "RUA DAS FLORES",
                "VARCHAR",
            ),
        ]
        assert all(len(row[0]) == 14 for row in rows)

        def filtered(uf: str, municipio: str) -> list[str]:
            return [
                row[0]
                for row in con.execute(
                    "SELECT cnpj FROM read_parquet(?) "
                    "WHERE uf = ? AND municipio_codigo = ? ORDER BY cnpj",
                    [str(path), uf, municipio],
                ).fetchall()
            ]

        assert filtered("SP", "7107") == ["00000000E08G12", "00000001000191"]
        assert filtered("SP", "7108") == ["00AB12C30A0191"]
        assert filtered("RJ", "7107") == ["00AB12C300B276"]
        assert filtered("RJ", "7108") == []
        assert filtered("SP", "3550308") == []  # IBGE 7 digits != RFB 4 digits
        assert "00000000008012" not in filtered("SP", "7107")
        assert "00AB12C3000276" not in filtered("RJ", "7107")
    finally:
        con.close()
