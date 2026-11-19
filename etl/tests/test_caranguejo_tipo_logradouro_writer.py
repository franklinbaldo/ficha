"""Prova DuckDB/Parquet sintética: tipo de logradouro separado na RFB (#291)."""

from pathlib import Path

import duckdb

from ficha_etl.transform import write_enderecos_parquet


def test_enderecos_writer_compoe_tipo_sem_duplicar_prefixo(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE estabelecimento (
                cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR,
                uf VARCHAR, municipio VARCHAR, tipo_logradouro VARCHAR,
                logradouro VARCHAR, numero VARCHAR, bairro VARCHAR, cep VARCHAR
            )
            """
        )
        casos = [
            ("RUA", "DAS FLORES", "RUA DAS FLORES"),
            ("AV", "BRASIL", "AVENIDA BRASIL"),
            ("RUA", "R. DAS FLORES", "RUA DAS FLORES"),
            ("RUA", "AVENIDA CENTRAL", "AVENIDA CENTRAL"),
            (None, "DAS FLORES", "DAS FLORES"),
            ("", "DAS FLORES", "DAS FLORES"),
            ("RUA", "RUA 25 DE MARÇO", "RUA 25 DE MARCO"),
            ("AV.", "  São João  ", "AVENIDA SAO JOAO"),
            ("RUA", "BRASIL 25", "RUA BRASIL 25"),
        ]
        con.executemany(
            "INSERT INTO estabelecimento VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{i:08d}",
                    "E08G",
                    "12",
                    "SP",
                    "7107",
                    tipo,
                    nome,
                    str(i),
                    "CENTRO",
                    "01000000",
                )
                for i, (tipo, nome, _) in enumerate(casos, start=1)
            ],
        )
        con.execute("CREATE TABLE lookup_municipios(codigo VARCHAR, descricao VARCHAR)")
        con.execute("INSERT INTO lookup_municipios VALUES ('7107', 'MUNICIPIO RFB')")
        output = tmp_path / "enderecos.parquet"
        write_enderecos_parquet(con, output)

        rows = con.execute(
            """
            SELECT cnpj, logradouro_normalizado, municipio_codigo, municipio_nome,
                   typeof(cnpj)
            FROM read_parquet(?)
            ORDER BY cnpj
            """,
            [str(output)],
        ).fetchall()
        assert len(rows) == len(casos)
        assert rows == [
            (f"{i:08d}E08G12", esperado, "7107", "MUNICIPIO RFB", "VARCHAR")
            for i, (_, _, esperado) in enumerate(casos, start=1)
        ]

        def buscar(termo: str) -> list[str]:
            return [
                row[0]
                for row in con.execute(
                    """
                    SELECT cnpj FROM read_parquet(?)
                    WHERE logradouro_normalizado ILIKE ?
                    ORDER BY cnpj
                    """,
                    [str(output), f"%{termo}%"],
                ).fetchall()
            ]

        assert buscar("RUA DAS FLORES") == ["00000001E08G12", "00000003E08G12"]
        assert buscar("AVENIDA BRASIL") == ["00000002E08G12"]
        assert buscar("AVENIDA CENTRAL") == ["00000004E08G12"]
        assert buscar("RUA AVENIDA CENTRAL") == []
        assert buscar("RUA 25 DE MARCO") == ["00000007E08G12"]
        assert buscar("AVENIDA SAO JOAO") == ["00000008E08G12"]
        assert buscar("RUA BRASIL 25") == ["00000009E08G12"]
    finally:
        con.close()
