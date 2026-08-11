from __future__ import annotations

import duckdb

from ficha_etl import transform


def test_raizes_from_cnpjs_spans_prefix_buckets_without_parts_leak(tmp_path):
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE src AS
            SELECT * FROM (VALUES
              ('01111111000100','01111111','1','02','20200101','4711301','SP','3550308','São Paulo','Comércio','ALFA','ALFA','2062','Sociedade',100.0,'03',''),
              ('11111111000100','11111111','1','02','20210101','6201500','RJ','3304557','Rio de Janeiro','TI','BETA','BETA','2062','Sociedade',200.0,'03',''),
              ('91111111000100','91111111','1','08','20220101','6201500','AM','1302603','Manaus','TI','GAMA','GAMA','2062','Sociedade',300.0,'03','')
            ) AS t(
              cnpj, cnpj_base, identificador_matriz_filial, situacao_cadastral,
              data_inicio_atividade, cnae_principal_codigo, uf, municipio_codigo,
              municipio_nome, cnae_principal_descricao, razao_social,
              razao_social_normalizada, natureza_juridica_codigo,
              natureza_juridica_descricao, capital_social, porte_empresa,
              ente_federativo_responsavel
            )
            """
        )
        src = tmp_path / "cnpjs.parquet"
        out = tmp_path / "raizes.parquet"
        con.execute(f"COPY src TO '{src}' (FORMAT PARQUET)")

        transform.write_raizes_parquet_from_cnpjs(con, src, out)

        rows = con.execute(
            f"SELECT cnpj_base, qtd_estabelecimentos, qtd_estabelecimentos_ativos "
            f"FROM read_parquet('{out}') ORDER BY cnpj_base"
        ).fetchall()
        assert rows == [
            ("01111111", 1, 1),
            ("11111111", 1, 1),
            ("91111111", 1, 0),
        ]
        assert not (tmp_path / "raizes.parts").exists()
    finally:
        con.close()
