"""Semântica da ordem de estabelecimentos e sócios dentro de uma raiz.

Até #147 esta ordem vinha do `ORDER BY` interno ao `list(struct ...)` do
`_COMPANIES_SQL`. Ela saiu do SQL porque a interação entre ordenar e
materializar um struct de 34 campos dominava o `pack` — 53,2 s contra 3,7 s de
cada fator isolado, no bucket 00 de 2026-05.

Estes testes existem porque a ordem deixou de ser garantida pelo banco e passou
a ser responsabilidade do código: eles fixam a semântica exata que o protobuf
espera, incluindo os casos que 2026-05 não exercita (medido: zero empates de
`cnpj_ordem` e zero NULLs nas chaves em 68.081.781 raízes).
"""

from __future__ import annotations

import duckdb
import pytest

from ficha_etl.pack import (
    ORDEM_ESTABELECIMENTOS,
    ORDEM_SOCIOS,
    em_ordem,
    row_to_company,
)


def _ordens(itens) -> list:
    return [d["cnpj_ordem"] for d in em_ordem(itens, ORDEM_ESTABELECIMENTOS)]


# --- a ordem que o caso comum exige ------------------------------------------


def test_ordena_por_cnpj_ordem():
    itens = [
        {"cnpj_ordem": "0003", "cnpj_dv": "11"},
        {"cnpj_ordem": "0001", "cnpj_dv": "22"},
        {"cnpj_ordem": "0002", "cnpj_dv": "33"},
    ]
    assert _ordens(itens) == ["0001", "0002", "0003"]


def test_listas_de_0_e_1_elemento_passam_intactas():
    """97,95% das raízes de 2026-05 têm exatamente um estabelecimento."""
    assert em_ordem(None, ORDEM_ESTABELECIMENTOS) == []
    assert em_ordem([], ORDEM_ESTABELECIMENTOS) == []
    um = [{"cnpj_ordem": "0001", "cnpj_dv": "99"}]
    assert em_ordem(um, ORDEM_ESTABELECIMENTOS) is um


# --- os casos que 2026-05 não tem, e por isso precisam ser fixados aqui -------


def test_cnpj_dv_desempata_cnpj_ordem():
    itens = [
        {"cnpj_ordem": "0001", "cnpj_dv": "99"},
        {"cnpj_ordem": "0001", "cnpj_dv": "11"},
    ]
    assert [d["cnpj_dv"] for d in em_ordem(itens, ORDEM_ESTABELECIMENTOS)] == ["11", "99"]


def test_nulos_vao_para_o_fim_como_no_duckdb():
    """DuckDB usa NULLS LAST em ASC por padrão; a ordenação replica isso."""
    itens = [
        {"cnpj_ordem": None, "cnpj_dv": "11"},
        {"cnpj_ordem": "0002", "cnpj_dv": "22"},
        {"cnpj_ordem": "0001", "cnpj_dv": "33"},
    ]
    assert _ordens(itens) == ["0001", "0002", None]


def test_nulo_na_segunda_chave_tambem_vai_para_o_fim():
    itens = [
        {"cnpj_ordem": "0001", "cnpj_dv": None},
        {"cnpj_ordem": "0001", "cnpj_dv": "11"},
    ]
    assert [d["cnpj_dv"] for d in em_ordem(itens, ORDEM_ESTABELECIMENTOS)] == ["11", None]


def test_todas_as_chaves_nulas_nao_quebra():
    """`None < None` levantaria TypeError; a comparação nunca pode chegar lá."""
    itens = [
        {"cnpj_ordem": None, "cnpj_dv": None},
        {"cnpj_ordem": None, "cnpj_dv": None},
    ]
    assert len(em_ordem(itens, ORDEM_ESTABELECIMENTOS)) == 2


def test_empate_total_preserva_a_ordem_de_entrada():
    """Sob empate total a ordem não é definida pelas chaves — nem aqui, nem no
    `ORDER BY` que existia antes. `sorted` é estável, então o resultado é ao
    menos previsível *dada* a entrada; a entrada em si vem do agregado do
    DuckDB e não é determinística sob paralelismo.

    Isto está registrado como risco aberto em #147: a reprodutibilidade do
    sha256 de `companies.zip` depende de não haver empate total, que é o que
    2026-05 mede (zero casos), não o que o cadastro garante.
    """
    a = {"cnpj_ordem": "0001", "cnpj_dv": "11", "marca": "a"}
    b = {"cnpj_ordem": "0001", "cnpj_dv": "11", "marca": "b"}
    assert [d["marca"] for d in em_ordem([a, b], ORDEM_ESTABELECIMENTOS)] == ["a", "b"]
    assert [d["marca"] for d in em_ordem([b, a], ORDEM_ESTABELECIMENTOS)] == ["b", "a"]


def test_socios_ordenam_pelas_onze_chaves_na_ordem_declarada():
    base = dict.fromkeys(ORDEM_SOCIOS)
    itens = [
        {**base, "qualificacao_codigo": "49", "nome_socio_razao_social": "ANA"},
        {**base, "qualificacao_codigo": "22", "nome_socio_razao_social": "ZEZE"},
        {**base, "qualificacao_codigo": "22", "nome_socio_razao_social": "ANA"},
    ]
    ordenados = em_ordem(itens, ORDEM_SOCIOS)
    assert [(d["qualificacao_codigo"], d["nome_socio_razao_social"]) for d in ordenados] == [
        ("22", "ANA"),
        ("22", "ZEZE"),
        ("49", "ANA"),
    ]


# --- a ordenação tem que chegar ao protobuf ----------------------------------


def test_row_to_company_emite_estabelecimentos_em_ordem():
    """O protobuf é o contrato observável: a ordem tem que valer nele."""
    row = {
        "cnpj_base": "12345678",
        "estabelecimentos": [
            {"cnpj_ordem": "0002", "cnpj_dv": "11"},
            {"cnpj_ordem": "0001", "cnpj_dv": "22"},
        ],
        "socios": [
            {**dict.fromkeys(ORDEM_SOCIOS), "qualificacao_codigo": "49"},
            {**dict.fromkeys(ORDEM_SOCIOS), "qualificacao_codigo": "22"},
        ],
    }
    c = row_to_company(row)
    assert [e.cnpj_ordem for e in c.estabelecimentos] == [1, 2]
    assert [s.qualificacao_codigo for s in c.socios] == [22, 49]


# --- equivalência com a semântica que o DuckDB aplicava antes -----------------


@pytest.mark.parametrize(
    "valores",
    [
        [("0003", "11"), ("0001", "22"), ("0002", "33")],
        [("0001", "99"), ("0001", "11")],
        [(None, "11"), ("0002", "22"), ("0001", "33")],
        [("0001", None), ("0001", "11")],
        [(None, None), ("0001", "11"), (None, "22")],
    ],
)
def test_bate_com_o_order_by_do_duckdb(valores):
    """Prova de equivalência da regra, não só da intenção.

    Compara `em_ordem` com o `ORDER BY cnpj_ordem, cnpj_dv` que o DuckDB
    executava dentro do `list(...)`, nos mesmos dados.
    """
    con = duckdb.connect()
    con.execute("CREATE TABLE t(cnpj_ordem VARCHAR, cnpj_dv VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", valores)
    esperado = con.execute(
        "SELECT list({'cnpj_ordem': cnpj_ordem, 'cnpj_dv': cnpj_dv} "
        "ORDER BY cnpj_ordem, cnpj_dv) FROM t"
    ).fetchone()[0]

    itens = [{"cnpj_ordem": o, "cnpj_dv": d} for o, d in valores]
    obtido = em_ordem(itens, ORDEM_ESTABELECIMENTOS)

    assert [(d["cnpj_ordem"], d["cnpj_dv"]) for d in obtido] == [
        (d["cnpj_ordem"], d["cnpj_dv"]) for d in esperado
    ]
