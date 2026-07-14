# ADR 0020 — `cnpj_cnaes.parquet`: associação CNPJ↔CNAE posicional

**Status:** Aceito
**Data:** 2026-07-14
**Contexto:** docs/perf-plan-2026-05.md §11

---

## Contexto

`cnpjs.parquet` guarda o CNAE secundário como array denormalizado
(`cnae_secundario_codigos`, `cnae_secundario_descricoes`) — barato para
renderizar a lâmina de uma empresa, mas caro para o padrão inverso: "todos os
CNPJs com CNAE 5611-2 (restaurantes), seja principal ou secundário" exige
`unnest` + filtro sobre a base inteira.

## Decisão

Produzir `cnpj_cnaes.parquet` (`write_cnpj_cnaes_parquet`,
`transform.py:1056`), uma linha por associação CNPJ↔CNAE:

| Coluna | Tipo | Notas |
|--------|------|-------|
| `cnpj` | VARCHAR(14) | CNPJ completo |
| `cnpj_base` | VARCHAR(8) | raiz do CNPJ |
| `cnae_codigo` | VARCHAR | código do CNAE |
| `posicao` | INTEGER | `0` = principal; `1, 2, …` = secundário, na ordem de registro |

Construído via `UNION ALL` de duas SELECTs sobre `estabelecimento` (principal
+ secundários explodidos via `generate_subscripts`/`unnest` de
`cnae_fiscal_secundaria`), sem joins.

**Sort:** `(cnae_codigo, posicao, cnpj_base)` — registrado no manifest em
`files.cnpj_cnaes.sort` (`manifest.py`). A ordenação mantém as linhas
`posicao=0` de cada CNAE contíguas, permitindo pruning por min/max mesmo em
queries "só principal" sem precisar de uma coluna booleana separada.

Os arrays denormalizados em `cnpjs.parquet` **permanecem** — atendem a lâmina
sem join; `cnpj_cnaes.parquet` é o índice inverso, mesmo padrão de
`socios.parquet` (forward) coexistindo com `pessoas.parquet` (inverso,
[ADR 0024](0024-pessoas-parquet.md)).

## Consequências

- ✅ "CNPJs com CNAE X, em qualquer posição" e "só como principal" viram
  lookups por sort prefix em vez de full scan.
- ✅ Construído só a partir de `estabelecimento` — sem joins, baixo custo de
  memória no phase 3.
- ⚠️ ~1 linha por (CNPJ × CNAE), então o parquet é maior que `cnpjs.parquet`
  em contagem de linhas (múltiplos CNAEs por estabelecimento).
- Frontend usa `attachCnpjCnaes(db, url)`
  (`web/src/lib/analytical.ts:134`).
