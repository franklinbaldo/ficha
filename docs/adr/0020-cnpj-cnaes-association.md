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

Produzir `cnpj_cnaes.parquet` (`write_cnpj_cnaes_parquet`), uma linha por
associação CNPJ↔CNAE:

| Coluna | Tipo | Notas |
|--------|------|-------|
| `cnpj` | VARCHAR(14) | CNPJ completo |
| `cnpj_base` | VARCHAR(8) | raiz do CNPJ |
| `cnae_codigo` | VARCHAR | código do CNAE |
| `cnae_descricao` | VARCHAR | descrição denormalizada do lookup CNAE |
| `posicao` | INTEGER | `0` = principal; `1, 2, …` = secundário, na ordem de registro |

As associações continuam construídas via `UNION ALL` das linhas de
`estabelecimento` (principal + secundários explodidos). Depois, um `LEFT JOIN`
com o minúsculo `lookup_cnaes` acrescenta a descrição antes da escrita.

Esse join acontece **uma vez no ETL**, não no consumo. O lookup parquet de CNAE
continua existindo como relação auxiliar barata (ADR 0019), mas uma linha de
`cnpj_cnaes.parquet` já é interpretável/renderizável sozinha. A repetição da
descrição é deliberada: Parquet comprime bem valores categóricos repetidos.

**Sort:** `(cnae_codigo, posicao, cnpj_base)` — registrado no manifest em
`files.cnpj_cnaes.sort`. A ordenação mantém as linhas `posicao=0` de cada CNAE
contíguas, permitindo pruning por min/max mesmo em queries "só principal".

Os arrays denormalizados em `cnpjs.parquet` **permanecem** — atendem a lâmina
sem join; `cnpj_cnaes.parquet` é o índice inverso, mesmo padrão de
`socios.parquet` (forward) coexistindo com `pessoas.parquet` (inverso,
[ADR 0024](0024-pessoas-parquet.md)).

## Consequências

- ✅ consultas por código continuam usando sort-prefix/pruning;
- ✅ resultado já traz a descrição do CNAE, sem join de renderização;
- ✅ `lookups/cnaes.parquet` permanece útil para consultas sobre o vocabulário;
- ✅ o único join novo fica no ETL e usa uma relação minúscula;
- ⚠️ `cnae_descricao` aumenta o payload lógico, mas a repetição é altamente compressível;
- ⚠️ mudança aditiva de schema: snapshots históricos sem a coluna continuam legíveis.
