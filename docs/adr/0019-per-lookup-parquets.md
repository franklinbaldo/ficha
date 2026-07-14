# ADR 0019 — Per-lookup parquets ao lado do `lookups.json`

**Status:** Aceito
**Data:** 2026-07-14
**Contexto:** docs/perf-plan-2026-05.md §10

---

## Contexto

`lookups.json` (`write_lookups_json`, `transform.py:443`) resolve o caso de
render síncrono no boot do site — código→descrição para linhas já carregadas.
Mas não serve consultas que precisam **filtrar ou agregar por descrição**
(ex.: "empresas em municípios cujo nome começa com BRAS"), porque isso exigiria
tradução nome→código no lado do cliente antes de montar a query DuckDB.

## Decisão

Emitir, além de `lookups.json`, um parquet por lookup em
`<snapshot>/lookups/<kind>.parquet` (`write_lookup_parquets`,
`transform.py:465`), para os seis kinds em `_LOOKUP_KINDS` (cnaes, motivos,
municipios, naturezas, paises, qualificacoes):

| Coluna | Tipo | Notas |
|--------|------|-------|
| `codigo` | VARCHAR | chave do lookup |
| `descricao` | VARCHAR | descrição original |
| `descricao_normalizada` | VARCHAR | `UPPER(strip_accents(descricao))` |

**Sort:** `codigo`. Cada arquivo é pequeno (o maior, municípios, tem ~5.500
linhas) — cabe num único row group, efetivamente memory-resident na primeira
leitura.

`lookups.json` **não é depreciado**: JSON continua servindo o render
síncrono; os parquets servem composição SQL (`JOIN lookup_<kind>`). Custo de
duplicação é irrelevante (< 1 MB no total).

## Frontend

`attachLookups(db, manifest)` (`web/src/lib/analytical.ts:47`) registra os
seis arquivos e cria uma `VIEW lookup_<kind>` por kind, permitindo:

```sql
SELECT c.cnpj, c.razao_social, m.descricao AS municipio
FROM cnpjs c
JOIN lookup_municipios m ON m.codigo = c.municipio_codigo
WHERE m.descricao_normalizada LIKE 'BRAS%'
LIMIT 50
```

## Consequências

- ✅ Filtro/agregação por descrição sem tradução client-side prévia.
- ✅ `lookups.json` mantido — nenhuma mudança no caminho de render existente.
- ⚠️ +6 writes no phase 3 do ETL, cada um trivial (poucas centenas de linhas).
- Manifest ganha o mapa `lookups: {cnaes: {...}, motivos: {...}, ...}`
  (`manifest.py`), separado da entrada `files.lookups` (o JSON).
