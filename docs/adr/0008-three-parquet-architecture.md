# ADR 0008 — Arquitetura de três Parquets + lookups.json

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

Cada snapshot mensal do FICHA precisa atender padrões de query distintos:

1. **Lookup por CNPJ exato** — "me dá a ficha da empresa X"
2. **Busca por razão social** — autocomplete tipo Google
3. **Listar sócios de uma empresa**
4. **Análises agregadas** (top capitais, distribuição por UF/CNAE)

Discutimos partitioning (`UF=AC/...`) vs index Parquets. DuckDB-WASM faz HTTP range
requests com pruning por row-group statistics + bloom filters, o que torna **partitioning
desnecessário** quando os Parquets têm sort/bloom adequados (ver [ADR 0009](0009-denormalization-and-roundtrip.md)).

## Decisão

Cada snapshot mensal contém **três Parquets + um JSON pequeno**, todos no mesmo item do Internet Archive `ficha-YYYY-MM`:

| Arquivo | Granularidade | Sort | Bloom filter | Tamanho est. | Padrão de acesso |
|---|---|---|---|---|---|
| **`cnpjs.parquet`** | uma linha por estabelecimento, com Empresa + Simples + lookups inline | `cnpj` | `cnpj` | ~3GB | Lookup por CNPJ; expansão de detalhes |
| **`raizes.parquet`** | uma linha por raiz, com agregados (qtd_estab, array de UFs) | `razao_social_normalizada` | — | ~150MB | Autocomplete; agregações |
| **`socios.parquet`** | sócios PF e PJ misturados, com flag `tipo` | `cnpj_base` | `cnpj_base`, `cpf_socio_mascarado` | ~500MB | "Sócios de X"; "X é sócio de Y" |
| **`lookups.json`** | tabelas de referência (Cnaes, Municípios, Naturezas, etc.) | — | — | ~50KB | Carregado no boot do site |

## Princípio de extensão

> Cada Parquet otimiza **um padrão de acesso**. Se um padrão pode ser servido pelo full-scan barato de um Parquet existente, não cria outro.

Padrões adicionais ficam em `experiments/` como hipóteses até a UI demandar:

- `experiments/003-geo-index/` — sorted por UF/Município
- `experiments/004-cnae-index/` — sorted por CNAE
- `experiments/005-grafo-relacionamentos/` — sócio↔empresa bidirecional

## Consequências

- ✅ Lookup por CNPJ exato: ~1-2MB de download (1 row group de `cnpjs.parquet`).
- ✅ Busca por nome: ~50-150MB de `raizes.parquet`, depois ~1-2MB pra detalhes.
- ✅ Sócios: ~1-2MB de `socios.parquet`.
- ✅ Sem partitioning: layout simples, ETL simples, manifest simples.
- ⚠️ ETL produz 3 Parquets em vez de 1 — complexidade ~30% maior, vale a pena.
- ⚠️ `cnpjs.parquet` denormaliza dados da raiz (razão social repete em filiais) — tradeoff aceito (ver [ADR 0009](0009-denormalization-and-roundtrip.md)).

## Manifest

`web/public/manifest.json` registra os três Parquets + lookups por snapshot:

```json
{
  "current": "2026-01",
  "snapshots": [{
    "date": "2026-01",
    "schema_version": "1.0.0",
    "files": {
      "cnpjs":   { "url": "...cnpjs.parquet",   "sha256": "...", "size": 3000000000 },
      "raizes":  { "url": "...raizes.parquet",  "sha256": "...", "size": 150000000 },
      "socios":  { "url": "...socios.parquet",  "sha256": "...", "size": 500000000 },
      "lookups": { "url": "...lookups.json",    "sha256": "...", "size": 50000 }
    }
  }]
}
```

## Alternativas consideradas e rejeitadas

- **Partitioning por UF / por raiz_prefix** — ver [ADR 0011](0011-no-partitioning.md).
- **Parquet único monolítico** — força full-scan da coluna `razao_social` (~200MB+) em qualquer busca por nome.
- **Tabela única por entidade RFB (mesmo layout do dump)** — força joins no client, performance pior que denormalizado.
