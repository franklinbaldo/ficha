# ADR 0023 — `enderecos.parquet`: reverse lookup por endereço e município

**Status:** Aceito  
**Data:** 2026-05-26  
**Contexto:** docs/perf-plan-2026-05.md §7

---

## Contexto

`cnpjs.parquet` é sorted por `cnpj` e não oferece pruning eficiente para queries
do tipo "todos os CNPJs em Av. Paulista 1000" ou "todos no município 7107".
Essas consultas fazem full scan de ~1 GB. O roadmap M4 prevê parquets
especializados para padrões de acesso invertidos.

## Decisão

Produzir `enderecos.parquet` com o seguinte shape:

| Coluna | Tipo | Notas |
|--------|------|-------|
| `uf` | VARCHAR | Código UF (2 chars) |
| `municipio_codigo` | VARCHAR | Código RFB do município |
| `logradouro_normalizado` | VARCHAR | UPPER + strip_accents + expansão de abreviações |
| `numero` | VARCHAR | Número do logradouro (nullable) |
| `cep` | VARCHAR | CEP (nullable) |
| `bairro` | VARCHAR | Bairro (nullable) |
| `cnpj` | VARCHAR(14) | CNPJ completo sem formatação |

**Sort:** `(uf, municipio_codigo, logradouro_normalizado, numero)`  
**Fonte:** `estabelecimento` only — sem joins, ~10 min / ~2 GB peak.

## Normalização de logradouro

Abordagem **vetorizada**: CTE computa a base normalizada uma vez por linha
(`UPPER(strip_accents(TRIM(regexp_replace(logradouro, '\s+', ' ', 'g'))))`) e
depois uma única extração de prefixo + lookup em MAP DuckDB expande abreviações:

```sql
COALESCE(
  MAP {'R': 'RUA ', 'AV': 'AVENIDA ', ...}[regexp_extract(_logr, '^([A-Z]+)\.?\s+', 1)]
  || regexp_replace(_logr, '^[A-Z]+\.?\s+', ''),
  _logr
)
```

Os top-10 prefixos cobrem ≥90% da variação (R, AV, TV, AL, PCA, PC, EST, ROD, VL, LG).
Não são feitas deduplicações fuzzy em v1 — "R DAS FLORES" e "RUA DAS FLORES" colapsam;
grafias genuinamente distintas permanecem distintas.

## Padrões de acesso servidos

| Query | Mecanismo |
|-------|-----------|
| `WHERE uf='SP' AND municipio_codigo='7107'` | Sort prefix → pruning por min/max |
| `WHERE uf=? AND municipio_codigo=? AND logradouro_normalizado=? AND numero=?` | Sort prefix + range |
| `WHERE uf=? AND municipio_codigo=? AND logradouro_normalizado LIKE 'PAULISTA%'` | Sort prefix + sequential scan parcial |

## Consequências

- +1 write no phase 3 do ETL (~10 min, ~2 GB peak — menor que `cnpjs`)
- Manifest ganha entrada `enderecos` com metadata de sort
- Frontend usa `attachEnderecos(db, url)` para registrar e criar VIEW
- Schema Zod em `web/src/schemas/v1/endereco.ts`
- **Não depreca** `cnpjs.parquet` — padrões de acesso complementares
