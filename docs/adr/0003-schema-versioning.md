# ADR 0003 — Versionamento de schema em três camadas

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

A RFB altera o layout dos dumps de CNPJ ocasionalmente (campos novos, renomeações). FICHA publica snapshots mensais imutáveis no Internet Archive — snapshots antigos precisam continuar legíveis para sempre. Precisamos de uma estratégia de versionamento que:

1. Não exija reprocessar histórico quando RFB mudar layout.
2. Permita ao frontend descobrir automaticamente qual schema usar para um dado snapshot.
3. Não dependa de servidor / catálogo dinâmico (anti-thesis de "static-first").

## Decisão

Versionamento em **três camadas complementares**:

### 1. Versão embutida no Parquet (footer KV-metadata)
```
ficha.schema_version = "1.0.0"
ficha.snapshot_date = "2026-01"
ficha.rfb_layout_date = "2023-05"
ficha.row_count = 62847391
```
Lido via `parquet_kv_metadata()` do DuckDB. Custo zero, colado ao dado.

### 2. Manifest público (single source of truth)
`web/public/manifest.json` commitado no repo, atualizado a cada snapshot:
```json
{
  "current": "2026-01",
  "snapshots": [
    { "date": "2026-01", "schema_version": "1.0.0", "url": "...", "sha256": "..." }
  ]
}
```
Frontend faz **um fetch** no boot e descobre snapshots disponíveis.

### 3. Schemas Zod versionados e imutáveis
```
web/src/schemas/
  v1/   ← nunca edite após publicado
  v2/   ← criar quando RFB quebrar layout
  registry.ts  ← lookup por versão
```

**SemVer:**
- *patch*: campo opcional novo
- *minor*: campo obrigatório com default
- *major*: campo removido ou tipo mudou

### Para queries cruzando versões
Função pura `migrations/v1_to_v2.ts` aplica conversão on-the-fly no client. Não regrava Parquet histórico.

## Consequências

- ✅ Snapshots históricos imutáveis e sempre legíveis.
- ✅ Sem dependência de Iceberg/Delta (mantém static-first).
- ✅ Site funciona se o dev sumir 6 meses (manifest + parquets seguem no IA).
- ⚠️ Duplicação de schemas entre Zod (TS) e validação no ETL Python — mitigado em [ADR 0006](0006-data-quality-pragmatic.md).
- ⚠️ Manifest commitado significa um commit extra por snapshot mensal — aceitável.

## Alternativas consideradas

- **Iceberg/Delta Lake**: requer catálogo + writes coordenados. Anti-thesis.
- **Schema único evolutivo**: força migração destrutiva quando RFB mudar.
- **Schema em arquivo lateral (não no Parquet)**: separa metadado do dado, fragiliza.
