# ADR 0016 — Estratégia de backfill dos 35 snapshots históricos

**Status:** Proposed
**Data:** 2026-05-07

## Contexto

[ADR 0015](0015-nextcloud-webdav-canonical.md) confirmou que o Nextcloud
WebDAV da RFB tem 35 meses de histórico (2023-05 → 2026-04). O cron mensal
([etl-monthly.yml](../../.github/workflows/etl-monthly.yml)) só captura snapshots
prospectivamente, e o bootstrap (PR #24) processou um único mês (2026-04). Falta
decidir como popular os 34 meses prévios no Internet Archive para que o site
ofereça histórico completo desde a abertura da base.

Restrições materiais:

- **Volume:** 35 × ~7 GB = ~245 GB de upload pra IA. ~25–30 GB de extração efêmera por mês durante o transform.
- **Tempo:** o bootstrap demorou ~30 min só pro stream de 1 mês; a transform/upload empilhada deve dar ~45–60 min total por mês. Sequencial: ~25–35 horas.
- **Limite do runner:** GH Actions tem timeout de 6 h (350 min) por job. Um único job não consegue rodar mais de ~5 meses sequenciais com folga.
- **Limite de concorrência:** GH Actions free tier permite ~20 jobs paralelos por repo; runners ubuntu-latest têm ~14 GB de disco livre, suficiente pra um snapshot extracted (~25–30 GB) só após `Free disk space`.
- **Confiabilidade externa:** RFB Nextcloud serve range requests; IA S3 às vezes retorna 5xx/409 (retry com backoff já implementado em PR #24).
- **Custo de re-execução:** stream falhar parcial deixa ZIPs no IA → próxima tentativa pula via fetcher chain → desperdício zero. Transform falhar → re-roda do IA, sem voltar pra RFB.

## Decisão

Adotar uma estratégia de **matrix-fan-out por mês** num workflow dedicado
`backfill.yml`, com **manifesto reconstruído ao final** a partir do que de fato
está no IA.

### Workflow

```
backfill.yml
├─ workflow_dispatch
│   ├─ from_month: YYYY-MM (default: oldest na RFB, e.g. 2023-05)
│   └─ to_month:   YYYY-MM (default: latest-1 na RFB, exclui o mês corrente já no manifest)
│
├─ job: plan
│   └─ Lista meses do range, cruza com manifest existente, emite matrix
│       de meses faltantes (saída JSON pra strategy.matrix dinâmica)
│
├─ job: snapshot   (matrix sobre os meses faltantes, max-parallel: 5)
│   └─ Roda `ficha-etl run --month <M>` por mês — exatamente como o
│       etl-monthly faz hoje, mas SEM commitar manifest.json. A saída
│       canônica fica no item ia:ficha-<M>.
│
└─ job: rebuild-manifest   (needs: snapshot, if: success())
    └─ Lê todos os ficha-* via IA metadata API, reconstrói
        web/public/manifest.json do zero, commita + push pra branch
        do PR (mesmo padrão [skip ci] do etl-bootstrap)
```

### Por que matrix em vez de loop sequencial

- 35 meses ÷ 5 paralelos = 7 lotes ≈ 5–8 horas wall-time vs 25–35 sequencial.
- Falhas isoladas por mês — um snapshot corrompido não derruba os outros.
- Re-execução é trivial: `workflow_dispatch` com `from_month` e `to_month`
  apontando para o range que falhou.

### Por que não atomizar stream/transform/upload em jobs separados

Discutido em #24: o handoff entre jobs custa setup duplicado (uv, free-disk,
sync) e força state via artifacts ou IA. Mas o IA **já é o bus**: stream
escreve em `ia:ficha-<M>/raw/`, transform lê de lá, upload escreve
`ia:ficha-<M>/{cnpjs,raizes,socios}.parquet`. Atomizar no GHA não adiciona
nada que o fetcher chain (`local cache → IA mirror → RFB upstream`)
não dê de graça via retomada idempotente.

### Por que reconstruir o manifest do IA, não acumular incrementalmente

- **Concorrência:** 5 jobs paralelos commitando manifest.json conflitam em
  push. Rebase loops dentro do job ficaram complicados no etl-monthly atual.
- **Source of truth:** o que existe no IA é a verdade; o manifest é estado
  derivado. ADR 0012 já estabeleceu IA como source-of-truth — coerente.
- **Idempotência:** `rebuild-manifest` pode rodar standalone (`workflow_dispatch`)
  para corrigir desvios entre IA e manifest sem rodar o pipeline.

### `max-parallel: 5`

Conservador propositalmente:

- RFB Nextcloud não anuncia rate-limits, mas 5 GETs simultâneos × 4 workers
  internos = 20 streams concorrentes. Mais que isso é potencialmente abusivo.
- IA S3 tolera mais, mas o gargalo prático costuma ser o GET da RFB.
- Se um mês trava, 4/5 dos slots ficam livres pra restantes.

## Consequências

- ✅ **Histórico completo desde 2023-05** disponível em wall-time ~5–8 h.
- ✅ Reuso 100% do `ficha-etl run` existente — zero código novo no ETL.
- ✅ Tolera falhas transientes via retry já implementado em PR #24.
- ✅ Re-execução é declarativa (`from_month`/`to_month`) e idempotente.
- ✅ Manifest sempre coerente com IA via job de rebuild.
- ⚠️ ~245 GB de upload para o Internet Archive. IA é gratuito mas não infinito —
  vale checar com a equipe do IA antes de disparar (cortesia, não obrigação).
- ⚠️ Custo computacional GH Actions: ~35 × 1 h CPU = ~35 h, dentro do free tier
  mensal de repos públicos (~3000 min/mês de GHA hosted).
- ⚠️ `rebuild-manifest` quebra se IA estiver fora do ar (502/timeout —
  visto em PR #24). Mitigação: retry loop com backoff no curl, falhar o job
  com diagnóstico claro pra re-execução manual.

## Alternativas consideradas e rejeitadas

- **Loop sequencial num único job de 6 h.** Cabe ~5 meses, precisaria de 7
  invocações manuais. Frágil e tedioso.
- **Matrix com max-parallel = 20 (free tier máximo).** Risco de hammering
  desnecessário na RFB; ganho de tempo marginal vs 5 paralelos (5 h vs 8 h).
- **Atomização stream/transform/upload em jobs separados.** Já discutido
  acima — IA já serve como bus, atomização adiciona complexidade sem benefício.
- **Backfill via local + script manual `for m in months; do ficha-etl run; done`.**
  Funciona, mas não documentado, sem retry, sem rebuild de manifest. Workflow
  versionado é melhor.
- **Manifest acumulado com rebase loop por job.** Prototipado em
  etl-monthly.yml; funciona pra 1 commit/mês, mas vira corrida com 5 paralelos.
  Rebuild final é estritamente mais simples.

## Próximos passos

1. Implementar `.github/workflows/backfill.yml` conforme acima.
2. Adicionar comando `ficha-etl rebuild-manifest --from-ia` que faz o trabalho
   do job final (single-source-of-truth pro próprio manifesto).
3. Disparar dry-run com `from_month=2026-04 to_month=2026-04` (1 mês,
   replica o bootstrap) pra validar o pipeline antes de soltar nos 34.
4. Disparar backfill completo. Monitorar via PR comments emitidos pelo workflow.
