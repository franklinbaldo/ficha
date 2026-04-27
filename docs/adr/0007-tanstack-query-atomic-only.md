# ADR 0007 — TanStack Query só para camada atômica

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

FICHA tem duas camadas de acesso a dados:

1. **Atômica** — `GET /{uf}/{cnpj}.json` no Internet Archive (resposta única, JSON).
2. **Analítica** — DuckDB-WASM consultando Parquet remoto via SQL.

TanStack Query (`@tanstack/svelte-query`) gerencia dedup, cache, retry, prefetch, suspense.

## Decisão

- **Sim**, usar TanStack Query para a **camada atômica** e qualquer fetch HTTP auxiliar (manifest, lista de UFs).
- **Não**, não envolver consultas SQL do DuckDB-WASM em TanStack Query.

## Justificativa

### Camada atômica
Caso de uso textbook do TanStack Query:

```ts
useQuery({
  queryKey: ['ficha', cnpj],
  queryFn: () => fetch(`https://archive.org/.../${uf}/${cnpj}.json`).then(r => r.json()),
  staleTime: Infinity, // snapshot é imutável (ver ADR 0003)
})
```

Ganhos: dedup, cache, prefetch on hover, suspense, devtools.

### Camada analítica (rejeitado)
- DuckDB-WASM tem buffer manager próprio que cacheia páginas Parquet via HTTP range. Envolver em Query duplica cache.
- `queryKey` = hash de SQL é desajeitado.
- Resultados (Arrow tables) podem ser grandes; não cabem confortavelmente no cache do Query.
- Estado de loading/error de uma query SQL é resolvido por um simples `$state` Svelte.

## Consequências

- ✅ Dois "drivers" claros: `web/src/lib/atomic.ts` (TanStack Query) e `web/src/lib/analytical.ts` (DuckDB direto).
- ✅ Separação de preocupações; cada camada otimizada para seu padrão de acesso.
- ⚠️ Bundle adiciona TanStack Query (~12kb gzip) — aceitável.

## Alternativas

- **TanStack DB**: rejeitado em [ADR 0002](0002-no-tanstack-db.md).
- **SWR / native fetch + cache manual**: TanStack Query é mais maduro, e o autor já conhece.
- **Apenas fetch + Svelte stores**: viável, mas perde devtools, retry com backoff, suspense.
