# ADR 0002 — Não adotar TanStack DB

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

TanStack DB ([tanstack.com/db](https://tanstack.com/db/latest)) é um reactive store para dados de API com sync, mutações otimistas e live queries. Avaliado como possível camada de dados do FICHA.

## Decisão

**Não adotar.** O FICHA não tem os problemas que TanStack DB resolve.

## Justificativa

| Feature do TanStack DB | Aplicabilidade no FICHA |
|---|---|
| Sync com backend (ElectricSQL/PowerSync) | ❌ Não há backend; dados moram estáticos no Internet Archive |
| Mutações otimistas + rollback | ❌ App é read-only; usuário não muta nada |
| Live queries com differential dataflow | ❌ Engine analítico já é DuckDB-WASM, infinitamente mais potente para Parquet |
| Reactive store para API | ❌ Camada atômica é um único `fetch` por CNPJ |

Differential dataflow numa collection in-memory **não escalaria** para 60M+ de CNPJs. DuckDB-WASM faz HTTP range requests e processa Parquet sem carregar tudo.

## Consequências

- ✅ Stack mais simples; menos uma abstração.
- ✅ Coerência com vision "static-first, zero-backend".
- 🔄 Reabrir decisão se o FICHA virar SaaS com login + anotações privadas.

## Alternativas para casos onde o TanStack DB *poderia* caber

- Favoritos / watchlist: `localStorage` + Svelte stores nativos resolvem em ~5kb.
- Histórico de busca: idem.
- Cache de fichas atômicas fetchadas: ver [ADR 0007](0007-tanstack-query-atomic-only.md).
