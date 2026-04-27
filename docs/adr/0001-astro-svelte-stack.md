# ADR 0001 — Stack frontend: Astro + Svelte 5 + TS + Zod

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

O FICHA é um app estático que consulta dados de CNPJ. A escolha de stack precisa equilibrar: peso da página (DuckDB-WASM já adiciona ~5MB), DX, e coerência com outros projetos do autor.

## Decisão

Adotar **Astro + Svelte 5 + TypeScript + Zod**:

- **Astro** como meta-framework: renderização estática por default, ilhas para interatividade (`client:load`), suporte nativo a múltiplos UI frameworks. Coerência com outros projetos do autor.
- **Svelte 5** para componentes interativos: runes (`$state`, `$derived`) com bundle menor que React, JSX-free.
- **TypeScript strict** para tipos.
- **Zod** para validação runtime de dados externos (Parquet, JSON do IA).

## Consequências

- ✅ Build estático → GitHub Pages direto, zero servidor.
- ✅ Bundle pequeno fora das ilhas (`SearchCNPJ.svelte` é a única ilha pesada).
- ⚠️ Svelte 5 ainda é recente; ecossistema pequeno comparado a React.
- ⚠️ Astro Content Collections só serão usadas se houver docs renderizadas no site.

## Alternativas consideradas

- **SvelteKit static**: viável, mas Astro vence pela coerência com outros repos.
- **Next.js / Remix**: exigem runtime servidor; conflitam com a vision "static-first".
- **Vanilla Vite + Svelte**: mais leve, mas perde benefícios do Astro (otimização de imagens, MDX, sitemap, SSG).
