# Architecture Decision Records

Decisões arquiteturais do FICHA, registradas no momento em que são tomadas.

Formato: cada ADR é um markdown curto (ideal: 1 página) com **contexto**, **decisão**, **consequências**. Numerados sequencialmente. Nunca editar um ADR após "Accepted" — se mudar de ideia, escrever novo ADR que **supersedes** o antigo.

## Status

| # | Título | Status |
|---|---|---|
| [0001](0001-astro-svelte-stack.md) | Stack frontend: Astro + Svelte 5 + TS + Zod | Accepted |
| [0002](0002-no-tanstack-db.md) | Não adotar TanStack DB | Accepted |
| [0003](0003-schema-versioning.md) | Versionamento de schema em três camadas | Accepted |
| [0004](0004-internet-archive-as-storage.md) | Internet Archive como storage primário | Accepted |
| [0005](0005-monorepo-web-etl.md) | Monorepo `web/` + `etl/` simétrico | Accepted |
| [0006](0006-data-quality-pragmatic.md) | Validação pragmática (não Great Expectations) | Accepted |
| [0007](0007-tanstack-query-atomic-only.md) | TanStack Query só para camada atômica | Accepted |
| [0008](0008-three-parquet-architecture.md) | Arquitetura de três Parquets + lookups.json | Accepted |
| [0009](0009-denormalization-and-roundtrip.md) | Denormalização e roundtrip-equivalence como gate | Accepted |
| [0010](0010-rfb-source-url.md) | Fonte: dumps RFB em arquivos.receitafederal.gov.br | Accepted |
| [0011](0011-no-partitioning.md) | Não usar partitioning Hive-style nos Parquets | Accepted |
| [0012](0012-ia-mirror-as-source-of-truth.md) | Internet Archive como source-of-truth do FICHA | Accepted |
| [0013](0013-rfb-token-discovery.md) | ~~Estratégia de discovery do token Nextcloud da RFB~~ | Superseded by 0014 |
| [0014](0014-rfb-flat-url-no-token.md) | RFB usa URL flat sem token; histórico fica no mirror IA | Accepted |
