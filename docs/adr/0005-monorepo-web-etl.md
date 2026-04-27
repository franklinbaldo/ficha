# ADR 0005 — Monorepo `web/` + `etl/` simétrico

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

FICHA tem dois subprojetos com linguagens, ciclos de release e targets de deploy distintos:

- **Frontend** (TypeScript/Astro) → GitHub Pages, deploy a cada push em `main`.
- **ETL** (Python) → GitHub Actions cron mensal, publica no Internet Archive.

Onde colocar cada um?

## Decisão

**Monorepo simétrico** com `web/` e `etl/` paralelos, cada um auto-contido com sua build, deps e config próprias.

```
ficha/
├── web/             # Astro project (package.json aqui)
│   ├── src/         # convenção JS — Astro encontra src/pages/
│   ├── public/
│   └── astro.config.mjs
├── etl/             # Python project (pyproject.toml aqui)
│   ├── src/         # convenção Python "src layout"
│   │   └── ficha_etl/
│   └── pyproject.toml
├── experiments/     # PoCs numerados, versionados em main
└── docs/
```

**Único contrato entre `web/` e `etl/`:** schema do Parquet (declarado em `web/src/schemas/vN/`) e formato do `manifest.json`. Ninguém em `web/` importa de `etl/`, e vice-versa.

## Por que não cada um em seu padrão

### Rejeitado: `src/etl/` para Python + `web/` para frontend
`src/` é convenção carregada em projetos JS — Astro/Vite assumem que `src/` é a raiz do pacote. Misturar `src/etl/` (Python) com `web/` deixa `src/` órfão e mistura duas culturas.

### Rejeitado: `apps/web/` + `apps/etl/`
Estilo Turborepo só faz sentido com 3+ pacotes JS compartilhando código. Cerimônia desnecessária aqui.

### Rejeitado: dois repos separados
- Schema Zod (em `web/`) descreve Parquet gerado pelo ETL — mudança em um afeta o outro. Monorepo facilita correlação.
- Custo de manter dois repos, dois CIs, dois READMEs > custo de uma pasta extra.

## Consequências

- ✅ Cada subprojeto self-contained — `cd web && bun dev` ou `cd etl && uv run ficha-etl ...`.
- ✅ Adicionar terceiro projeto futuro (ex.: `cli/` em Rust) é trivial.
- ✅ Cada um respeita suas convenções nativas.
- ⚠️ Workflows GH Actions precisam `working-directory: web` ou `etl` em cada step.
- ⚠️ Precisa de path filters em CI (`paths: ["web/**"]`) para não rodar tudo em qualquer push.

## Reversibilidade

Separar em dois repos é mecânico (`git filter-branch` ou `git subtree split`). Acoplamento baixo.
