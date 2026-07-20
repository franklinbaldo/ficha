# GitHub Workflows

| Workflow | Trigger | O que faz |
|---|---|---|
| [`ci.yml`](ci.yml) | PR / push em `main` | `astro check` + build do `web/`; lint + tests do `etl/`. Path-filtered: cada job só roda se sua pasta mudou. |
| [`deploy.yml`](deploy.yml) | Push em `main` (paths `web/**`) | Build do Astro e deploy no GitHub Pages. |
| [`etl-monthly.yml`](etl-monthly.yml) | Cron mensal (dia 5, 03:00 UTC) + manual | Roda o pipeline ETL para o mês alvo, publica no Internet Archive, commita `manifest.json` atualizado em `main` (que dispara `deploy.yml`). |
| [`canonical-shadow-history.yml`](canonical-shadow-history.yml) | Manual + smoke offline em PR | Baixa um `EstabelecimentosN.zip` histórico do mirror IA, executa o writer canônico em shadow, preserva checksums/métricas e publica apenas um artifact temporário. Não altera o ETL mensal nem produtos públicos. |

## Setup necessário

### Para `deploy.yml`
- Repository Settings → Pages → **Source: GitHub Actions**

### Para `etl-monthly.yml`
- Conta no Internet Archive com S3-like keys
- Repository Secrets:
  - `IA_ACCESS_KEY`
  - `IA_SECRET_KEY`

`canonical-shadow-history.yml` usa somente leitura pública do mirror IA e não exige secrets.

## Princípios

- **Path filters por subprojeto** (ver [ADR 0005](../../docs/adr/0005-monorepo-web-etl.md)): mudar `etl/` não dispara CI do `web/`.
- **ETL → manifest → deploy**: o cron mensal faz commit do manifest atualizado, que naturalmente dispara o deploy do site. Sem encadeamento explícito de workflows.
- **Workflow_dispatch** em todos os que fazem sentido — facilita debug manual.
