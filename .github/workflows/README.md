# Workflows do FICHA

Inventário da `main` em **21/11/2026**: **16 workflows YAML** após remoção de `etl-bootstrap.yml` na [#275](https://github.com/franklinbaldo/ficha/pull/275). Os gatilhos descrevem configuração, não comprovam execução de um teste ou publicação.

## Site e gates de PR

| Workflow | Eventos | Proteção e limites |
|---|---|---|
| [`deploy.yml`](deploy.yml) | PR e push `main` em `site/**`, manifesto ou workflow; manual | **Pages SSG Rust** (`site/`): testes Node, build e identidade de assets. Deploy/HTTP `verify` só fora de PR. Não prova CNPJ/Parquet remoto via CORS+Range ([#208](https://github.com/franklinbaldo/ficha/issues/208)). |
| [`visual-capture.yml`](visual-capture.yml) | PR seletiva do site/auditoria; manual | Chromium/Playwright e axe para acessibilidade do SSG; não comprova consulta remota. |
| [`ci.yml`](ci.yml) | Toda PR; push `main` | Jobs com filtro **interno**: ETL Linux/Windows, `ficha-py/` e `web/`. Desde a [#286](https://github.com/franklinbaldo/ficha/pull/286), mudanças específicas no wire Python/Protobuf ativam também Vitest (`web/`), e mudanças no wire TypeScript/manifesto ativam também ETL Linux e Windows. O job `web/` cobre **Astro legado**, não o site publicado. `SUCCESS` com testes `SKIPPED` não significa testes executados. |
| [`okf-contract.yml`](okf-contract.yml) | PR/push `main` seletivos em schemas/knowledge | Compatibilidade de contratos OKF, SQL, Zod e v1. |

## ETL, dados e publicação

| Workflow | Eventos | Proteção e limites |
|---|---|---|
| [`etl-smoke.yml`](etl-smoke.yml) | PR em ETL; segunda-feira 06:00 UTC; manual | PR usa **fixtures HTTP**; consultas reais RFB/IA/freshness ficam puladas. Sondas externas pertencem ao semanal/manual, não publicam dados. |
| [`etl-streaming-benchmark.yml`](etl-streaming-benchmark.yml) | PR em ETL; manual | Prova de corretude/smoke da trilha streaming W3.1, não ETL mensal real. |
| [`etl-monthly.yml`](etl-monthly.yml) | Cron `0 3 5,10,15,20 * *` UTC; manual | Snapshot/IA/manifesto. **Contenção produtiva:** job `resolve` possui `if: github.event_name == 'workflow_dispatch'`; ticks de cron não executam o pipeline. Não disparar manualmente sem autorização operacional. |
| [`release-monthly-data.yml`](release-monthly-data.yml) | `workflow_run` do mensal; push `main` do manifesto | Job só libera `workflow_run` com conclusão `success` ([#271](https://github.com/franklinbaldo/ficha/issues/271)); push do manifesto **não** autoriza release. |
| [`canonical-empresa-history.yml`](canonical-empresa-history.yml) | PR seletiva; manual | Smoke em PR; investigação manual de histórico canônico de empresas. |
| [`canonical-shadow-history.yml`](canonical-shadow-history.yml) | PR seletiva; manual | Smoke em PR; estudo histórico de estabelecimentos em shadow. |
| [`canonical-simples-history.yml`](canonical-simples-history.yml) | PR seletiva; manual | Smoke em PR; estudo histórico do Simples. |
| [`estabelecimento-key-audit.yml`](estabelecimento-key-audit.yml) | PR seletiva; manual | Smoke em PR; unicidade de chaves entre partes históricas. |
| [`etl-evidence.yml`](etl-evidence.yml) | PR seletiva em benchmark; manual | Evidência de desempenho; o perfil manual pode ser pesado e não altera dados publicados. |

## Experimentos e diagnósticos especiais

| Workflow | Eventos | Proteção e cautela |
|---|---|---|
| [`_tmp-ovigia-first-story-probe.yml`](_tmp-ovigia-first-story-probe.yml) | Push na branch `ops/ovigia-first-story-probe`; manual | Probe experimental, candidato a retirada após decisão explícita. |
| [`claude-diagnostics.yml`](claude-diagnostics.yml) | Push em duas branches `claude/*` específicas; manual | Harness antigo de diagnóstico com credenciais IA; revisar necessidade antes de executar/retirar. |
| [`probe-unzip-meta.yml`](probe-unzip-meta.yml) | Push em `probe/run-unzip-meta*`; manual | Experimento operacional com item IA descartável; **não** é gate de PR, não disparar sem autorização. |

## Configuração e fronteiras de prova

- **Pages:** Settings → Pages → **Source: GitHub Actions**. Push normal em `main` nos caminhos observados por `deploy.yml` inicia a publicação. O manifesto canônico permanece em `web/public/manifest.json`.
- **ETL:** `IA_ACCESS_KEY` e `IA_SECRET_KEY` pertencem aos secrets das operações autorizadas. `workflow_dispatch` não constitui autorização.
- **CI exato:** conferir o SHA de HEAD e os **steps executados**; não contar `SKIPPED` como cobertura. O leitor Astro em `web/` e o site SSG em `site/` são consumidores distintos.
- **Cobertura que sai ou muda:** [#275](https://github.com/franklinbaldo/ficha/pull/275) retirou só o bootstrap one-shot; `etl-monthly.yml`, cron, manifesto e gates foram preservados. [#280](https://github.com/franklinbaldo/ficha/pull/280) colocou fixtures no smoke de PR, preservando sondas semanais. A [#286](https://github.com/franklinbaldo/ficha/pull/286) foi **integrada em 21/11/2026**, squash [`3a5970ed`](https://github.com/franklinbaldo/ficha/commit/3a5970ed750f36095494c9732ed10ac8dabe0d4a): acrescentou replays cruzados seletivos Python/Protobuf ↔ TypeScript/Vitest, **sem retirar gates ou checks**. Não adicionou teste do site SSG, cuja verificação continua em `deploy.yml` e `visual-capture.yml`. O [CI pós-merge #44927852890](https://github.com/franklinbaldo/ficha/actions/runs/44927852890) concluiu com quatro jobs e steps `Test` executados com sucesso, mas o diff de workflow aciona todas as suítes: **não mede custo marginal de uma mudança isolada no wire**. Para medir, comparar runs com caminho e cobertura executada equivalentes, separando tempo de ciclo da soma de durações dos jobs ([método #268](https://github.com/franklinbaldo/ficha/issues/268#issuecomment-7052643123)). Outras remoções dependem de decisão na [#268](https://github.com/franklinbaldo/ficha/issues/268).
- **Este README é documental:** modificá-lo não executa ETL produtivo, publicação ou mudança de contrato. A aferição de custo dos filtros cruzados permanece separada na [#268](https://github.com/franklinbaldo/ficha/issues/268).

Ver também [ADR 0005](../../docs/adr/0005-monorepo-web-etl.md), [site/README.md](../../site/README.md) e [Cafezinho](https://github.com/franklinbaldo/cafezinho/issues/21).
