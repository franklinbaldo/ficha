# FICHA — O que está impedindo o projeto de alcançar sua visão

**Data:** 2026-07-13 · **HEAD de referência:** `06d4f91d`

A visão (README + ADRs): um Data Lakehouse serverless de CNPJ com **snapshots
mensais automáticos** no Internet Archive, **histórico completo desde 2023-05**,
**camada atômica por CNPJ**, **camada analítica rica** no navegador (Pessoa,
Endereço, CNAE, grafo de sócios) e **rastreamento temporal** de alterações
cadastrais ("Rolodex digital").

Este documento identifica os bloqueios, por dimensão, com evidências. O achado
central inverte a intuição: **o código está adiantado em relação à visão; os
dados estão 3 meses atrasados em relação ao código.** O gargalo não é falta de
features — é a operação do pipeline que nunca fechou o ciclo.

---

## TL;DR — os 3 bloqueios que dominam tudo

1. **A fábrica de snapshots não funciona.** O `etl-monthly.yml` tem 6 execuções
   na história e **zero sucessos**. Os crons de maio, junho e julho falharam
   todos no mesmo ponto: `ficha-etl list-snapshots` retornou **vazio** no runner
   (com o erro suprimido por `2>/dev/null` até o fix de 2026-07-12). Enquanto
   isso, o upstream da RFB **está no ar e já publicou 2026-05, 2026-06 e
   2026-07** — verificado em 2026-07-13 via `PROPFIND` com o token conhecido
   (`upstream.py:43`), que respondeu HTTP 207 listando as três pastas. Ou seja:
   **há 3 meses de dados disponíveis upstream que o projeto não capturou por
   falha própria de resolução do mês, não por atraso da RFB.**

2. **Existe um único snapshot, incompleto, gerado semi-manualmente.** O
   `manifest.json` publicado tem só `2026-04` (gerado 2026-05-15), com **3 dos 7
   parquets** que o ETL atual produz — `cnpj_contatos` e `cnpj_cnaes` deram 404
   no item do IA (`manifest.json`, `_comment_2026-04`) e `enderecos`/`pessoas`
   nem existiam à época. Esse snapshot não veio do cron nem do bootstrap em
   `main`: veio de um caminho de diagnóstico (PR #41). `git log
   --author=ficha-etl-bot` é vazio — o bot de publicação nunca commitou nada.

3. **O histórico — núcleo do conceito "Ficha" — não existe.** O backfill dos 34
   meses anteriores (ADR 0016) está `Proposed` desde 2026-05-07: `backfill.yml`
   não existe, `ficha-etl rebuild-manifest --from-ia` não existe, e o frontend
   não tem nenhuma UI temporal. 1/35 meses publicado. Sem série histórica, o
   "rastreamento de alterações cadastrais via snapshots mensais" (README:61) é
   apenas texto.

---

## Dimensão 1 — Operação do pipeline (blocker nº 1)

**Fatos:**

- `etl-monthly.yml`: 6 runs, 0 sucessos (3 dispatch/cancelados em 2026-04, 3
  crons agendados em 2026-05, 2026-06 e 2026-07 (todos dia 5) — todos
  `failure`).
- Modo de falha idêntico nos 3 crons: `MONTH=$(uv run ficha-etl list-snapshots
  2>/dev/null | ...)` → saída vazia → `Invalid month ''`. O `2>/dev/null`
  engoliu a causa-raiz por 3 meses seguidos.
- Teste direto em 2026-07-13 (fora do runner): `PROPFIND
  https://arquivos.receitafederal.gov.br/public.php/webdav/` com o token
  `KNOWN_TOKENS[0]` → **HTTP 207**, pastas até `2026-07/` presentes. O token não
  rotacionou e o serviço está no ar. A falha é específica do ambiente do runner
  (bloqueio de IP do Azure/GH Actions, timeout, ou bug no cliente) — **causa
  ainda não diagnosticada** porque o erro era descartado. O fix `2ef1e6cd`
  (2026-07-12) passou a expor o stderr e tentar 4×/mês (dias 5/10/15/20); a
  próxima janela (2026-07-15) é a primeira chance de ver o erro real.
- O pipeline completo (download RFB → transform → upload IA → manifest) **nunca
  foi confirmado de ponta a ponta em `main`** (`docs/perf-plan-2026-05.md`,
  "Status of the bootstrap OOM"). O OOM estrutural da fase 3 foi mitigado por
  chunk-per-ZIP (`transform.py:738`, pico ~5 GB vs ~70 GB), mas a validação em
  escala real segue pendente.
- Fragilidade do upload ao IA já causou dano real: parquets declarados no
  manifest que nunca subiram (404), headers S3 com Unicode que derrubavam o PUT
  (`7a817f98`), 5xx/409 intermitentes. O guard `verify_snapshot_files()`
  (`manifest.py:147`) existe hoje, mas nasceu do incidente.

**Por que isso trava a visão:** todas as camadas prometidas (histórico, atômica,
analítica) dependem de snapshots saindo todo mês. Hoje a taxa de sucesso do
mecanismo automático é 0%.

**Destravamento imediato disponível:** `workflow_dispatch` do `etl-monthly` com
`inputs.month=2026-05` (e depois 06/07) contorna a resolução automática — o
operador fornece o mês e o pipeline segue. Isso também é o teste E2E que falta.

## Dimensão 2 — Infraestrutura (teto do free tier)

- Runner GH Actions: 16 GB RAM, ~14 GB de disco livre (o workflow apaga
  .NET/Android/Swift para caber — `etl-monthly.yml:44-51`), 6 h de timeout
  (orçamento interno: 350 min).
- Consequências: `threads=1` load-bearing (`transform.py:350`, `:1493-1502`),
  wall-time longo, engenharia contorcida (streaming RFB→IA em chunks de 1 MiB,
  deleção agressiva de CSVs no meio do transform).
- Backfill de 35 meses ≈ 245 GB de upload e ~35 h de computação (ADR 0016) — 
  viável no free tier via matrix, mas sem margem para erro.
- GitHub Pages exige habilitação manual em Settings (o `GITHUB_TOKEN` não pode
  fazer enablement — `deploy.yml:30-34`); já causou ciclos de fix/revert
  (`641090e9` → `25052d12`).

**Por que isso trava a visão:** cada run é uma operação de alto risco no limite
do hardware; falhas parciais (como os 404 de 2026-04) são o resultado esperado
de operar sem folga. O perf-plan (M1) define a meta — run mensal < 4 h com
spill < 30 GB — ainda não comprovada.

## Dimensão 3 — Histórico/backfill (visão central não implementada)

- ADR 0016 `Proposed`; nenhum dos 4 "próximos passos" executado
  (`backfill.yml`, `rebuild-manifest --from-ia`, dry-run, disparo).
- 1/35 meses no IA. O gap guard do `etl-monthly` apenas emite warning e pede
  dispatch manual mês a mês (`etl-monthly.yml:130-134`).
- Frontend: `ManifestSchema` suporta N snapshots, mas `currentSnapshot()` só lê
  `manifest.current` (`web/src/lib/manifest.ts:35-37`). Não há seletor de mês,
  diff, ou qualquer UI temporal.

## Dimensão 4 — Produto/frontend (features prometidas ausentes ou invisíveis)

O que funciona é bom: busca por CNPJ/razão social via DuckDB-WASM com range
requests contra o IA, ficha com sócios/filiais/badges, degradação elegante.
Mas:

- **Código pronto, dados ausentes:** as abas Pessoa/Endereço/CNAE existem no
  código e **não aparecem em produção** porque `pessoas`/`enderecos`/
  `cnpj_cnaes` faltam no snapshot 2026-04 (`SearchCNPJ.svelte:367`). O botão
  Contatos renderiza e **falha** ao clicar (parquet 404). Um snapshot novo
  completo destravaria tudo isso sem escrever uma linha de frontend.
- **Camada atômica desconectada:** implementada como `companies.zip` de
  protobufs `.pb` por raiz (`pack.py:13,239`) com reader pronto
  (`web/src/lib/companies.ts`) — mas **nenhum componente da UI a usa** (só o
  teste). Toda a UI passa por DuckDB/Parquet.
- **Inexistentes:** grafo de sócios (nenhuma lib, nenhuma query de travessia;
  sócios são lista plana), histórico temporal, autocomplete, rota
  `/empresa/[cnpj]` (sem deep-link — a ficha é um card inline).
- **Busca por nome** ainda é `ILIKE '%...%'` sobre `cnpjs.parquet` — derrota o
  bloom filter e baixa centenas de MB (W4.1/W4.2 do perf-plan, parcialmente
  endereçado só para CNPJ exato).
- **Manifest congelado no bundle:** o site serve `web/public/manifest.json`
  empacotado no build — publicar dado novo exige commit + redeploy do site.

## Dimensão 5 — Coerência arquitetural e documental

- **README raiz mente sobre a camada atômica** (corrigido em PR posterior):
  promete "milhões de `{cnpj}.json`" (README:14); a realidade é protobuf
  `.pb` por `cnpj_base`.
- **ADRs fantasma** (corrigido em PR posterior): a numeração pulava 0018→0023;
  os ADRs 0019-0022 dos parquets já shippados (per-lookup, cnpj_cnaes,
  cnpj_contatos) e o de `cnpjs_summary` (não implementado, `Proposed`) foram
  escritos.
- **ADR 0017 (ficha-py/Ibis)** — cumprido parcialmente (corrigido em PR
  posterior): `ficha-py` passou a cobrir os 7 parquets + lookups, ganhou CI
  próprio e notebooks, e o ETL passou a importá-lo de verdade em
  `write_lookup_parquets`. Os joins pesados de `cnpjs`/`raizes` continuam em
  SQL bruto deliberadamente — risco de OOM documentado no perf-plan torna
  essa migração perigosa sem benchmark prévio (ver ADR 0017, "Estado da
  implementação"). PyPI não publicado.
- **ADR 0018 (raizes v2)** `Proposed`, não implementado; não existe
  `web/src/schemas/v2/`.
- **Código morto no frontend:** schemas Zod de dados nunca validados em
  runtime; `raizes.parquet` publicado e nunca consultado; `files.lookups`
  (JSON) não lido.
- `etl/README.md:6` dizia "Esqueleto inicial — implementação ainda não
  começou" para um pipeline de ~1.800 linhas testado — corrigido neste
  mesmo PR.

## Dimensão 6 — Processo (retrabalho crônico e sequenciamento violado)

- ~15 dos últimos 60 commits são fixes recorrentes dos mesmos 4 temas: deploy
  do Pages, resolução do mês, integridade do manifest, OOM. O projeto está em
  modo "manter as luzes acesas" sobre um snapshot único e quebrado.
- O perf-plan avisou: *"Don't start M4 until M0 lands"* (não multiplicar
  parquets antes de o pipeline completar E2E). M4 foi implementado assim mesmo
  (W7/W8/W10/W11/W12) — e os parquets novos foram exatamente os que falharam no
  upload de 2026-04, multiplicando a superfície de falha prevista.
- Observabilidade fraca até 2026-07-12: erros suprimidos (`2>/dev/null`),
  freshness check só no smoke semanal, nenhum alerta ativo. Três crons
  falharam em silêncio por três meses seguidos.

---

## Caminho crítico recomendado (ordem importa)

1. **Diagnosticar/destravar a resolução do mês** — a run de 2026-07-15 (com
   stderr agora visível) diz a causa; independente disso, `workflow_dispatch`
   com `inputs.month=2026-06` contorna hoje. Considerar fallback no resolver:
   se o PROPFIND falhar no runner, tentar via mirror IA ou aceitar mês
   calendário com validação posterior.
2. **Fechar o primeiro ciclo E2E real:** rodar `etl-monthly` manual para
   2026-05/2026-06, com os 7 parquets + `companies.zip`, verificação de URLs e
   commit do manifest pelo bot. Isso valida de uma vez o OOM-fix, o upload e a
   publicação — o teste que está pendente desde maio.
3. **Reparar ou aposentar o snapshot 2026-04** (re-upload dos parquets 404 ou
   apontar `current` para o primeiro mês completo).
4. **Implementar o backfill (ADR 0016)** — `backfill.yml` matrix +
   `rebuild-manifest --from-ia`. Só isso materializa o "Rolodex histórico".
5. **Só então** gastar em features de visão no frontend: UI temporal, grafo,
   ligação da camada atômica, autocomplete — todas dependem dos dados dos
   passos 1-4 (e as abas Pessoa/Endereço/CNAE destravariam de graça no passo 2).
6. **Higiene contínua:** corrigir README (atômica = protobuf), escrever ADRs
   0019-0022, decidir o destino do ficha-py (integrar de verdade ou rebaixar a
   experimento).

**Síntese em uma frase:** o FICHA não está bloqueado por engenharia faltante —
está bloqueado porque o ciclo operacional *RFB → transform → IA → manifest →
site* nunca completou sozinho; consertar esse ciclo (passos 1-3) destrava em
cascata quase tudo o que a visão promete.
