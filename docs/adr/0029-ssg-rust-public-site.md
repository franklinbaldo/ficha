# ADR 0029 — SSG Rust como gerador do site público do FICHA

**Status:** Accepted (registro de decisão já implantada)  
**Data:** 2026-11-18  
**Decisão de produto:** [Ficha #269](https://github.com/franklinbaldo/ficha/issues/269)  
**Implementação:** [PR #290](https://github.com/franklinbaldo/ficha/pull/290), integrada em `2a29089ba137d62735e78fd818c671d30d9b7843`

## Contexto

A [ADR 0001](0001-astro-svelte-stack.md), aceita em abril de 2026, escolheu
Astro, Svelte, TypeScript e Zod para a interface. A
[ADR 0005](0005-monorepo-web-etl.md) descreveu `web/` como a aplicação
publicada no GitHub Pages e `etl/` como o produtor dos dados.

Em novembro, a direção decidiu publicar primeiro uma **home estática
informativa**, sem condicionar sua disponibilidade à busca remota.
A execução Chromium sobre o antigo Astro identificou bloqueio CORS ao acessar
Parquet arquivado no Internet Archive
([evidência histórica na #208](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-6974257617)).
Essa medição **não** descreve o novo site SSG, que não executa tal consulta.

## Decisão

- A fonte do **site público** passa a ser `site/`, gerada com
  **SSG Rust**, HTML e CSS, servida em `/ficha/` pelo GitHub Pages.
- A primeira entrega oferece home, página Sobre, competência declarada pelo
  manifesto e links para os dados. **Não tem formulário de busca CNPJ,
  DuckDB-WASM ou resultado simulado.**
- `web/` permanece no monorepo como **protótipo não publicado** da
  interface Astro/Svelte e como local do manifesto canônico
  `web/public/manifest.json`. Sua presença não significa que Astro
  continue no Pages.
- `.github/workflows/deploy.yml` gera `site/dist/ficha` na branch
  principal, preserva o manifesto canônico **byte a byte** e verifica por
  HTTP home, Sobre, CSS e manifesto após o deploy. A identidade dos bytes
  do manifesto não comprova acesso aos objetos remotos.
- `etl/`, preservação no Internet Archive, contrato de snapshots e
  política de promoção **não mudam** por esta decisão.

## Relação com as decisões anteriores

A ADR 0001 fica **superada para a implementação publicada**, mas continua
como registro histórico e descrição do protótipo `web/`.
A ADR 0005 permanece válida quanto ao **monorepo e à separação ETL/interface**;
fica superada somente na afirmação de que `web/` é a pasta cujo build é
publicado no Pages. Não editar retroativamente as ADRs aceitas.
As decisões de custódia, especialmente a
[ADR 0004](0004-internet-archive-as-storage.md), são independentes.

## Evidência e limites

A [PR #290](https://github.com/franklinbaldo/ficha/pull/290)
introduziu a primeira home SSG; o
[workflow de Pages pós-merge #44302002473](https://github.com/franklinbaldo/ficha/actions/runs/44302002473)
foi concluído com build, deploy e smoke HTTP, incluindo comparação
byte a byte do manifesto público da competência `2026-07`.
Isso comprova a **primeira fatia estática naquela execução**,
não a disponibilidade universal dos dados, a competência mais recente
da RFB ou a navegação acessível em todas as condições.

As verificações específicas de Chromium/axe do SSG pertencem à
[PR #293](https://github.com/franklinbaldo/ficha/pull/293).
O acesso remoto por CORS+Range, a identidade dos Parquets e
a futura consulta CNPJ permanecem assuntos da
[issue #208](https://github.com/franklinbaldo/ficha/issues/208) e da
[PR experimental #276](https://github.com/franklinbaldo/ficha/pull/276).
A publicação estática não autoriza espelhos, proxy, custos novos,
ETL produtivo, upload ao IA, releases ou promoção de manifesto.

## Consequências e reversibilidade

A página institucional deixa de depender do JavaScript pesado e do
transporte dos Parquets para aparecer. A busca não deve ser anunciada
como funcional até passar por provas próprias em navegador e identidade.
O mecanismo de rollback da primeira publicação está documentado em
[`site/README.md`](../../site/README.md); voltar ao Astro não é
parte desta decisão e exige nova autorização de produto.
