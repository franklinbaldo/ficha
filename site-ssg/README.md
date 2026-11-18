---
type: Documentation
title: Ficha SSG — prévia técnica isolada
description: Estado demonstrado do shadow Rust e gates ainda necessários antes do corte público
---

# Ficha: site SSG em paralelo

**Estado R54 (18/11/2026): formulário e ESM presentes no artefato shadow, com Chromium offline executado; ainda NÃO APTO a deploy.** A carga real do DuckDB-WASM/CDN, a leitura remota de Parquet, Protobuf v1/v1.1, modalidades complementares, teste E2E A→B e paridade com o site Astro continuam pendentes. O diretório `web/`, a busca atual, os workflows Pages, o contrato de `manifest.json` e o ETL não são alterados por esta etapa. Não publicar `site-ssg/dist/` no Pages. [Direção Ficha #269](https://github.com/franklinbaldo/ficha/issues/269).

## Escolha

Branch nova a partir da `main`, não baseada na PR #257. A #257 permanece como referência de riscos e testes; seu adaptador simulado não será reaproveitado na busca real. O objetivo é trocar o gerador de HTML, não reescrever o contrato de dados.

Estrutura: `content/` contém home de transição e Sobre; `templates/` contém HTML estático; `static/` contém CSS nativo; `scripts/` traz verificação fail-closed da saída. `views/` não entra. `static/js/` agora contém leitor parcial do manifesto publicado, SQL parametrizado CNPJ/razão social, controlador de formulário, entrypoint e um carregador experimental DuckDB-WASM por CDN versionado. São copiados para o artefato SSG depois do build e não para Pages. O carregador JavaScript depende de jsDelivr com **versão fixa, mas sem SRI**: sua operabilidade, CSP, CORS, cache, privacidade e confiabilidade precisam ser comprovados em navegador antes da publicação. **Protobuf não foi portado**.

## Gerador pinado

Usar exatamente **ssg 0.0.75**, como no Conde, arquivo `ssg-v0.0.75-x86_64-unknown-linux-gnu.tar.gz` da release `v0.0.75` do repositório `sebastienrousseau/static-site-generator`. SHA-256 do TAR, conferido no workflow do Conde: `f6305109e9bec5895e9c90f97a5c569c74048b3d50823d40175e15ca99c50843`. Nenhum `latest`. Executores devem conferir checksum e `ssg --version` antes de rodar.

A partir da raiz de `site-ssg/`, com Node 22 e o binário verificado no `PATH`:

```sh
node --test tests/*.test.mjs
ssg check --config config.toml
ssg build --config config.toml
node scripts/remove-ssg-manifest.mjs
node scripts/materialize-assets.mjs
node scripts/check-built.mjs
```

**Gate shadow:** `.github/workflows/site-ssg-shadow.yml` executa Node 22, SSG Rust 0.0.75 com SHA-256 fixado e um smoke Chromium 375 px sobre o HTML gerado, no SHA exato da PR, sem deploy. Consulte resultado e steps antes de afirmar PASS. O Chromium atual cobre navegação, ESM, 404/500, falha de rede/CORS e modo sem JS; não carrega Worker/WASM/Parquet nem substitui E2E A→B, paridade de dados, axe, Prek/OKF ou publicação. As fontes `static/css/site.css` e `static/js/*.mjs` são copiadas somente depois de o SSG produzir o HTML. O verificador exige prefixo `/ficha/`, páginas inicial e Sobre e CSS; falha se aparecer manifesto incompatível com o contrato da Receita. Não mascarar o erro de manifest já observado na PoC #257 apenas porque `ssg build` retorna zero.

## Próximas fatias antes da troca

1. Repetir `ssg check/build` e Chromium offline no **SHA recomposto** após `main`, com asserção de checkout e inspeção dos avisos do gerador. Não transferir PASS entre commits; coordenar CI com Texugo (#268) sem editar os workflows protegidos.
2. Migrar a busca REAL a partir de `manifest.json`, em módulo JS separado. Preservar modalidades da home atual quando disponíveis, CNPJ numérico e alfanumérico textual, v1/v1.1, estados de erro/ausência, proveniência, CORS/Range, foco/teclado. Protobuf atômico não deve ser apresentado como fluxo da home antes de ser integrado.
3. Comparar resultados da busca atual e nova com fixtures comuns, negativos de rede, Chromium 320/375/1440 e axe; medir KB sem confundir shell com busca completa; testar rollback com artifact real.
4. Só depois abrir PR de corte do Pages com revisão independente e gates. Remover Astro/Svelte/Bun quando a busca nova realmente substituir a antiga. Não mudar ETL, IA, cron nem promover manifesto.

O gerador SSG 0.0.75 **emite** um `manifest.json` PWA em `dist/ficha/manifest.json`, confirmado em build real no CI. `remove-ssg-manifest.mjs` exige igualdade estrutural exata com o PWA observado e só então o apaga **na saída shadow**. Testes negativos comprovam que o manifesto de dados da RFB, payloads inesperados e symlinks são preservados com falha explícita; a ausência do PWA é tolerada. Não se cria nem se publica um manifesto RFB nesta PR. O check final rejeita qualquer PWA remanescente. Não existe plano de colocar dados sintéticos no Pages.

## Fatia R52 (18/11/2026): módulo JS no artefato, não em produção

- [PR #276, commit `25fa3623`](https://github.com/franklinbaldo/ficha/commit/25fa3623e3b97e4a4c087c4f0fbfd8bce25d9789): formulário acessível na home, controller ESM, inicialização lazy de DuckDB 1.33.1-dev45.0 a partir do jsDelivr, renderização por `textContent`, tratamento distinto de erro e ausência de dados, atualização visível de estado e assets verificados na saída do SSG.
- [Shadow CI #44250083678](https://github.com/franklinbaldo/ficha/actions/runs/44250083678): **18/18 testes Node** no SHA exato; `ssg check/build`, retirada estrita do PWA e checagem do HTML/CSS/JS produzidos PASS. No log do SSG: **Quality Gate 8/10**, seis avisos WARN. Nenhuma publicação.
- **Não demonstrado naquela fatia R52:** import do CDN e Worker/WASM em navegador real; HTTP Range/CORS dos dados remotos; resultados de CNPJ numérico/alfa e nome equivalentes à home Astro; Protobuf/ZIP v1/v1.1; testes Chromium, axe, leitor de tela, reflow e bytes transferidos. O teste de DOM era um duplo de teste Node, não browser; houve progresso parcial de Chromium nas rodadas posteriores.
- **Gate seguinte naquela fatia:** executar Playwright/axe com servidor do artefato shadow e stubs offline + teste controlado contra dados remotos; comparar respostas do site Astro e do SSG antes de propor troca de Pages. O smoke Chromium offline foi executado posteriormente, mas a carga remota, axe e paridade ainda não.

## Estado observado R54 (18/11/2026): o que foi e o que não foi testado

- **HEAD da branch shadow `32e5a4fb`**, [workflow SSG #44265295019](https://github.com/franklinbaldo/ficha/actions/runs/44265295019): jobs/steps de checkout SHA-exato, Node, SSG check/build, remoção estrita do PWA, verificação de CSS/ESM e Chromium **SUCCESS**, com **22/22 testes Node** segundo o log do job. O navegador foi executado localmente no CI a 375 px: skip-link, ESM, 404, 500, rede/CORS abortada e fallback sem JS. **Não** foi teste de consulta DuckDB remota nem deploy.
- **Regressão 404 tratada:** quando o manifesto retorna ausência, o controlador libera a sessão para nova tentativa na mesma página. Esta correção aparece na [#276](https://github.com/franklinbaldo/ficha/pull/276); não deve ser descrita como defeito vigente.
- **Mudança empilhada ainda separada:** a [PR #284](https://github.com/franklinbaldo/ficha/pull/284) propõe descartar resultados de uma busca A se o usuário editar o campo para B durante a consulta. No [HEAD `9a2bbcc5` e workflow shadow #44266039762](https://github.com/franklinbaldo/ficha/actions/runs/44266039762), **25/25 testes Node** e o mesmo smoke Chromium offline passaram. A #284 tem como base a branch da #276, **não a `main`**; seus três cenários de corrida são testes Node com dependências injetadas, não prova E2E no navegador. Esses resultados **não** significam que a correção já esteja incorporada à #276.
- **Manifesto RFB no corte:** o shadow elimina o PWA do gerador e **não inclui** `dist/ficha/manifest.json` de dados. Isso é deliberado; copiar o shadow direto para Pages quebraria a busca pública. Antes da troca, a PR final precisa preservar/materializar o manifesto RFB verdadeiro, validar `current`, demonstrar HTTP positivo e consultas numérica/alfanumérica/razão social com dados controlados ou remotos, verificar CORS/Range, o fluxo Protobuf v1/v1.1, teste de corrida A→B em navegador e rollback. Sem ETL, IA, release ou promoção do manifesto por esta PR.
- **Pendências de produto:** buscas adicionais e `views/` não devem ser silenciosamente descartadas. A equivalência ou a decisão explícita de retirar funcionalidades pertence ao gate de paridade e à direção do projeto.
