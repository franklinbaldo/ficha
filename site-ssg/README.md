---
type: Documentation
title: Ficha SSG — esqueleto isolado
description: Migração sem troca prematura do site público
---

# Ficha: site SSG em paralelo

**Estado: esqueleto estático, sem consulta real e não apto a deploy.** O diretório `web/`, a busca atual, os workflows Pages, o contrato de `manifest.json` e o ETL não são alterados por esta etapa. Não publicar `site-ssg/dist/` no Pages. [Direção Ficha #269](https://github.com/franklinbaldo/ficha/issues/269).

## Escolha

Branch nova a partir da `main`, não baseada na PR #257. A #257 permanece como referência de riscos e testes; seu adaptador simulado não será reaproveitado na busca real. O objetivo é trocar o gerador de HTML, não reescrever o contrato de dados.

Estrutura: `content/` contém home de transição e Sobre; `templates/` contém HTML estático; `static/` contém CSS nativo; `scripts/` traz verificação fail-closed da saída. `views/` não entra. O módulo JS real de busca será acrescentado isoladamente em fatia posterior, com mesma lógica de manifest, CNPJ textual alfanumérico, DuckDB-WASM e Protobuf, e provas comparativas contra `web/`.

## Gerador pinado

Usar exatamente **ssg 0.0.75**, como no Conde, arquivo `ssg-v0.0.75-x86_64-unknown-linux-gnu.tar.gz` da release `v0.0.75` do repositório `sebastienrousseau/static-site-generator`. SHA-256 do TAR, conferido no workflow do Conde: `f6305109e9bec5895e9c90f97a5c569c74048b3d50823d40175e15ca99c50843`. Nenhum `latest`. Executores devem conferir checksum e `ssg --version` antes de rodar.

A partir da raiz de `site-ssg/`, com Node 22 e o binário verificado no `PATH`:

```sh
node --test tests/source.test.mjs
ssg check --config config.toml
ssg build --config config.toml
node scripts/materialize-assets.mjs
node scripts/check-built.mjs
```

**Este commit não tem prova de build real:** SSG/Chromium/Prek/OKF permanecem NOT_RUN até a execução em executor autorizado e no SHA exato. A fonte `static/css/site.css` é copiada somente depois de o SSG produzir o HTML. O verificador exige prefixo `/ficha/`, páginas inicial e Sobre e CSS; falha se aparecer manifesto incompatível com o contrato da Receita. Não mascarar o erro de manifest já observado na PoC #257 apenas porque `ssg build` retorna zero.

## Próximas fatias antes da troca

1. Confirmar `ssg check` e `ssg build` com SHA exato, inspecionar HTML gerado e os avisos do gerador. CI do SSG sem pular os testes; coordenar com Texugo (#268) sem editar os workflows protegidos.
2. Migrar a busca REAL a partir de `manifest.json`, em módulo JS separado. Preservar modalidades da home atual quando disponíveis, CNPJ numérico e alfanumérico textual, v1/v1.1, estados de erro/ausência, proveniência, CORS/Range, foco/teclado. Protobuf atômico não deve ser apresentado como fluxo da home antes de ser integrado.
3. Comparar resultados da busca atual e nova com fixtures comuns, negativos de rede, Chromium 320/375/1440 e axe; medir KB sem confundir shell com busca completa; testar rollback com artifact real.
4. Só depois abrir PR de corte do Pages com revisão independente e gates. Remover Astro/Svelte/Bun quando a busca nova realmente substituir a antiga. Não mudar ETL, IA, cron nem promover manifesto.

O arquivo `manifest.json` do gerador, se existir, **não é** o `manifest.json` da RFB. O check rejeita um PWA manifest no lugar do contrato público. Não existe plano de colocar dados sintéticos no Pages.
