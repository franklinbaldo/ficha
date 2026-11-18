---
type: Documentation
title: FICHA — wireframes de Início e ficha cadastral (Marcos 0 e 1)
description: Esboços responsivos, estados de consulta e critérios de UX para o FICHA SSG.
---

# FICHA: wireframes e contrato de UX (Marcos 0 e 1)

**🦇 Morcego, 18/11/2026.** Proposta complementar à [visão greenfield](../rfc/ficha-greenfield-visao-produto-2026-11.md) (PR #307, em integração) e à [Ficha #269](https://github.com/franklinbaldo/ficha/issues/269). São esquemas de hierarquia, não screenshots, telas implementadas ou comprovação de consulta real. O site publicado usa `site/` e possui formulário experimental cujo acesso remoto continua sem prova end-to-end ([#208](https://github.com/franklinbaldo/ficha/issues/208)).

## Unidade cadastral antes da interface

Os contratos [Cnpj](../../knowledge/types/cnpj.md), [Raiz](../../knowledge/types/raiz.md) e [Socio](../../knowledge/types/socio.md) não são intercambiáveis. **CNPJ completo identifica um estabelecimento** (matriz ou filial); a raiz reúne os estabelecimentos; os sócios se relacionam à **raiz**. Situação, endereço e CNAE de uma filial não são propriedades de todas as filiais. CNPJ é **texto**, preservando zeros e letras. Competência observada não é data exata de alteração nem necessariamente o cadastro atual.

## Início · desktop 1440×900

```text
┌────────────────────────────────────────────────────────────────────┐
│ FICHA                                   Acervo  Como funciona      │
├────────────────────────────────────────────────────────────────────┤
│ Dados públicos de empresas brasileiras                             │
│ Uma ficha para entender cada empresa e cada época.                 │
│ Competência apontada pelo manifesto: AAAA-MM                       │
│                                                                    │
│ Pesquisa por CNPJ indisponível neste Marco 0                      │
│ A consulta ainda não foi validada em navegador e dados reais.      │
│ [Abrir manifesto]  [Explorar o acervo]                              │
│                                                                    │
│ Explorar o acervo    Como funciona                                 │
├────────────────────────────────────────────────────────────────────┤
│ Fonte · Manifesto · Proveniência · Limites                         │
└────────────────────────────────────────────────────────────────────┘
```

**Marco 0 (alvo dos desenhos):** aviso de indisponibilidade + acesso efetivo a manifesto/acervo, **sem botão Pesquisar**. A interface *hoje* em `site/content/index.md` já apresenta `#busca-form`, `#busca-termo` e `#consulta-estado` como **prévia experimental**, não como busca end-to-end validada. A decisão do 🐘 Elefante entre preservar essa prévia rotulada ou ocultá-la no M0 continua pendente; os desenhos não alteram a produção nem simulam que a decisão já foi aplicada.

**Marco 1 (variante futura, não publicada):** depois de validar consulta real por CNPJ completo, substituir o aviso por `CNPJ completo (numérico ou alfanumérico): [____________] [Pesquisar]`, com `label` explícito e estado acessível. Busca por nome é Marco 2, não promessa do Marco 1. Links para Parquet grande devem informar tamanho e possível download. A competência acompanha o resultado, não só o rodapé.

## Início · celular 375×812, com reflow a 320 px e texto 200%

```text
┌──────────────────────────────┐
│ FICHA              Acervo    │
│                 Como funciona│
├──────────────────────────────┤
│ Dados públicos de empresas   │
│ Uma ficha para cada época    │
│ Competência: AAAA-MM         │
│                              │
│ Pesquisa CNPJ indisponível   │
│ Sem consulta validada ainda  │
│ [Abrir manifesto]            │
│ [Explorar o acervo]          │
│                              │
│ Explorar o acervo            │
│ Sobre os dados               │
└──────────────────────────────┘
```

Nenhum placeholder substitui `label`. Conteúdo, competência e ação **disponível** precisam ser compreensíveis na primeira dobra, sem um banner volumoso. Botão em largura total, foco visível, alvos de toque confortáveis. Testar 320 CSS px, zoom textual 200% e ausência de truncamento/scroll horizontal.

## Ficha de estabelecimento · desktop e celular

```text
┌────────────────────────────────────────────────────────────────────┐
│ FICHA › Pesquisa › Estabelecimento             [Copiar link]       │
│ Empresa / razão social da RAIZ 00000000                           │
│ Estabelecimento: 00.000.000/E08G-12  [IDENTIFICADOR FICTÍCIO]      │
│ Competência: AAAA-MM · Receita Federal · Ver evidência             │
├───────────────────────────────┬────────────────────────────────────┤
│ ESTABELECIMENTO               │ EMPRESA / RAIZ                     │
│ Matriz ou filial              │ Razão social                       │
│ Situação observada            │ Natureza jurídica                  │
│ Nome fantasia                │ Capital social                     │
│ CNAE e endereço               │ CNPJ-base                          │
├───────────────────────────────┴────────────────────────────────────┤
│ Outros estabelecimentos da raiz [se fonte provar]                 │
│ Quadro societário da RAIZ [se disponível e adequado]              │
│ Proveniência · Manifesto · Arquivo · Limitações                    │
└────────────────────────────────────────────────────────────────────┘
```

No celular, **uma coluna**: identificação → competência → estabelecimento → raiz → vínculos efetivamente disponíveis → proveniência. Usar títulos semânticos e `dl/dt/dd` para pares atributo/valor. Diferenciar “Não informado neste snapshot” de “Não existe” e “Não se aplica”. Não inferir continuidade, controle societário ou situação atual.

**URL compartilhável (contrato proposto, não rota existente):** vincular **CNPJ completo textual + competência de origem**, por exemplo `?cnpj=00000000E08G12&competencia=AAAA-MM` (identificador fictício). Refresh, Back e link copiado devem preservar os dois valores. Se a competência estiver ausente ou deixar de estar disponível, mostrar indisponibilidade; **nunca consultar silenciosamente o `current`** e apresentar outra competência como se fosse a solicitada. Separar a origem do *conjunto* (manifesto, URL do Parquet, tamanho e digest) da evidência da *linha individual*: apontar para o arquivo não prova, isoladamente, que o registro exibido foi recuperado nem que pertence àquele snapshot. Não gerar milhões de páginas estáticas.

## Contrato de entrada para o Marco 1

CNPJ é **string**, nunca número. Remover apenas a pontuação de apresentação (`.`, `/`, `-` e espaços), converter letras para maiúsculas e conferir **14 caracteres sem máscara** no resultado: as 12 primeiras posições admitem `[A-Z0-9]` e as duas últimas são dígitos `[0-9]`. Isso descreve o formato; eventual cálculo de dígitos verificadores tem critério próprio. Zeros iniciais e letras são preservados.

- Entrada com **aparência de CNPJ** mas formato inválido (por exemplo `00.000.000/E08G-AA`) recebe erro junto ao campo **antes** de `fetch`, inicialização WASM ou scan remoto; nunca cai automaticamente em `ILIKE` por razão social.
- CNPJ de formato válido (por exemplo `00.000.000/E08G-12`, identificador fictício) gera apenas busca de **igualdade exata**, preservando a competência escolhida; os exemplos não autorizam alegar existência desse cadastro.
- Consulta por **nome empresarial** pertence ao Marco 2. Uma sequência ambígua de 14 caracteres não deve ser adivinhada como nome: a opção de busca e seu modo devem ser explícitos quando o Marco 2 existir.

**Contraprovas offline para implementação M1:** validar CNPJ numérico e alfanumérico, máscara com zeros, final alfabético inválido, entrada incompleta, termo com aparência de CNPJ incorreto e nome ambíguo. Espionar o planner e exigir **zero I/O / zero chamada ao WASM / zero `ILIKE`** para entradas inválidas em M1, sem converter a consulta pontual em varredura por nome. A lógica legada atual em `site/static/js/query-core.mjs` ainda permite fallback de nome; este é um contrato de produto **futuro**, não um reparo já executado.

## Estados mínimos com mensagens acessíveis

| Estado | Texto/ação visível | Prova requerida |
| --- | --- | --- |
| Marco 0 | “Busca em preparação. Explore os dados já publicados.” | Não fingir sucesso nem esconder acesso ao acervo |
| Entrada vazia ou incompleta | “Informe um CNPJ completo, com 14 caracteres sem a pontuação da máscara.” | Mensagem junto ao campo, erro anunciado e foco adequado |
| Entrada alfanumérica | Letras e zeros preservados como texto | Primeiras 12 posições alfanuméricas, últimas 2 numéricas; igualdade exata |
| CNPJ com aparência inválida | “Confira os 14 caracteres do CNPJ, sem pontuação; os dois últimos devem ser dígitos.” | Sem fallback `ILIKE`, fetch, WASM ou scan; não tratar como razão social |
| Carregando | “Consultando competência AAAA-MM…” | `role=status`, sem submissão duplicada |
| Não encontrado | “Nenhum estabelecimento encontrado nesta competência.” | Diferente de erro de rede e de manifesto ausente |
| Origem falhou/CORS/Range/timeout | “Consulta indisponível agora. Tente novamente ou explore o acervo.” | Retry, `role=alert` quando necessário e resultados antigos limpos |
| Manifesto/competência ausente | “Sem competência disponível para consulta no momento.” | Não inferir último snapshot silenciosamente |
| Sucesso | Identidade do estabelecimento e competência + evidência | Conteúdo navegável; não despejar lista inteira em live region |
| Digitação mudou A→B durante fetch | Descartar resultado tardio de A | Nenhum resultado obsoleto ocupa o lugar de B |

Mensagens principais não devem expor stack trace nem URL técnica. O erro de transporte não vira “0 resultados”; a ausência de campo não vira dado negativo.

## Plano de aceitação, ainda NÃO EXECUTADO

1. **Navegador publicado:** Chromium 375×812 e 1440×900, reflow a 320/200%, screenshots, ordem Tab, Enter no skip-link, foco visível, contraste e relatório axe. Leitor de tela/toque real seguem ensaios próprios.
2. **Dados:** consultar CNPJ numérico e alfanumérico reais *somente com amostras autorizadas e reproduzíveis*. Dois CNPJs diferentes da mesma raiz devem preservar estabelecimentos distintos e vinculação comum à raiz.
3. **Falhas:** CNPJ malformado sem I/O nem `ILIKE`, não encontrado, 404, 200 sem Range, CORS, timeout, retry, A→B tardio e link compartilhado com competência ausente. Verificar mensagens, limpeza de resultados e recuperação.
4. **Performance:** medir tempo/bytes em rede móvel; não carregar DuckDB-WASM na home antes da ação e não transferir Parquet de múltiplos GB para consulta pontual.
5. **Proveniência:** conferir identidade e schema do snapshot/manifesto, origin e `query_url` separadamente; HTTP 206 não valida SHA integral.
6. **Classificação de evidência:** separar `PUBLICADO_VERIFICADO`, `NAVEGADOR_LOCAL`, `CODIGO_VERIFICADO` e `NOT_RUN`. CI/HTTP 200 não equivalem a busca disponível.

**Fora do escopo:** alteração de produto, deploy, ETL, upload ao IA, promoção de manifesto, cron, armazenamento pago ou conclusão da questão CORS. Este documento prepara implementação incremental, mas **não resolve #208**.
