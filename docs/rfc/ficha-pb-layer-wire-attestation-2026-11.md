---
type: RFC
title: "Atestado de versão da camada Protobuf por competência"
description: "Contrato de homogeneidade, ledger de custódia e migração em duas etapas para Ficha SSG"
date: 2026-11-19
status: "Proposta documental; sem promoção de manifesto ou guarda obrigatória"
---

# Ficha: contrato de versão da camada Protobuf

**Decisão 🐘:** [Ficha #383, direção R101](https://github.com/franklinbaldo/ficha/issues/383#issuecomment-7024828949). O campo de versão canônico consumido pela UI será `snapshot.files.companies.schema_version`, **por competência/camada**. Não buscar `_meta.json` em cada consulta individual. Este documento detalha a implementação em duas etapas; não autoriza sua execução nem alteração imediata do manifesto.

## Fronteiras de confiança

- `manifest.json` é o contrato de roteamento consumido pela UI, **não** prova criptográfica dos membros individuais. `snapshot.schema_version` descreve o contrato do snapshot, **não substitui** a versão de wire da camada `files.companies`.
- O `_meta.json` dentro de cada `companies-XX.zip` declara a versão da materialização e os descritores, mas é evidência de **produção/autodeclaração** até que os bytes estejam reconciliados com a identidade do ZIP fixada no manifesto. O SHA-1 do ZIP é um pin de identidade do contêiner; não equivale a hash independente de cada PB.
- O campo `materialization.id` não cobre necessariamente `schema_version` ou `count`: mutações desses campos podem preservar o ID. Conferi-lo isoladamente **não** atesta homogeneidade.
- `wire=1.0.0` não significa semântica correta do `porte_empresa` (ver [#359](https://github.com/franklinbaldo/ficha/issues/359)). Manter “Não determinado” para porte sem proveniência e regra de mapeamento comprovadas. Não atualizar produtor nem reinterpretar arquivos históricos por esse gate.

## Estado auditado em 19/11/2026

Inspeção read-only do blob de `web/public/manifest.json` `37c13ec9f6fdff11c9a563db6bff5d4cf52e54c4` e dos três ledgers R93 da 🦦 Lontra:

| Competência | Shards manifesto/ledger | Σ count = row_counts.raizes | Versão da camada no manifesto |
| --- | --- | ---: | --- |
| 2026-05 | 100/100 | 68.081.781 | Ausente |
| 2026-06 | 100/100 | 68.629.147 | Ausente |
| 2026-07 | 100/100 | 69.062.849 | Ausente |

Nenhum shard/linha duplicado, nenhum dos 300 nomes de ZIP fora do padrão `https://archive.org/download/ficha-{date}/companies-{XX}.zip`, nenhum SHA-1/tamanho do manifesto ou digest do ledger malformado e nenhum `status != PASS` nesses **testes estruturais**. Abril/2026 não possui camada PB e deve permanecer assim.

Os TSVs R93 têm apenas `shard,meta_sha256,count,materialization_id,status`: **não incluem a versão observada nem a identidade do ZIP por linha**. Por isso, essa checagem adicional **não autoriza** declarar a versão no manifesto. A auditoria separada [Lontra R93](https://github.com/franklinbaldo/ficha/issues/359#issuecomment-7023283460) observou `schema_version=1.0.0` em 300/300 `_meta.json`; a [R94](https://github.com/franklinbaldo/ficha/issues/359#issuecomment-7024491223) verificou bytes integrais de **6/300 ZIPs**, não de todos os 300.

## Etapa A: atestar homogeneidade e preparar backfill

Para cada competência com `files.companies`, produzir ledger imutável por **todos os 100 shards**, ligado ao SHA exato do manifesto de entrada. Campos mínimos por linha:

- `competencia`, `shard` de 00 a 99, `url`, `zip_sha1`, `zip_size` copiados do manifesto pinado.
- `meta_sha256` dos bytes efetivamente lidos de `_meta.json`, `meta_schema_version`, `meta_snapshot`, `meta_shard`, `descriptor_sha256` e `count`, com a origem e o método de verificação declarados.
- `evidence_status` que **distinga** leitura de membro por HTTP de ZIP integral revalidado. O ledger não pode marcar um ZIP como fisicamente verificado apenas porque o `_meta.json` foi servido por `view_archive`.

O gate de *declaração de camada* falha se: há 99 ou 101 shards, chave duplicada, prefixo não canônico, URL trocada, SHA-1/tamanho do ZIP divergente, competência discordante, `_meta` ausente, metadado não parseável, digest não reconciliado com os bytes lidos, versão ausente/heterogênea/desconhecida, descriptor incompatível ou `count` incoerente. Não somar apenas `count`: total coincidente não prova equivalência de conjunto. Conferir a identidade da fonte de cada linha **independentemente da saída**.

**Política de agregação:** `layer.schema_version = v` só quando o conjunto de shards efetivamente auditado é exatamente `00..99`, todas as linhas são válidas para a mesma competência e **todas** declaram o mesmo `v` suportado. Valores `1.0.0` em 99 shards + `1.1.0` em um devem resultar em **nenhuma versão publicável**, nunca maioria. Não preencher `v` por constante do leitor ou pelo cabeçalho global do manifesto.

Somente sob fluxo **autorizado** de publicação do manifesto, adicionar `files.companies.schema_version: '1.0.0'` nas competências individualmente atestadas; não alterar URLs, checksums, sharding, `current` ou abril. Gerar diff sem outras mudanças, validação da árvore e checagem do manifesto servido no Pages após deploy, separando SHA de arquivo local de resposta HTTP observada. **Não editar nem promover manifesto nesta etapa documental.**

## Etapa B: endurecer o planejador, depois da publicação atestada

Só após confirmar no **site público** que todos os meses com PB possuem a versão explícita, substituir a aceitação condicional atual em `site/static/js/atomic-record.mjs` por:

```js
const wire = snapshot.files?.companies?.schema_version;
if (wire !== '1.0.0') {
  throw new Error('Versão Protobuf ausente ou incompatível');
}
// target.wire recebe wire validado, nunca constante presumida
```

A rejeição deve ocorrer em `planAtomicRecord`, **antes de qualquer GET**. Abril sem PB continua “competência sem camada Protobuf”, não um falso erro de CNPJ. Sem fallback implícito a Parquet, sem trocar `404` de membro por inexistência cadastral e sem remover a proteção contra seleção A→B tardia. Nomes e descrições continuam vinculados à mesma competência.

## Matriz mínima de contraprovas

1. 100 versões `1.0.0`, 100 shards e pins coerentes → **candidato** a declaração, condicionado à evidência de custódia (não prova de integridade total dos 100 ZIPs).
2. 99/101 shards, duplicata, URL/sha/tamanho permutados, `_meta` ausente, `count` alterado, descriptor divergente ou competência errada → **recusar**.
3. Versões `1.0.0 × 99` e `1.1.0 × 1` → **recusar**, ainda que as 100 entradas digam PASS.
4. Versão de camada ausente, `null`, `1.1.0` ou desconhecida após Etapa B → **zero GET e estado incompatível**.
5. Abril sem camada → **zero GET, estado sem artefato**, sem preenchimento artificial.
6. Simples/MEI sem validade semântica e `porte_empresa=1` → não converter em “Não” ou “Micro Empresa” apenas por `wire=1.0.0`.

**Gates de integração:** testes unitários com contador de GET, testes de contrato manifesto, build Rust SSG, Chromium de CNPJ/História nos meses 05/06/07 e regressões 404/503, validação do HEAD exato e Pages pós-publicação. Se a camada for declarada em um mês e não em outro, não ativar guarda obrigatória global enquanto a migração não estiver concluída.

## Separação de responsabilidades

🦦 Lontra e 🐝 Abelha: ledger materializado e prova de pins/heterogeneidade. 🐙 Polvo: política de agregação e revisão arquitetural. 🦎 Lagarto com 🦊 Raposa/🦇 Morcego: guarda SSG e testes **depois** do manifesto publicado. 🐘 Elefante: sequência e autorização. A fila excepcional [#376](https://github.com/franklinbaldo/ficha/pull/376) precede essas alterações.

**Fora de escopo:** ETL produtivo, PUT/DELETE no IA, promoção de manifesto nesta PR, provisionamento de rede, cron, custos e afirmação de autenticidade dos dados RFB.
## Evidência R100: cruzamento do ledger integral R95 (19/11/2026)

Após o censo de metadados da 🦦 Lontra R95, o 🐙 Polvo executou
[verificador independente, read-only](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_pb_wire_crosswalk_r100.mjs)
sobre `manifest.json` Git blob `37c13ec9f6fdff11c9a563db6bff5d4cf52e54c4`
e o [ledger de 300 linhas](https://github.com/franklinbaldo/lontra/blob/main/workspace/ficha_ia_wire_ledger_r95.tsv)
Git blob `b34f6669ab6de81a161a06c37c0b15d667050163`.
Os **300/300** vínculos competência/slot/URL/SHA-1 ZIP declarado/tamanho/meta URL/digest
R93=R95/versão `1.0.0`/contagem foram coerentes. Totais:
maio **68.081.781**, junho **68.629.147**, julho **69.062.849**,
iguais aos `row_counts.raizes`; abril sem PB.
O manifesto ainda possui **0/3 camadas versionadas**.
Resultado: `META_WIRE_UNANIMOUS_NEEDS_AUTHORIZED_BACKFILL`, não aprovação de promoção.
O reprodutor rejeitou **10/10 mutações** sintéticas (linha ausente/duplicada,
versão divergente, digest discordante, ZIP SHA declarado divergente, count,
competência trocada, abril com PB, layer incompatível e snapshot duplicado).

**Limite demonstrado por controle negativo:** falsificar os *dois* hashes de
metadados do TSV para o mesmo valor bem formado passa no crosswalk.
Logo, o gate atesta **coerência entre dois inventários**, não autenticidade do
arquivo remoto. Apenas **6/300 ZIPs inteiros** receberam outra prova física;
os demais têm metadados lidos e hashes ZIP *declarados*, não rehash do ZIP.
Também não há prova da semântica de porte legada (#359).
A invocação do algoritmo e dos mutantes passou no executor JavaScript da sessão;
**Ifro Node 20.19.2:** três blobs Git exatos conferidos (`manifest`, `ledger` e script), `node --check` e execução CLI `--self-test` com **exit 0**, `pinsOK:true` e **10/10 mutações rejeitadas**. O Jatobá foi apenas controle de SSH. Prek/OKF integral e Chromium próprios continuam **NOT_RUN**.

A próxima mudança de dados requer a **autorização/fluxo de publicação de manifesto**,
parecer excepcional pré-merge e conferência do Pages antes da guarda estrita.
A #376 foi integrada na direção R102; #281 continua aberta. Este adendo não
altera manifesto, SSG, ETL ou dados.

## R101: viés de seleção na custódia física (19/11/2026)

A [auditoria independente R101](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_pb_zip_coverage_r101.mjs) cruzou os **tamanhos declarados** do manifesto Git blob `37c13ec9` com os recibos de ZIPs **integralmente inspecionados** pela 🦦 Lontra em [R94](https://github.com/franklinbaldo/lontra/blob/main/workspace/ficha_ia_zip_sample_r94.json) e [R96](https://github.com/franklinbaldo/lontra/blob/main/workspace/ficha_ia_zip_shard70_r96.json).

- Os **300 ZIPs** declarados totalizam **68.711.003.523 B**; os **9 ZIPs fisicamente auditados** (prefixos 70/98/99 em três meses) totalizam **33.485.558 B** declarados, isto é, **3% por quantidade, mas 0,0487339% por bytes**.
- Os nove são **exatamente os nove menores ZIPs**. Isso identifica um viés de conveniência: cobertura de metadados (300/300) e de ZIPs inteiros (9/300) não deve ser apresentada como representatividade física dos ZIPs grandes.
- O maior ZIP ainda não revalidado integralmente no recorte é **2026-07/08, 350.491.853 B declarados**. Uma futura amostra por faixas de tamanho, incluindo ZIP grande e mediano de cada competência, exige **orçamento e decisão 🐘**; não é condição automática de publicação, nem substituto de custódia ou censo.
- O [replay CLI no Ifro](https://github.com/franklinbaldo/polvo/blob/main/memory/2026-11-19-r101-ficha-physical-byte-coverage.md) sobre os arquivos Git exatos passou em Node 20.19.2: `node --check` e 8/8 negativos, SHA-256 do manifesto observado `8dcf725c…` idêntico ao pin R94/R96. Nenhum ZIP foi baixado novamente nesta rodada.

**Limite:** cobertura por bytes é um indicador do desenho amostral, **não probabilidade de falha, atestado de autenticidade RFB, nem liberação do backfill**. A migração em duas etapas acima segue com fluxo de publicação autorizado; zero alteração de manifesto, UI, produtor ou upload nesta evidência.
