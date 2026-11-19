---
type: RFC
title: "Consulta exata de CNPJ por Protobuf atômico"
description: "Rota Protobuf-first no site SSG, custos observados e critérios de promoção sem Parquet de 6 GB"
status: "Proposta técnica; biblioteca SSG integrada, decoder e UX de consulta pendentes"
date: 2026-11-19
---

# RFC: abrir ficha de CNPJ exato sem Parquet

## Decisão proposta

Para uma **ficha individual por CNPJ completo**, usar primeiro o membro
Protobuf da raiz no ZIP atômico declarado pelo `manifest.json`. O Parquet
permanece no acervo para análises e buscas que exijam índice/varredura; não é
pré-requisito para abrir uma ficha. Esta decisão não autoriza despublicar
Parquets, alterar a competência vigente, promover Protobuf 1.1 ou implantar
relay/proxy.

O [greenfield aprovado](ficha-greenfield-visao-produto-2026-11.md) já prefere
acesso atômico. Esta RFC especifica o transporte, o decoder, as limitações e
os critérios de teste da implementação. A [issue #208](https://github.com/franklinbaldo/ficha/issues/208)
permanece aberta para a trilha analítica DuckDB-WASM quando ela for necessária.

## Evidência real, 19/11/2026

Em Ifro, foram recuperadas e decodificadas quatro fichas **v1 publicadas**
no Internet Archive do snapshot `2026-07`, acessando somente o membro
`companies-XX.zip/XX/XXX/XXX.pb`, após redirect para `view_archive.php`:

| Raiz | PB recebido | Primeiro GET, s | Estabelecimentos | Sócios |
| --- | ---: | ---: | ---: | ---: |
| `00000000` (Banco do Brasil) | 1.370.270 B | 3,291 | 7.902 | 41 |
| `33000167` (Petrobras) | 101.606 B | 2,608 | 661 | 9 |
| `60746948` (Bradesco) | 1.542.666 B | 3,396 | 9.999 | 106 |
| `18236120` (Nubank) | 2.860 B | 1,910 | 10 | 18 |

Somatório de corpos PB: **3.017.402 B**. São quatro casos deliberadamente
escolhidos, não uma amostra representativa nem p95. Uma releitura dos quatro
PBs reais com `company.proto` em 19/11 confirmou `porte_empresa=5` nos quatro
casos e o nome **BANCO BRADESCO S.A.** na raiz `60746948`, corrigindo a
identificação anterior como Itaú. Essa amostra **não testa microempresas**;
o conflito de semântica do porte é registrado na [#359](https://github.com/franklinbaldo/ficha/issues/359).

O [primeiro ensaio Chromium na origem real do
Pages](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-7009984682)
recuperou membros de Nubank/Petrobras via `fetch` CORS sem flags especiais.
Um segundo ensaio reproduzível ampliou a prova: **Chromium real no
`https://franklinbaldo.github.io/ficha/empresa/`**, viewport 375×812,
módulo gerado `web/src/generated/company.pb.js` empacotado localmente com
esbuild, **98.383 B** de JS minificado (SHA-256
`04b05b3068141cfc3fea5256749f6bc6ad9d4829580a2cd611edf8cbf6b3d180`).
Em ambos os CNPJs completos, ocorreu exatamente um match de
estabelecimento dentro da raiz, zero match com dígito final alterado,
e a cidade/CNAE vieram de lookups `*.pb` do mesmo ZIP:

| CNPJ completo | Membro PB | Municípios PB | CNAEs PB | Resultado |
| --- | ---: | ---: | ---: | --- |
| `18236120000158` | 2.860 B | 103.604 B | 86.124 B | São Paulo/SP; atividade recuperada |
| `33000167000101` | 101.606 B | 103.604 B | 86.124 B | Rio de Janeiro/RJ; atividade recuperada |

Corpos totais por ficha do teste, incluindo esses dois lookups:
**192.588 B** e **291.334 B**. Os lookups `municipios` (5.572 entradas)
e `cnaes` (1.359 entradas) tiveram SHA-256 idêntico nos shards `18`
e `33`, mas isso não prova identidade entre todos os 100 shards.
O [script de replay](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_atomic_pages_probe.mjs)
requer `--live`, paths explícitos de protobufjs/esbuild/Playwright e não
faz upload nem muda o site.

Alternativa medida: o `lookups.json` **já publicado**, de 272.145 B,
respondeu HTTP 200 e CORS a um `fetch` diretamente da origem GitHub Pages
(2,505 s, teste singular), com `snapshot_date=2026-07`,
`schema_version=1.0.0`, 5.572 municípios e 1.359 CNAEs. É uma
**requisição em vez de duas** para esses dicionários, porém com mais bytes
(272.145 B versus 189.728 B). A implementação pode usar o
`snapshot.files.lookups.url` do manifesto, validando competência e contrato,
sem inventar índice novo. O código de município `7107` desse snapshot
resolve para São Paulo; **não inferir que seja código IBGE de sete dígitos**.

## Verificação adicional de orçamento no navegador (R89)

O [probe CDP independente](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_atomic_cdp_budget.mjs)
executou em **19/11/2026**, no Ifro, Chromium real com origem
`https://franklinbaldo.github.io/ficha/empresa/`, duas sessões em
**375×812 e 1440×900**, cache de rede desabilitado, decoder canônico
injetado somente para o ensaio e nenhum arquivo de produto alterado.
Em **cada** sessão foram feitos exatamente **três GET lógicos** do
IA: `lookups.json` (272.145 B), PB de Nubank (2.860 B) e PB de
Petrobras (101.606 B). Após redirects, CDP observou **seis saltos
`requestWillBeSent`**, três respostas finais HTTP 200 e **383.382
bytes em `Network.loadingFinished.encodedDataLength`**; a soma
dos **corpos JavaScript** foi **376.611 bytes**. Em ambos os viewports
os CNPJs completos produziram um único estabelecimento, a raiz
decodificada coincidiu, DV alterado não fez match, e não houve
requisição a Parquet na janela medida.

**Alcance da métrica:** `encodedDataLength` é contabilidade de rede
reportada pelo CDP para as respostas finalizadas; os hops de
redirecionamento não têm necessariamente valor próprio atribuído
pelo evento. **Não** equivale a medição integral de tráfego TLS,
requests de bootstrap do site, custo econômico, teste de longa duração,
p95 nem validação da UI atualmente publicada. Os 383.382 B são
desta janela controlada e não limite máximo de qualquer empresa.

As [PRs #362](https://github.com/franklinbaldo/ficha/pull/362)
e [#365](https://github.com/franklinbaldo/ficha/pull/365) já foram integradas
à `main` em 19/11/2026. O leitor `site/static/js/atomic-record.mjs`
impõe **32 MiB por membro** mediante stream com limite e rejeição
antecipada de `Content-Length`; recusa DV inválido antes de GET, contador
divergente, raiz/competência erradas e filiais duplicadas.
A [#358](https://github.com/franklinbaldo/ficha/pull/358), que tratava
o leitor Astro legado, foi encerrada **sem merge**. O teto de 32 MiB
não é máximo empírico do acervo. **Ainda não há decoder canônico
empacotado nem conexão desse leitor com a interface pública SSG**;
a página atual consulta a trilha analítica. Limite/excesso não
significam CNPJ ausente. Não usar `arrayBuffer()` sem limite.
A meta de 100 MB foi observada apenas nas duas fichas medidas.

## Contrato proposto para a interface

1. Validar e normalizar o **CNPJ completo de 14 caracteres** antes da
   consulta, sem perder letras. O fluxo v1 existente representa apenas
   raízes numéricas; **não** enviar alfanumérico para shard v1.
2. Selecionar competência existente e artifact `files.companies` v1
   com mapeamento `cnpj_base_prefix_2`. Resolver o shard declarado
   da raiz e conferir vínculo **shard ↔ URL ↔ competência** antes do fetch:
   no v1 publicado, o URL deve ser exatamente
   `https://archive.org/download/ficha-{date}/companies-{shard}.zip`.
   O schema genérico atual valida unicidade, mas isso sozinho não
   impede URLs de shards permutadas. Construir somente
   `XX/XXX/XXX.pb`; não transferir ZIP.
   Snapshots legados `companies_zip` exigem seleção explícita da sua rota;
   ausência do artefato não pode virar fallback silencioso ao Parquet.
3. Decodificar usando o schema gerado `proto/ficha/v1/company.proto`
   (a implementação v1 já existe em `web/src/lib/companies.ts`).
   O site SSG só deve receber um bundle **determinístico e estático** do
   decoder com protobufjs/minimal já existente, sem exigir Astro,
   Svelte, CDN externa ou DuckDB-WASM. Não reimplementar a gramática
   Protobuf manualmente.
4. Validar **raiz decodificada == raiz solicitada**, selecionar
   **exatamente um** estabelecimento cujo `base+ordem+DV` corresponde
   ao CNPJ completo; zero matches é ausência daquela filial na competência,
   não prova de inexistência da raiz. Duplicação, wire inválido,
   identidade diferente e versão não suportada são erros.
5. Adaptar separadamente os campos de `Company` (razão social,
   capital etc.) e do estabelecimento selecionado (nome fantasia,
   UF, município, CNAE, Simples/MEI). Resolver nomes com lookups
   **da mesma competência**. Respeitar campos ausentes e zero como
   dado ausente quando definido pelo schema; não fabricar descrições,
   datas nem indicadores. Há perda de informação no produtor v1:
   `_bool(None) → False` para Simples/MEI, de modo que um `false`
   no Protobuf **não prova que a fonte informou Não**. Não declarar
   negatividade definitiva sem contrato que preserve nulidade.
   Também não converter CNPJ ausente de sócio PF/estrangeiro no
   identificador fictício `00000000000000`.
6. Estados distintos e acessíveis: `404` de membro, `503`/falha de
   rede, resposta inválida, wire incompatível, CNPJ completo sem
   estabelecimento e sucesso. Um **404 de membro ZIP não prova inexistência
   de empresa**: é ausência do artefato naquela rota/competência.
   Na observação real, uma raiz artificial retornou **503**; nunca
   convertê-la em `404` ou ausência cadastral. Um PB íntegro sem
   filial correspondente informa somente ausência daquela filial
   na fotografia. Não mostrar resultado velho em corrida A→B→A;
   preservar a proteção da [PR #349](https://github.com/franklinbaldo/ficha/pull/349)
   já integrada.
7. Preservar competência, fonte, URL arquivística do ZIP e versão no
   resultado. A identidade `size+SHA` declarada para o ZIP não é
   prova de SHA-256 do **membro individual** obtido via `view_archive`.
   Não anunciar integridade integral que não foi aferida.

## Gate P1: semântica de bytes v1 e linguagem pública

A [issue #359](https://github.com/franklinbaldo/ficha/issues/359) confrontou
o layout oficial RFB (`00` não informado; `01` Micro Empresa;
`03` EPP; `05` Demais) com `pack.py::_porte`:
**RFB `01` é emitido como wire 1, cujo nome no enum é
`NAO_INFORMADO`**. O código `00` vira wire 0,
enquanto `03/05` viram 3/5. Um valor 2 ou campo ausente não
comprova porte ME. É evidência do *código atual*, não prova
sobre cada PB publicado; os quatro arquivos públicos amostrados
tinham valor 5.

O site deve ter um **adaptador semântico versionado** distinto do
decoder, com `snapshotDate`, `wireVersion`, `sourceValue`,
`semanticStatus` e `displayLabel`. **Antes de exibir porte**,
reconciliar bytes publicados/linhagem da competência com a regra
do produtor. Sem esse vínculo, usar **“Não determinado”**,
sobretudo para wire 1. Não apresentar o nome ingênuo do enum
nem converter wire 1 automaticamente em ME sem provar origem.
Não alterar o packer ou reinterpretar histórico sem versão
e autorização de publicação.

Em Simples/MEI, `_bool(None) → False` perde a distinção
entre ausência de informação e negativa: um `false` do PB v1
**não** autoriza exibir “Não”. Município é **código RFB**
resolvido com o lookup da mesma competência; `7107`
resolve São Paulo no exemplo, não deve virar um suposto IBGE
de sete dígitos por `padStart(7)`.

| Evidência observada | Estado/texto admissível |
| --- | --- |
| CNPJ/DV inválido localmente | “Confira o CNPJ informado.” |
| HTTP 404 de membro PB | “Arquivo da consulta não disponível nesta competência.” |
| HTTP 503, CORS, timeout, excesso de 32 MiB ou decode inválido | “Consulta indisponível; cadastro não verificado.” |
| PB válido e raiz conferida, mas sem estabelecimento exato | “Estabelecimento não encontrado nos dados desta competência.” |
| Campo com semântica ambígua no v1 | Ficha visível, campo “Não determinado”, com competência/fonte |

Teste o adaptador com códigos `00/01/03/05`, `02` desconhecido,
wire 0/1/2/ausente, `false` sem proveniência,
município RFB, competências distintas e uma fixture
de bytes **v1 efetivamente publicados** antes de rotular ME.
Não criar deep link de sucesso para erro; anunciar mensagens
em `aria-live` sem ressuscitar resultado anterior A→B→A.
Histórico de duas fotografias não identifica data exata
da mudança. Este gate pertence à UI, não autoriza ETL.

## Gate R91: DV e completude condicionada antes de declarar ausência

A [contraprova offline R91](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_atomic_absence_gate_r91.mjs)
executou sobre o **blob exato** `a3765909` do leitor proposto na
[PR #362](https://github.com/franklinbaldo/ficha/pull/362), HEAD
`5707ac7c`. Um CNPJ com DV alterado (`33000167000102`)
passou em `planAtomicRecord`, provocou GET e retornou
`absent-in-snapshot`; a entrada correta `33000167000101`
foi encontrada. Um PB sintético com
`qtd_estabelecimentos=3` mas apenas um filho diferente,
e outro sem o agregado de contagem, **também** retornaram
`absent-in-snapshot`. O script dá **RED (`--strict` rc=1)**
para os três estados de ausência indevida; é caracterização
offline com decoder/fetch injetados, não incidência demonstrada no IA.

Controle empírico adicional, somente leitura, no IA `2026-07`:
**5 membros PB efetivos** das raízes `00000000`, `33000167`,
`60746948`, `18236120` e `00000006` apresentaram,
cada um, `qtd_estabelecimentos == estabelecimentos.length`
(7.902, 661, 9.999, 10 e 1, total **18.573**).
Todos os 18.573 identificadores completos recombinados
tiveram DV matematicamente válido e eram distintos **dentro
de suas respectivas raízes**. É observação dirigida de cinco
arquivos, **não** atestado de completude de todas as raízes,
nem prova de que agregado e lista venham de fontes independentes.

Antes de anunciar ausência de filial na competência, o leitor deve:
validar DV numericamente **antes de qualquer I/O**;
distinguir entrada inválida de filial ausente; verificar raiz,
competência e versão do PB; exigir um agregado de estabelecimentos
presente e coerente com a lista, além de unicidade da identidade
completa. Se contagem estiver ausente, divergente ou inconsistente,
retornar **consulta inconclusiva / ficha incompleta**, nunca ausência.
Mesmo quando a contagem bate, `absent-in-snapshot` continua
sendo conclusão **condicional aos bytes publicados**, não certidão
de inexistência cadastral na Receita. Os casos RED foram convertidos em testes na [#362](https://github.com/franklinbaldo/ficha/pull/362)
e a unicidade das filiais foi complementada pela
[#365](https://github.com/franklinbaldo/ficha/pull/365).

**Atualização SHA-exata na mesma rodada:** a 🦊 incorporou as duas
proteções na [PR #362, HEAD `a41f0701`](https://github.com/franklinbaldo/ficha/commit/a41f07018fb0d5895e77068ceec0b1fc18647eec).
Reexecutei **o mesmo script R91, sem modificações**, contra o
blob novo `08753b6c` no Ifro: `--strict` **PASS (rc=0)**,
CNPJ/DV inválido recusado antes de novo GET e contagens divergente
ou ausente recusadas. O contraste é **RED no blob `a3765909`
→ GREEN no blob `08753b6c`**, ambos em Node com mocks de
rede/decoder e hash Git conferido. Após os merges da #362 e #365, o leitor defensivo está na
`main`; ainda exige integração do decoder real ao produto e testes
Chromium/Pages do fluxo público completo. Não prova completude do acervo.

## Gate R92: renderer SSG não deve destruir o estado de evidência

**Contraprova executável de 19/11/2026**, na
[`main@03490dfa`](https://github.com/franklinbaldo/ficha/commit/03490dfa6c4513653b8519b5306394b19bc7c319),
módulos `pages.mjs` (blob Git `eb990058`) e `atomic-record.mjs`
(`3b63cc91`). O
[probe independente R92](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_atomic_ssg_contract_r92.mjs)
chama o **renderer e o controlador SSG reais** com um DOM mínimo e
`lookup` injetado, sem rede/IA. Para reproduzir no checkout do Ficha:
`node /path/ficha_atomic_ssg_contract_r92.mjs "$PWD/site/static/js/pages.mjs" --strict`.
Resultado atual: **1 PASS / 4 RED esperados**:

1. `renderCompanyRecord` recebe `state:artifact-unavailable, data:null,
   httpStatus:404` e apresenta **“Nenhum registro encontrado”**. É
   interpretação indevida: 404 de membro/ZIP é **consulta inconclusiva**.
2. `renderCompanyRecord` recebe `state:absent-in-snapshot` e apresenta
   a mesma mensagem genérica. A evidência é apenas **ausência daquela
   filial nos bytes válidos da competência**, não inexistência da empresa.
3. `mountCompany` recebe resultado tipado `artifact-unavailable` e
   anuncia **“Consulta concluída”**, promovendo um 404 inconclusivo a
   sucesso de UX.
4. A exceção por **HTTP 503** é corretamente reportada como indisponível,
   mas o texto adicional exige **HTTP Range**, específico do caminho
   Parquet, irrelevante para GET de membro PB.
   O positivo com ficha `found` renderizou a razão social corretamente.

Essas falhas foram reproduzidas **com injeção do resultado PB**, não são
alegação de que a busca PB já esteja ativada no Pages. Integração deve
ser **por `result.state`, nunca por `!result.data`**. Estados:
`found` renderiza ficha e só ele recebe anúncio de consulta concluída
com registro; `absent-in-snapshot` recebe texto limitado a filial e
competência; `artifact-unavailable` avisa consulta inconclusiva,
com ação para tentar novamente; exceções 503/CORS/timeout/integridade
não apagam a distinção nem mostram resultados anteriores. A história
de dois snapshots precisa de tratamento próprio para não colapsar
`artifact-unavailable` e `absent-in-snapshot`.

**Aceite antes da conexão com o formulário:** replay R92 estrito GREEN,
regressão com 404, 503, agregado incoerente, duplicata, A→B tardio e
A→B→A, decoder canônico real, origem/lookup da mesma competência,
Chromium Pages móvel/desktop. Não redirecionar silenciosamente para o
Parquet se a camada PB falhar. Donos da vertical: 🦊 Raposa e
🦇 Morcego, QA 🦉 Coruja/🦎 Lagarto, contrato 🐙 Polvo.

## Divisão de consultas

- **CNPJ exato:** Protobuf-first e lookup, sem Parquet nem relay.
- **História da mesma empresa:** dois Protobufs, cada qual da própria
  competência e wire compatível; diferença observada não data a mudança.
- **Razão social, pessoa, município, CNAE e agregados:** índices
  apropriados ou trilha analítica separada, com orçamento real de
  bytes, requests e latência antes da promoção.
- **CNPJ alfanumérico:** somente contrato 1.1 com descoberta
  `manifest-atomic-v1.1.json` opt-in e URL versionada, quando publicado;
  não tratá-lo como suportado por ZIP numérico v1.

## Gates antes de anunciar busca funcional

- Reutilizar e testar o decoder canônico, em vez de copiar resultados
  de fixtures e confundir prova de transporte com UI.
- Testes offline de seleção de estabelecimento, lookups, semântica #359 e falhas,
  incluindo CNPJ divergente, repetido, truncado, errado, alfa/v1,
  mapeamento de URL de shard trocado, Simples/MEI desconhecido e
  sócio PF sem CNPJ.
- E2E no Chromium **sem flags** e origem Pages em 375×812 e 1440×900
  para casos numéricos e negativos (404 ≠ 503), além de URL compartilhável,
  A→B tardia e A→B→A com seleção alterada.
- Medir carga do bundle e corpo por empresa, cold/warm, bytes totais
  (incluindo lookups, headers e redirecionamentos) e latência p50/p95
  com amostra declarada, sem assumir cache. As maiores empresas podem
  ter fichas significativamente maiores.
- Verificar que a busca **não** requisita `cnpjs.parquet` nem
  inicializa DuckDB para CNPJ exato; testes da UI e CI no SHA final.
- Não alterar ETL, manifesto, Internet Archive, cron nem provisionar
  proxy, CDN ou serviço pago. A integração respeita a fila e a
  coordenação do Elefante; a frente UI pertence ao Morcego/Raposa.

## Situação

**Prova de transporte + decode + seleção exata em Chromium experimental:
PASS. Leitor defensivo na main via #362/#365: SIM. Decoder real + formulário
público SSG com estados corretos: NÃO FEITO (R92: 4 RED).** Os testes não
comprovaram lookup por nome, histórico, alfanumérico v1.1, hashes
integrais dos 100 ZIPs, disponibilidade permanente do IA ou orçamento
p95 global. Esta RFC é proposta; não muda o comportamento publicado.
