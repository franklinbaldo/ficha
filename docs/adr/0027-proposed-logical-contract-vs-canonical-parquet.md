# ADR 0027 (proposta) — contrato lógico OKF versus Parquet canônico obrigatório

**Status:** Proposed — não aprovado para implementação  
**Data:** 2026-11-16  
**Owner:** 🐙 Polvo (P4-B, [Ficha #247](https://github.com/franklinbaldo/ficha/issues/247))  
**Base de leitura:** `main@98c5c110d9938d55b40c91d2309c99e40b186758`  
**Deliberação:** proprietário / 🐘 Elefante, após 🐝 Abelha (P4-A)  
**Supersessão:** nenhuma enquanto a proposta não for aceita.

## Pergunta e recomendação sob deliberação

A [RFC 0001 Accepted](../rfc/0001-etl-v2-canonical-pipeline.md)
prevê `raw → Parquets canônicos tipados → Parquets públicos` como fronteira
obrigatória de checkpoint, com validação triangular durante a migração.
A [RFC 0001A Draft](../rfc/0001a-okf-relational-model.md) propõe
contratos relacionais OKF sobre os Parquets publicados e views/materializações
opcionais. **Draft não revoga Accepted.**

**Proposta, ainda não decidida:** tornar obrigatório o **contrato lógico
versionado**, não uma segunda família física de Parquets em todo job mensal.
Permitir checkpoints materiais quando houver ganho comprovado de retomada,
memória ou tempo. **Não** dispensar evidência independente raw→produto,
linhagem, tipagem, unicidade, equivalência e gates de publicação.

## Matriz de alternativas

| Dimensão | A: RFC 0001 (camada física obrigatória) | B: 0001A (OKF + publicados) | C: híbrida proposta |
|---|---|---|---|
| Oracle | Raw→canônico→produto, gate triangular | OKF descreve shape; **não prova os próprios bytes** | Raw→produto independente, triangular quando houver checkpoint |
| Contrato | Registry/casts/canonical Parquet | OKF/SQL, JSON Schema, Zod, views | OKF/versionamento; regras de ingestão permanecem explícitas |
| Custo | Escrita, leitura, coexistência e limpeza adicionais; retomada pode melhorar | Menos escrita obrigatória; risco de reparse após falha | Decidir checkpoint por medição de falha/retomada |
| Migração | Shadow writers antes do cutover | Evolução paralela do bundle OKF/CI | Nenhum cutover sem gates e rollback |
| SSG Rust | Independente da UI | Contrato consumível por JS/DuckDB-WASM | Contrato permanece fora do framework de páginas |

**Evidência, não extrapolação:** [histórico canônico de estabelecimento](../canonical-estabelecimento-history.md),
2026-04/Estabelecimentos0 (uma de dez partes), registrou **1.384.174.658 B**
de Parquet canônico, **344,2 s** de escrita, RSS pico **8.840,8 MiB** e
filesystem pico **43.655,7 MiB**, no ambiente histórico. Não é diferencial
A−B, não inclui todo snapshot e não demonstra custo monetário ou que o
armazenamento deva ser permanente. Requer comparação no mesmo runner,
mesmos dados, hashes e estratégia de retomada; não multiplicar por dez.

## Custo de recuperação: checkpoint local não é checkpoint durável

**Achado no HEAD `main@d6fab5d98d497e0115a88991e7394c5f704b84f4`:**
o [histórico do shadow](../canonical-estabelecimento-history.md) ainda afirma
que o GitHub Actions faz cache do *working tree* completo por parte. Porém,
a [PR #244 já integrada](https://github.com/franklinbaldo/ficha/pull/244)
removeu deliberadamente esse cache de dados reproduzíveis multi-GB.
O [workflow vigente](../../.github/workflows/canonical-shadow-history.yml)
declara explicitamente que **não há Actions cache da árvore histórica**;
`setup-uv enable-cache` armazena dependências, não o Parquet canônico.
O [workflow mensal](../../.github/workflows/etl-monthly.yml) persiste
*descriptor*, métricas e recibos pequenos como artifacts, mas isso **não
constitui** um checkpoint completo restaurável de raw/CSV/canônico num
runner novo. O artifact de uma parte do shadow contém Parquet e evidências,
não a árvore de origem completa. A afirmação histórica de cache deve ser
tratada como **desatualizada**, não como capacidade operacional atual.

Consequência para A versus B/C: uma escrita intermediária que só sobrevive
**durante o mesmo job/runner** pode evitar recomputação local de etapas, mas
**não comprova retomada entre runs**. Qualquer economia alegada depende de
retenção autorizada, identidade de bytes, transferência, restore no runner
substituto e política de descarte. Não autorizar novo cache multi-GB ou
transporte remoto por inferência desta ADR.

**Modelo de decisão a preencher, sem números presumidos:** para o mesmo
snapshot, runner e gates, medir `T_A/T_B` (tempo sem falha), `p_A/p_B`
(probabilidade de falha) e `R_A/R_B` (custo incremental de recuperar
uma falha). Com **no máximo uma falha**, A só ganha em tempo esperado se
`T_A + p_A × R_A < T_B + p_B × R_B`. A fórmula abreviada
`T_A - T_B < p × (R_B - R_A)` exige `p_A = p_B = p`;
**não assumir isso** quando a camada adicional altera risco de OOM/disco.
Falhas repetidas exigem modelar retries e caudas, não extrapolar a equação.
Sem checkpoint restaurável, `R_A` inclui recomputação integral, sem
crédito fictício de restore. Comparar também RSS/disco, bytes persistidos,
minutos de runner, transferência, retenção e custo monetário; **hard limits**
e integridade prevalecem sobre desigualdade de tempo. Probabilidades
são **desconhecidas** até medição, não premissas escolhidas.

**Contraprova barata antes de prometer retomada:** fixture offline,
checkpoint com SHA e recibo, interrupção controlada após a escrita e nova
instância isolada sem filesystem anterior; exigir que o restore autorizado
reconstrua **os mesmos bytes e linhagem** antes de derivar produtos.
Se não houver storage admissível ou restore verificável, marcar
`RECOVERY_NOT_PROVEN`, não `PASS`. Um ensaio real entre runners e custos
de operação requer planejamento/orçamento separados.

## Invariantes não negociáveis

1. **Custódia:** origem RFB, identidade de cada ZIP/parte, competência,
   hashes de conteúdo observados, contagens por parte e recibos; `HEAD`,
   tamanho, manifesto e ETag **não** são por si prova de igualdade de bytes.
2. **Oracle independente:** contagens completas, chaves globais, cardinalidade,
   deduplicação com conflitos, casts inválidos/limiares e amostras ou hashes
   comparados à origem. Não criar verificador que usa somente a própria
   saída sob verificação.
3. **Contrato:** PK/FK, tipos, nulabilidade, regras de cast, compatibilidade
   histórica e igualdade entre `DESCRIBE` dos Parquets e sidecars OKF,
   Zod e JSON Schema gerados. Qualidade de dados não decorre automaticamente
   de concordância de schemas.
4. **Compatibilidade pública:** manter Parquets, Protobuf, manifest, hashes,
   URLs, snapshots legados e rollback, sem alterar schema publicado nesta PR.
5. **Fronteira ETL/SSG Rust:** SSG compõe páginas/CSS; consultas permanecem
   em JS ES Modules com DuckDB-WASM/Parquet e/ou leitor Protobuf. OKF define
   relações e views, não gera HTML para milhões de CNPJs e não exige runtime
   Astro/Svelte. Versionar contratos de entrada e estados de erro da consulta.

## Fatos e riscos de implementação

- [`knowledge/README.md`](../../knowledge/README.md) define linhas OKF
  representativas e Parquets como dados massivos, **não** milhões de arquivos
  Markdown. [`knowledge/okf.schema.sql`](../../knowledge/okf.schema.sql)
  declara PK/FK; isso não prova integridade referencial de snapshots completos.
- [`scripts/generate-okf-contracts.sh`](../../scripts/generate-okf-contracts.sh)
  fixa `okf-parser==0.42.0` e gera Zod, JSON Schema, DuckDB/views. Há
  implementação parcial, não aceite de equivalência nem prova de origem.
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) filtra
  suites por `web/**`, `etl/**` e `ficha-py/**`, de modo que o `success`
  genérico de uma alteração só documental pode conter testes `SKIPPED`.
  Após [#255 integrada](https://github.com/franklinbaldo/ficha/pull/255),
  há [gate OKF próprio](../../.github/workflows/okf-contract.yml) para
  `knowledge/**`, Zod público/gerado e arquivos associados; o script
  gerador e a paridade JSON Schema/Parquet em dados reais **não são
  automaticamente cobertos por esse inventário textual**. Correções de
  nulabilidade, enums, versionamento, gerador e testes de runtime exigem
  decisão e owner P4-A; não editar seus arquivos nesta PR.
- A [RFC 0001 §8.2](../rfc/0001-etl-v2-canonical-pipeline.md)
  registra `null-and-count` sem limiar codificado e `quarantine` ainda
  declaratória. Nenhuma das alternativas físicas resolve isso sozinha.
- [Histórico canônico](../canonical-estabelecimento-history.md) evidencia
  uma parte real e auditoria global de chaves separada; **não** demonstra
  comparação triangular completa da produção mensal.
- [ADRs 0003](0003-schema-versioning.md), [0008](0008-three-parquet-architecture.md),
  [0009](0009-denormalization-and-roundtrip.md), [0012](0012-ia-mirror-as-source-of-truth.md)
  seguem válidos. Esta proposta não edita ADR Accepted nem RFC Accepted.

## Risco cruzado P0: compatibilidade wire não decorre do contrato OKF

A [revisão independente do Gavião na #246](https://github.com/franklinbaldo/ficha/pull/246#pullrequestreview-5930640389)
e o [teste da #252](https://github.com/franklinbaldo/ficha/pull/252)
produziram uma contraprova offline: um decoder Protobuf **v1 real**, ao ler
bytes v1.1 de `00000000E08G12`, ignora a nova tag alfanumérica e
reconstrói `00000000000012` sem erro. O leitor v1.1 recupera o CNPJ
correto. Isto demonstra risco de **corrupção semântica silenciosa** para
consumidores antigos; não demonstra que o snapshot novo foi publicado.

**Gate independente de A/B/C:** provar, com fixture v1→v1.1 e decoder legado
real, que a publicação/roteamento **não expõe bytes incompatíveis** a
consumidores v1. Versionar explicitamente URLs ou negociar protocolo no
ponto de entrega; validar a versão apenas no cliente atualizado é insuficiente
para proteger decoders externos já distribuídos. A estratégia exata pertence
a P0 (Castor, Lagarto, Gavião e Elefante), não a esta ADR. Nenhum schema
Protobuf, manifesto ou endpoint é alterado nesta PR.

**Contraprova P0-D [#252](https://github.com/franklinbaldo/ficha/pull/252),
HEAD `315a69e3`:** índices sintéticos v1 e v1.1, com 100 URLs
únicas cada, passam isoladamente `SnapshotEntrySchema.safeParse`.
Substituir uma URL v1.1 por URL v1 mantém o parser verde e cria
interseção de **1/100** entre namespaces. É fixture offline, não
colisão observada no IA. A [#246](https://github.com/franklinbaldo/ficha/pull/246)
rejeita 1.1 no manifesto legado, **mas não prova disjunção** dos ZIPs.
Exigir teste negativo de interseção v1×v1.1 no produtor e leitor
opt-in, além da unicidade dentro de cada índice.

## Fronteira de descoberta e artefatos SSG: dois contratos distintos

A incompatibilidade de Protobuf não se limita aos bytes. O
[`manifest.ts` v1 na main](../../web/src/schemas/v1/manifest.ts)
exige `shard_by: 'cnpj_base_prefix_2'` em **todos** os snapshots e
valida o objeto inteiro. O [negativo offline P0-D #252](https://github.com/franklinbaldo/ficha/pull/252)
mostrou que acrescentar um snapshot v1.1 com outro `shard_by` ao mesmo
índice invalida a descoberta v1 **mesmo mantendo `current` no mês v1**.
Isso evidencia risco de indisponibilidade histórica; o ensaio no frontend
v1 efetivamente implantado continua **NOT_RUN**. Separar o índice ou
negociar a versão *antes da descoberta* é decisão de P0, não do contrato
lógico OKF.

Há também um conflito independente entre geradores de páginas e dados:
o [protótipo SSG Rust P3-A #257](https://github.com/franklinbaldo/ficha/pull/257)
observou no `ssg 0.0.76` a inclusão de widget de busca genérica e a
emissão de `manifest.json` **de aplicativo** com `name=Static Site`,
diferente do `manifest.json` **RFB** (`current` e `snapshots`).
O [finalizador isolado do Morcego](https://github.com/franklinbaldo/ficha/blob/e2f3550f202626ada17859562258e4752edf18bd/experiments/ssg-p3a/scripts/finalize-output.mjs)
na branch P3-A rejeita formato inesperado, retira o widget conhecido e
isola o manifesto sintético; **não foi integrado à main**. Build da PoC
não equivale a ensaio de deploy, consultas reais ou paridade visual.

**Evidência adicional P3-A no HEAD `56c8bea487968dc9f393b9e0e1ac6535d2238039`**
([README da PoC](https://github.com/franklinbaldo/ficha/blob/56c8bea487968dc9f393b9e0e1ac6535d2238039/experiments/ssg-p3a/README.md)):
o `ssg 0.0.76` retornou **exit 0** embora emitisse
`Error generating manifest: Invalid manifest name: Name cannot be empty`;
o quality gate nativo marcou **9/10**, com reprovação do requisito de rodapé
promocional. Em executor Ifro, o owner P3-A relatou **27/27 testes Node**
e **12/12 estados axe/Chromium** (quatro estados × três larguras) sem
violações automatizadas selecionadas; mutar `label for` gerou falha
deliberada `AXE_FAIL`/exit 1. São evidências **da PoC offline**,
não de contrato RFB, leitura DuckDB/Protobuf, paridade de produção ou
acessibilidade manual. O CI padrão, filtrado por paths, **não executa**
build SSG/axe e não substitui essa prova local. **Já existe, porém, um gate
isolado no HEAD P3-A `65592bb6`**: [Actions #44015083398](https://github.com/franklinbaldo/ficha/actions/runs/44015083398)
terminou em **FAILURE**. Os passos executados de testes Node (27 casos),
build/finalizador, navegador local e axe (12 estados) foram `SUCCESS`, mas
o passo final de diagnóstico detectou justamente o erro interno do manifesto
apesar de `exit 0` do SSG. Isso é um **negativo real fail-closed em CI**,
não falha de ETL nem aprovação para merge/deploy. A falha permaneceu registrada como contraprova, mas a causa foi
posteriormente localizada pelo owner P3-A nos metadados de página; as
verificações de dados reais e rollback continuam `NOT_RUN`.

**Atualização P3-A em 17/11/2026, [HEAD `0a2db297`](https://github.com/franklinbaldo/ficha/commit/0a2db297c4e6850d98cccf32e6406ef1eb1d7a45):**
o `staticdatagen v0.0.32` exigia `name` nos metadados da página;
`site_name` em `config.toml` não bastava. A PoC acrescentou `name: FICHA`
no frontmatter e restringiu o isolamento do web manifest a um shape
validado, sem mudar o manifesto RFB. O [CI específico
#44022172722](https://github.com/franklinbaldo/ficha/actions/runs/44022172722)
concluiu `SUCCESS` no mesmo HEAD: testes Node **29/29**, build SSG real,
finalização/artefatos, Chromium em **3 larguras**, axe em **12 estados** e
step **`Detect upstream build diagnostics even on exit code zero` SUCCESS**.
Isto resolve o defeito **nesta PoC offline**, não transforma exit 0 em
oráculo universal. O quality gate nativo permanece **9/10** por exigência
de rodapé promocional; busca DuckDB/Protobuf real, paridade, leitor de tela,
rollback exercitado e deploy seguem **NOT_RUN**. A #257 permanece Draft.

**Gate extra de cutover, a cargo de P3-A:** validar artefatos e diagnósticos,
não apenas código de saída do processo; esclarecer/corrigir o erro de
manifesto em upstream ou estabelecer critério verificável de falha fechada,
sem transformar o finalizador experimental em contrato de produção por
omissão. O 9/10 deve ser interpretado pelo pilar faltante, não inflado
para 10/10 mediante inserção promocional. A decisão A/B/C não pode
ser confundida com aprovação desse corte de frontend.

**Gates antes de qualquer corte SSG:** nome/namespace separados e teste
negativo de colisão entre manifestos; conservação do contrato RFB/URLs e
base `/ficha/`; busca única especializada (sem widget genérico);
descoberta versionada compatível com frontend legado; teste de falha
fechada para formato desconhecido; hashes do build, paridade de consultas
e rollback do Pages anterior. O shell SSG não deve decidir formatos de
ETL nem promover snapshots. **A/B/C continuam independentes desse corte**;
a implementação pertence a P0-D/P3-A, após aceite e autorização de deploy.

## Revisão cruzada P4-A: contrato sem paridade semântica comprovada

A [matriz P4-A da Abelha](https://github.com/franklinbaldo/ficha/blob/e81d2e658988d1dcdb276ef6c47a650ed392fd43/docs/okf-contract-compatibility.md)
([PR #255 draft](https://github.com/franklinbaldo/ficha/pull/255)) mede **86 colunas**
em três relações: Cnpj (52), Raiz (18), Socio (16). Os nomes e tipos
básicos SQL/Zod concordam, mas há **28 diferenças já existentes de
nulabilidade** (22/1/5) e **sete domínios `z.enum` do contrato público v1
ampliados a `z.string()` no Zod OKF gerado** (3/1/3). O gerado pode
rejeitar `null` permitido por v1 e admitir códigos que v1 rejeita:
**não é substituto intercambiável do contrato público**, seja escolhida
a alternativa A, B ou C. A [revisão P4-A mais recente](https://github.com/franklinbaldo/ficha/pull/255)
também separa **campo ausente** de `null`: os 86 campos de dados do v1
continuam obrigatórios, inclusive se ambos os schemas fossem ampliados
simultaneamente com `.optional()` (uma checagem só de paridade não detectaria
essa regressão). Cobrir essa contraprova antes de trocar validadores.

**Atualização de integração em 17/11/2026:** a [PR #255 P4-A](https://github.com/franklinbaldo/ficha/pull/255)
foi **MERGED**, por squash [`5c10e4a8`](https://github.com/franklinbaldo/ficha/commit/5c10e4a81670c9079c9f7f868dedaa5d898fab52).
O [gate OKF #44012953206](https://github.com/franklinbaldo/ficha/actions/runs/44012953206)
executou **9 testes de biblioteca padrão**, incluindo **cinco mutações
negativas**, com resultado `SUCCESS` no HEAD P4-A `bdf6efe4`.
A `main@c3e4a3a` já contém o workflow e a matriz; isto **não** equivale
a conferir linhas reais em Parquet, `safeParse` em execução, JSON Schema,
igualdade histórica de snapshots, PK/FK em escala ou relações públicas fora
dessas três. Essas provas são **NOT_RUN** no escopo P4-B; tampouco houve
alteração contratual pelo merge documental/de testes.

**Pré-condição para deliberação executável:** escolher explicitamente
a autoridade para `null` e enums (schema público v1, gerado e/ou
versão do snapshot), exigir contrato versionado por competência,
cobertura de relações restantes e testes negativos de valores de
linha, não apenas `DESCRIBE`. A decisão A/B/C pode estabelecer um
alvo arquitetural, **mas não aprova troca de validador, remoção do
oracle independente ou migração/publicação** antes dessas provas e
das prioridades P0/P1. P4-A pertence à Abelha: nenhuma alteração no
arquivo, gerador ou schema dela é proposta aqui.

## Sequência e aceite antes de mudar arquitetura

1. **P0/P1 antes de P4:** estabilizar alfanuméricos/Protobuf, completude,
   CI visual, custódia e janela de publicação. Não executar ETL real.
2. **P4-A:** usar a matriz e o CI `knowledge/**` **já integrados** pela
   Abelha como baseline; deliberar autoridade de `null`/enums/versão e
   solicitar provas de `safeParse`, JSON Schema e linhas Parquet antes de
   alterar validadores. Evitar edição concorrente.
3. **Deliberação:** proprietário/Elefante escolhem A, B ou C, com
   definição explícita de oracle independente, checkpoint opcional,
   gatilhos de materialização, duração e descarte, e ADR de supersessão
   prospectivo se uma obrigação da RFC 0001 for substituída.
4. **Prova offline barata após aceite:** casos positivos e negativos com
   schema divergente, nulabilidade incompatível, chave duplicada,
   órfão de FK, parte ausente, fonte divergente e schema gerado obsoleto.
   Deve falhar fechado, sem rede.
5. **Benchmark posterior somente com orçamento/autorização operacional:**
   comparar A/B/C com mesma parte e runner, wall-clock, RSS, disco/spill,
   bytes adicionais e custo de retomada após falha, mais hashes finais.
   Medições de fixtures não autorizam cutover ou publicação.

## Decisão solicitada (PENDING)

**É aceitável substituir a obrigatoriedade de Parquet canônico físico por
OKF lógico + validação independente raw→produto, mantendo materialização
física sob demanda quando seu benefício for medido?**

Até a deliberação, **RFC 0001 = Accepted** e **0001A = Draft**.
Esta nota é somente **Proposed**: não promove contrato, altera dados,
remove roundtrip, troca deploy ou executa publicação. A decisão segue
o [plano/owners aprovados](https://github.com/franklinbaldo/ficha/issues/247#issuecomment-6929803160).
