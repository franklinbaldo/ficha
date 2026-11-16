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
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) ativa
  verificações por caminhos `web/**`, `etl/**` e `ficha-py/**`;
  mudanças apenas em `knowledge/**` ou no script gerador podem deixar os
  gates relevantes sem execução. Corrigir a matriz OKF↔Zod↔Parquet,
  nulabilidade, JSON Schema e CI é o escopo **P4-A da Abelha**. Não editar
  arquivos dela nesta PR.
- A [RFC 0001 §8.2](../rfc/0001-etl-v2-canonical-pipeline.md)
  registra `null-and-count` sem limiar codificado e `quarantine` ainda
  declaratória. Nenhuma das alternativas físicas resolve isso sozinha.
- [Histórico canônico](../canonical-estabelecimento-history.md) evidencia
  uma parte real e auditoria global de chaves separada; **não** demonstra
  comparação triangular completa da produção mensal.
- [ADRs 0003](0003-schema-versioning.md), [0008](0008-three-parquet-architecture.md),
  [0009](0009-denormalization-and-roundtrip.md), [0012](0012-ia-mirror-as-source-of-truth.md)
  seguem válidos. Esta proposta não edita ADR Accepted nem RFC Accepted.

## Revisão cruzada P4-A: contrato sem paridade semântica comprovada

A [matriz P4-A da Abelha](https://github.com/franklinbaldo/ficha/blob/e81d2e658988d1dcdb276ef6c47a650ed392fd43/docs/okf-contract-compatibility.md)
([PR #255 draft](https://github.com/franklinbaldo/ficha/pull/255)) mede **86 colunas**
em três relações: Cnpj (52), Raiz (18), Socio (16). Os nomes e tipos
básicos SQL/Zod concordam, mas há **28 diferenças já existentes de
nulabilidade** (22/1/5) e **sete domínios `z.enum` do contrato público v1
ampliados a `z.string()` no Zod OKF gerado** (3/1/3). O gerado pode
rejeitar `null` permitido por v1 e admitir códigos que v1 rejeita:
**não é substituto intercambiável do contrato público**, seja escolhida
a alternativa A, B ou C.

A [Action OKF #43973816419](https://github.com/franklinbaldo/ficha/actions/runs/43973816419)
foi conferida no HEAD P4-A `e81d2e658988d1dcdb276ef6c47a650ed392fd43`:
job de compatibilidade e seus passos de checkout, SHA e comparação
**SUCCESS**; a PR documenta **6 testes offline, três negativos**.
Isto **não** prova linhas reais em Parquet, `safeParse` em execução,
JSON Schema, igualdade entre versões de snapshot, PK/FK em escala ou
ainda as relações públicas fora dessas três. `NOT_RUN` para essas
provas e para ETL. O gate `knowledge/**` existe na PR P4-A, não
deve ser presumido integrado à `main`.

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
2. **P4-A:** receber matriz da Abelha de nulabilidade/tipos, diffs dos
   artefatos gerados e CI para `knowledge/**`. Evitar edição concorrente.
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
