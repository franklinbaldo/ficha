# P4-A: matriz OKF / Zod / Parquet publicado

Estado de referência: `main@98c5c110d9938d55b40c91d2309c99e40b186758` (16/11/2026).
Escopo: **observação e testes de compatibilidade**, sem alterar schemas físicos,
manifesto, publicações, views ou a RFC 0001 (Accepted). A RFC 0001A continua Draft.

## Fronteira observada

| Relação publicada | Sidecar SQL OKF | Zod gerado | Zod público v1 | Colunas | Gaps de nulabilidade | Domínios enum v1 ampliados a string |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `cnpjs.parquet` | `knowledge/types/cnpj.schema.sql` | `CnpjSchema` | `EstabelecimentoSchema` | 52 | 22 | 3 |
| `raizes.parquet` | `knowledge/types/raiz.schema.sql` | `RaizSchema` | `RaizSchema` v1 | 18 | 1 | 1 |
| `socios.parquet` | `knowledge/types/socio.schema.sql` | `SocioSchema` | `SocioSchema` v1 | 16 | 5 | 3 |

Os **tipos básicos e os nomes das colunas** são equivalentes nesses três
contratos: `VARCHAR` e enums Zod são strings; `VARCHAR[]` é lista de strings;
`DOUBLE`, número; `INTEGER`, inteiro; `BOOLEAN`, booleano. A concordância
de *shape* não implica paridade de validação de valores.

Há **sete domínios de valores** presentes como `z.enum(...)` no contrato público
v1 que o Zod OKF gerado atualmente expressa como `z.string()` sem restrição:
Cnpj (`identificador_matriz_filial`, `porte_empresa`, `situacao_cadastral`),
Raiz (`porte_empresa`) e Socio (`tipo`, `tipo_descricao`, `faixa_etaria`).
Um código fora do enum público pode ser aceito pelo gerado, embora ambos
sejam strings em SQL. O teste offline fixa esses sete domínios e a dívida
atual, **sem** declarar equivalência de validação nem executar `safeParse`.

O Zod público v1 aceita `null` em 26 campos de Cnpj, 1 de Raiz e 9 de Socio;
o Zod OKF gerado só aceita em 4, 0 e 4, respectivamente. Em especial:

- **Cnpj**: `nome_fantasia`, `complemento`, `pais_codigo`, `ddd_1`,
  `opcao_simples` e outros 17 campos são `.nullable()` no schema
  público, mas estritos no gerado.
- **Raiz**: `ente_federativo_responsavel` é nullable somente no público.
- **Socio**: `cnpj_socio`, `pais_codigo`, `pais_nome`,
  `representante_legal_qualificacao_descricao` e `faixa_etaria`
  são nullable somente no público. O exemplo OKF contém um sócio PJ,
  insuficiente para inferir todos os casos PF/estrangeiro.

**Guard adicional do contrato público v1 (18/11/2026):** a lista de
campos que aceitam `null` é agora fixada independentemente do diff entre Zods:
26 em Cnpj, 1 em Raiz e 9 em Socio. O inventário de 28 divergências,
sozinho, não detectava a mudança **simultânea** dos dois contratos (por
exemplo, `cnpj` passando a `.nullable()` em ambos, ou
`cpf_mascarado` deixando de aceitar `null` em ambos). Três testes
adversariais cobrem alargamento e estreitamento simultâneos e uma segunda
falha: editar o inventário de dívida junto com o Zod público poderia alterar
indiretamente uma baseline derivada dessa dívida. A lista pública 26/1/9 agora
é **literal e independente** de `KNOWN_NULLABILITY_GAPS`; alterar qualquer
uma das duas exige revisão explícita do contrato e do versionamento.
Isto não valida valores reais, `safeParse` nem nulabilidade física do
Parquet e não altera a escolha de versão feita por consumidores.

Os 28 desacordos aparecem em
`knowledge/tests/test_contract_compatibility.py` como inventário explícito
de **dívida existente**, não autorização para aceitar linhas através do Zod
gerado. Se o gerador ou os contratos forem corrigidos por decisão posterior,
a lista deve ser revisada junto à mudança, com teste de valores reais/nulos.

## Fronteira provisória de validação (proposta, não aprovada)

Até a decisão da ADR 0027 e de versionamento, o contrato **público v1**
é a referência observável para valores recebidos pelas views. O Zod
gerado OKF é um artefato de modelagem, **não** substituto seguro para
`safeParse` de linhas v1: há 28 nulos permitidos que ele rejeita e 7
domínios de enum cujos valores ele alarga. A ausência de campo também é
diferente de `null`: os 86 campos de dados são obrigatórios no v1 atual.
Este guard rejeita `.optional()` em **qualquer coluna de dados**, inclusive
quando ambos os schemas passam a aceitá-la simultaneamente (um teste
de paridade sozinho não detectaria esse enfraquecimento). Campos de
metadados do gerado não são colunas e ficam fora desse teste.

A futura escolha de contrato por `schema_version`/competência e a
política de colunas extras nas views requerem decisão coordenada com
🐙 Polvo e 🐘 Elefante. Nada aqui altera o parser em runtime, o
Parquet físico, o contrato publicado ou a RFC 0001 aceita.

## Garantias e lacunas do teste isolado

O teste Python de biblioteca padrão compara as declarações SQL do OKF com
os schemas Zod v1 e gerado. Além dos campos individuais, fixa que cada
schema público v1 é exportado diretamente como `z.object({...});`, sem
modificadores no objeto inteiro: `.partial()` tornaria todas as colunas
opcionais sem alterar suas declarações, enquanto `.passthrough()` passaria
a aceitar colunas adicionais. Duas contraprovas sintéticas asseguram que
ambos os atalhos são rejeitados e exigem revisão explícita do contrato.
O leitor estático agora recusa qualquer linha não reconhecida dentro de
`z.object({...})`, exceto linhas vazias e comentários `//`. Antes ele
ignorava silenciosamente uma linha `/*` ou `*/` e podia **contar como ativa**
uma coluna situada dentro de um comentário de bloco; também ignorava spreads
`...outrosCampos` que acrescentam colunas não declaradas. Três mutantes
adversariais em `test_zod_static_parser_fail_closed.py` cobrem o campo
comentado no contrato público e no gerado e um spread público. Syntax
TypeScript nova/complexa exige revisão explícita em vez de um verde implícito.

Esta proteção é estática, não execução de Zod/`safeParse`. Falha quando coluna ou tipo muda, quando surge
uma nova incompatibilidade de nulabilidade, ou quando a lista de desvios
deixa de descrever o estado do código, ou quando um dos sete domínios
enum públicos muda sem revisão. Inclui cinco controles negativos em memória
(mutação de `cnpj` para número, remoção do `.nullable()` de
`cpf_mascarado`, exclusão de um valor permitido do enum `tipo`, ampliação
de um campo obrigatório para `.optional()` apenas no gerado e ampliação
simultânea nos dois schemas). Não cria Parquet, não acessa RFB/IA e não executa ETL.

**Não prova** nulabilidade do Parquet observado, contagem de valores nulos,
versões anteriores do snapshot, integridade de JSON Schema não versionado,
nem comportamento de `safeParse` em runtime. O sidecar SQL, sem
`NOT NULL`, também não deve ser tomado sozinho como evidência de
nulabilidade física. O teste ETL existente
`etl/tests/test_okf_published_schema.py` compara nomes/tipos com um
Parquet de fixture; não substitui testes de linha com `null`.

A cobertura OKF gerada abrange somente Cnpj, Raiz e Socio; não se deve
assumir automaticamente cobertura dos demais artefatos Parquet do manifesto.
O adaptador `web/src/lib/okf-row-schemas.ts` também não seleciona o
contrato pela versão do snapshot. Ambas são dependências de decisão do
Elefante/Polvo e não foram alteradas.

### Consumidor de views do Astro legado: fronteira observável

O módulo legado `web/src/lib/okf-views.ts` chama
`getOkfParquetRowSchema(view.output)` e depois `safeParse(row)`.
Esse adaptador usa **Zod gerado OKF** (`web/src/lib/okf-row-schemas.ts`),
não o schema público v1. Para `Socio`, o v1 permite
`cnpj_socio: null`, enquanto o gerado exige `z.string()`. Uma linha
sintética de sócio PF com esse campo nulo é aceita por
`web/src/schemas/v1/socio.ts`, mas recusada por
`validateOkfConvenienceViewRows('socios_de_raizes_ativas', ...)`.
O teste Vitest do consumidor registra explicitamente as duas expectativas,
sem alterar o comportamento atual.

Uma segunda diferença de aceitação também é exercitável sem consultar dados
remotos: o v1 rejeita códigos fora dos enums `tipo`, `tipo_descricao` e
`faixa_etaria` de `Socio`, mas o gerado usa `z.string()` e os admite.
Testes adicionais exercitam **validadores de campo** para dois nulos
representativos em `Cnpj.nome_fantasia` e
`Raiz.ente_federativo_responsavel`, e os outros quatro domínios enum
(`Cnpj.identificador_matriz_filial`, `Cnpj.porte_empresa`,
`Cnpj.situacao_cadastral`, `Raiz.porte_empresa`). Assim os sete enums
inventariados têm um controle runtime. Os testes anteriores de
`Cnpj` e `Raiz` isolam o validador **de campo**; não confundir esse nível
de prova com a aceitação de linha inteira.

Três testes adicionais usam **linhas sintéticas completas** de `Cnpj`
(52 colunas) e `Raiz` (18 colunas), enumeradas explicitamente sem derivar
as fixtures do gerador sob teste. Primeiro verificam que os dois validadores
aceitam a linha de controle. Em seguida comprovam, por `safeParse` da
**linha inteira**, a rejeição OKF de `null` permitido pelo v1 em
`Cnpj.nome_fantasia` e `Raiz.ente_federativo_responsavel`, e a aceitação
OKF de valores inválidos em quatro enums v1 de Cnpj/Raiz. Isso estende a
contraprova runtime aos três tipos. Um teste posterior percorre **28 testemunhas
literais de nulabilidade** em linhas sintéticas completas, independentes dos
schemas gerados: 22 campos Cnpj, 1 Raiz e 5 Socio. Para cada mutação isolada,
`safeParse` da linha no v1 deve aceitar o `null`, enquanto o OKF gerado
deve rejeitá-lo. Isso exercita os 28 desvios em runtime sem inferir que a
nulabilidade física dos Parquets reais ou a política do novo site SSG sejam
equivalentes.

Uma terceira diferença ocorre com coluna futura: o `z.object` público v1
aceita a linha e retira a chave desconhecida, enquanto o adaptador de views
aplica `.strict()` e a rejeita. Um controle posterior percorre **Cnpj, Raiz e
Socio com linhas completas independentes**: (a) linha válida aceita nos dois
validadores, (b) campo nullable **ausente** rejeitado nos dois, embora o mesmo
campo com `null` seja aceito pelo v1, e (c) coluna `future_column` aceita e
descartada pelo v1, mas rejeitada pelo gerado OKF. Assim, `null` não é
confundido com ausência nem a política de evolução de colunas é inferida
a partir de um único tipo. Todos são casos sintéticos do consumidor legado
Astro; não provam Parquet físico nem o SSG público. Os testes documentam
estas fronteiras, **sem corrigir nem aprovar** mudança de validador,
contrato ou versão.

O consumidor `validateOkfConvenienceViewRows('empresas_ativas', ...)` também
recebe `Cnpj` e apresenta, em linha sintética completa, as duas direções:
rejeita `nome_fantasia: null` que o v1 aceita e admite `porte_empresa: '99'`
que o v1 recusa. A regressão chama o consumidor real, sem consultar Parquet,
IA ou a home SSG; não autoriza substituir contratos nem publicar dados.

A regressão de lote sintético estende essa observação para a prévia de duas
linhas. No consumidor `empresas_ativas`, um segundo Cnpj com
`nome_fantasia: null` (v1 válido) interrompe a validação identificando
**row 1**, ao passo que um segundo Cnpj com `porte_empresa: '99'` (v1 inválido)
é devolvido normalmente pelo validador OKF. Na view
`socios_de_raizes_ativas`, o segundo sócio PF com `cnpj_socio: null`
também produz erro em **row 1**, apesar da aceitação pública v1. São
testemunhas de **validação de output e de lote**, não uma execução de SQL nem
afirmação de que essas linhas estejam nos Parquets publicados. Em especial,
a view relacional declara `Raiz` como **input**, mas
`validateOkfConvenienceViewRows` valida somente o **output Socio**;
seu sucesso não atesta a nulabilidade, integridade referencial ou versão
da relação `Raiz` consumida pelo JOIN.

A regressão seguinte distingue explicitamente **formato da saída** de
**correção semântica da consulta**. Uma linha Cnpj sintética com
`situacao_cadastral: '08'` é válida no Zod v1 e passa pelo validador
OKF de saída de `empresas_ativas`, embora a SQL da view filtre
`situacao_cadastral = '02'`. Analogamente, um Socio sintético com
`cnpj_base: '99999999'` passa pelo validador de saída de
`socios_de_raizes_ativas` sem que uma relação Raiz correspondente tenha
sido apresentada ao validador. **Isso não demonstra defeito no SELECT/JOIN**:
o teste deliberadamente não executa SQL. Demonstra apenas que
`validateOkfConvenienceViewRows`, quando invocado isoladamente, não
atesta a seleção ativa, a existência da Raiz, a cardinalidade do JOIN,
a competência dos inputs ou a integridade referencial. A página Astro
legada efetua a consulta separadamente antes de chamar o validador;
nem essa composição nem o teste provam a versão/proveniência dos Parquets
consumidos. O site público SSG permanece fora do escopo.

Essa diferença é **dívida já conhecida**, não um incidente novo na
home pública SSG (`site/`), que é separada do módulo Astro `web/`.
O teste não consulta Parquet remoto nem prova que determinada linha
foi publicada. Antes de usar esse validador numa busca SSG ou substituir
o contrato gerado, decidir com 🐙 Polvo/🐘 Elefante qual contrato lógico
e versão governam valores `null`, enums e campos ausentes (ADR 0027,
PR #253 ainda em proposta). Não equiparar a paridade estática SQL/Zod
à aceitação de linhas pelo validador.

## Baseline independente de colunas públicas v1 (18/11/2026)

O teste adicional `knowledge/tests/test_public_v1_column_baseline.py` congela
**nome e tipo escalar de cada uma das 86 colunas v1** (Cnpj 52, Raiz 18,
Socio 16) em `knowledge/tests/public-v1-column-kinds.json`. Essa baseline
literal **não é regenerada** dos SQL ou Zods durante o teste; uma edição
sincronizada dos três artefatos ainda poderia satisfazer o teste de paridade
anterior, mas agora falha se alterar o contrato público v1. As duas
contraprovas em memória simulam (a) renomear `cnpj` e (b) trocar
`capital_social` de número para string nos três contratos simultaneamente.
As duas conservam paridade e são rejeitadas pela baseline.

A baseline fixa nomes/tipos básicos; nulabilidade (26/1/9), obrigatoriedade,
domínios enum e modificadores Zod de objeto continuam cobertos pelos testes
separados. A atualização intencional de uma coluna pública exige análise de
versionamento, não regeneração automática desta fixture. Não há validação de
`safeParse`, de linhas reais ou do Parquet físico nesta fatia.

## Gate enxuto

`.github/workflows/okf-contract.yml` observa `knowledge/**`, os schemas
Zod públicos v1, `ficha-okf.zod.ts`, este documento e o próprio workflow.
Roda `python3 -m unittest discover -s knowledge/tests -p 'test_*.py' -v`
sobre o SHA de origem da PR, sem instalar pacotes, gerar dados ou usar
segredos. Uma PR que só altera `knowledge/**` não deve depender do filtro
`web/**` do job atual de CI, que não observa esse diretório.

**Próxima decisão, não executada:** alinhar política de `null` e versão
com o dono do contrato público, e só então revisar gerador/consumidores e
acrescentar provas de `safeParse`/Parquet sobre fixtures. A avaliação das
RFCs 0001/0001A pertence ao Polvo e ao Elefante. Não se propõe nova
materialização física.

### Regressão de SQL executável com fixtures isoladas (R164)

`knowledge/tests/test_okf_view_sql_semantics.py` lê a declaração JSON
das duas views no artefato TypeScript **gerado** e executa o próprio SQL
em relações SQLite sintéticas e pequenas. Confere que `empresas_ativas`
seleciona código `02`, mas não `08` ou `NULL`; e que o JOIN de
`socios_de_raizes_ativas` exclui raiz ausente, contagem zero e `NULL`.

Uma contraprova adicional alimenta **duas linhas Raiz para o mesmo
`cnpj_base`**, ambas ativas: o JOIN devolve a mesma linha Socio duas
vezes. Isso demonstra que a SQL não impõe unicidade de raiz e que a
cardinalidade depende de um contrato físico/relacional externo.

**Limite essencial:** SQLite cobre somente o subconjunto SELECT/JOIN
portável destas duas declarações; não executa DuckDB-WASM, Astro,
Parquet remoto ou dados RFB. Não comprova duplicatas reais nem erro no
produto. A próxima validação precisa de DuckDB com PK/FK e um oráculo
raw→consulta→produto sob versão fixada. Nenhuma política foi aprovada
pela ADR 0027; esta PR continua DRAFT/HOLD.

### DuckDB e Parquet sintético: teste de cardinalidade e chaves (R165)

A regressão `etl/tests/test_okf_view_duckdb_parquet.py` executa as
**declarações SQL geradas existentes**, sem reescrevê-las, sobre três
relações criadas como arquivos Parquet locais pelo DuckDB. O caso ativo
`'02'` continua separado de `'08'`/NULL. Duas linhas Raiz ativas com a
mesma chave e dois sócios distintos dessa chave produzem **quatro**
linhas no JOIN gerado, enquanto um oráculo independente `EXISTS`
preserva **duas**: a divergência decorre da multiplicidade física de
Raiz, não do `safeParse` de Socio. Esse oráculo representa a regra
hipotética «ao menos uma Raiz ativa», que ainda precisa de aprovação
semântica.

O teste também tenta carregar o mesmo Parquet duplicado numa tabela
DuckDB com `PRIMARY KEY(cnpj_base)` e exige erro de restrição;
outro controle, com `PRIMARY KEY` e `FOREIGN KEY` **declaradas**
em tabelas DuckDB independentes, rejeita tanto uma segunda Raiz quanto
um sócio órfão e faz JOIN e `EXISTS` coincidirem preservando os dois
sócios válidos. **Uma tabela DuckDB restrita não torna automaticamente
os arquivos Parquet publicados restritos.** Nenhuma política de
deduplicação, abortar publicação ou substituir JOIN por `EXISTS`
é decidida por esta regressão.

São dados locais, sintéticos e com apenas as colunas consumidas pelas
views. Não são os 52/18/16 campos inteiros, não exercitam DuckDB-WASM
no navegador nem autenticam Parquet publicado, snapshot por competência,
PK/FK reais, integridade do JOIN em produção ou o site SSG. O teste
deve ser exercido no job ETL Linux/Windows com DuckDB instalado;
o gate OKF Python stdlib continua separadamente. A ADR #253 e a
decisão de contrato/versão permanecem em HOLD.


### Competência sintética: pareamento de snapshots em SQLite (R166)

`knowledge/tests/test_okf_view_sql_semantics.py` agora executa a SQL
gerada existente sobre dois conjuntos **sintéticos** de Cnpj, Socio
e Raiz rotulados `2026-07` e `2026-08`. Os rótulos são apenas
identificadores de fixtures; não afirmam versões RFB publicadas.
Em cada competência, o teste audita unicidade da chave Raiz e
ausência de sócio órfão, comparando as duas views com um oráculo Python
independente construído das tuplas literais de origem.

Uma contraprova troca somente as linhas Raiz pela competência vizinha.
Cada conjunto isolado preserva unicidade de chave e existência dos pais,
mas o resultado do JOIN deixa de coincidir com o oráculo do mês pedido.
**Validação individual de chave/FK não garante pareamento temporal**:
é preciso um contrato explícito de versão, competência e proveniência
de todos os inputs. Este teste não escolhe política de versionamento.

Esta regressão é **SQLite em memória**, não um teste DuckDB de
snapshot físico nem dos Parquets publicados. O experimento R165
anterior continua cobrindo DuckDB/Parquet sintético, sem competência.
Não houve acesso a RFB, Internet Archive, dados publicados, DuckDB-WASM
ou site SSG. ADR #253 e PR #346 seguem DRAFT/HOLD; não há migração
de contrato, ETL ou publicação autorizada.

### Saída idêntica, competência distinta (R167)

Teste SQLite sintético: Raiz de `2026-07` tem 1 ativo; de `2026-08`, 2.
Hashes das tuplas diferem, mas a SQL gerada devolve os mesmos sócios.
Chaves únicas, ausência de órfãos e paridade da saída **não provam**
pareamento temporal: requer-se proveniência/competência de **cada input**.
Não usa Parquet RFB, site publicado ou hashes de arquivos reais;
nenhum contrato ou produto é alterado.
