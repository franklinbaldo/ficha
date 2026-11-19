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
aplica `.strict()` e a rejeita. Os testes Vitest usam somente linhas
sintéticas e documentam estas fronteiras, **sem corrigir nem aprovar**
qualquer mudança de validador, contrato ou versão.

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
