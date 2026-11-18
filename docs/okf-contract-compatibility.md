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
