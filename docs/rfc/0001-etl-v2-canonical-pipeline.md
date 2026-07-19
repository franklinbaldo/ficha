# RFC 0001 — ETL V2: pipeline canônico tipado sob computação restrita

**Status:** Proposed  
**Data:** 2026-07-19  
**Escopo:** aquisição, ingestão, transformação, validação e publicação dos snapshots RFB

## 1. Resumo

Esta RFC propõe reorganizar o ETL do FICHA em torno de uma fronteira canônica:

1. preservar os ZIPs originais da Receita Federal no Internet Archive;
2. ler cada CSV como strings, uma única vez por snapshot;
3. converter imediatamente para Parquets canônicos tipados e validados;
4. derivar todos os produtos públicos apenas desses Parquets canônicos;
5. executar joins e escritas em unidades limitadas por memória e disco;
6. medir tempo, throughput, memória, spill e espaço em disco em cada etapa;
7. manter validação independente entre fonte, camada canônica e produtos.

A proposta mantém DuckDB, Parquet, Internet Archive, os contratos públicos e as
decisões já aceitas. O objetivo não é trocar de ferramenta: é mudar o formato da
linha de produção para evitar parse repetido, materializações desnecessárias e
otimizações baseadas em intuição.

## 2. Contexto

A Receita publica o CNPJ em ZIPs separados por tabela lógica e, nos conjuntos
maiores, por vários arquivos da mesma tabela. O FICHA também tem um objetivo de
preservação: os arquivos de origem devem ser espelhados no Internet Archive,
independentemente dos produtos derivados.

O ambiente de execução é propositalmente restrito. O pipeline precisa operar num
runner com memória limitada, disco local finito e duração máxima de job. Incidentes
anteriores já mostraram que uma query aparentemente simples pode consumir dezenas
de GiB de spill. Por isso, velocidade isolada não é a métrica de sucesso: uma
mudança só é melhor se respeitar simultaneamente tempo, memória, disco e correção.

O pipeline atual já contém boas decisões locais — DuckDB, Parquet, escrita por
chunks, materializações controladas e verificação de roundtrip — mas ainda usa o
CSV de `estabelecimento` como fonte operacional em mais de uma etapa. Isso faz o
custo de parse reaparecer e mistura ingestão com construção dos produtos finais.

Esta RFC propõe uma fronteira estável entre essas responsabilidades.

## 3. Decisões existentes preservadas

A RFC não substitui as decisões abaixo:

- [ADR 0004](../adr/0004-internet-archive-as-storage.md): Internet Archive como
  storage dos snapshots;
- [ADR 0006](../adr/0006-data-quality-pragmatic.md): validações simples em SQL,
  sem framework pesado de data quality;
- [ADR 0008](../adr/0008-three-parquet-architecture.md): produtos públicos
  especializados por padrão de acesso;
- [ADR 0009](../adr/0009-denormalization-and-roundtrip.md): liberdade para
  denormalizar, com equivalência como gate;
- [ADR 0011](../adr/0011-no-partitioning.md): não expor os Parquets públicos em
  layout Hive particionado;
- [ADR 0012](../adr/0012-ia-mirror-as-source-of-truth.md): mirror do IA como
  fonte durável do projeto;
- [ADR 0017](../adr/0017-ibis-shared-analytical-layer.md): Ibis onde ele melhora
  expressão e compartilhamento, sem exigir que toda execução física passe por ele.

A camada canônica proposta aqui é interna ao build. Partes, buckets ou arquivos
intermediários não alteram a decisão de publicar cada produto como um Parquet
simples e estável.

## 4. Terminologia

### 4.1 Raw

Os ZIPs exatamente como publicados pela Receita, com nome, tamanho, checksum,
origem e data de descoberta. São imutáveis e preservados no Internet Archive.

### 4.2 Canônico

Parquets internos, tipados e validados, ainda próximos das entidades da Receita:
`empresa`, `estabelecimento`, `simples`, `socio` e lookups. São o contrato de
entrada para todas as transformações posteriores.

A camada canônica não precisa copiar a representação textual da Receita. Ela
preserva a semântica, a linhagem e as chaves necessárias para provar equivalência.

### 4.3 Produto

Artefatos públicos otimizados para consulta, como `cnpjs.parquet`,
`raizes.parquet`, `socios.parquet`, `pessoas.parquet`, `enderecos.parquet`,
`cnpj_cnaes.parquet` e `cnpj_contatos.parquet`.

### 4.4 Atômico

Fichas protobuf por raiz de CNPJ e o empacotamento usado para acesso pontual.
Essa etapa consome dados tipados; sua estratégia de sharding fica fora do escopo
desta RFC e pode ser tratada numa RFC própria.

## 5. Objetivos

1. **Parse único:** cada byte de CSV deve ser parseado no máximo uma vez no
   caminho normal de um snapshot.
2. **Tipagem precoce:** strings existem na fronteira de ingestão; datas, números,
   booleanos e códigos assumem tipos canônicos antes das derivações.
3. **Working set limitado:** nenhuma etapa deve depender de manter todas as
   tabelas gigantes em memória.
4. **Recomeço barato:** uma falha após a ingestão não deve exigir novo parse dos
   CSVs se os Parquets canônicos válidos ainda existem.
5. **Validação independente:** fonte valida canônico; canônico valida produtos.
6. **Determinismo:** mesma entrada e mesma versão de código produzem os mesmos
   registros, ordenação e decisões de deduplicação.
7. **Observabilidade:** cada estágio reporta linhas, bytes, tempo, throughput,
   memória e pico de disco temporário.
8. **Migração incremental:** nenhuma reescrita big-bang do ETL mensal.

## 6. Não objetivos

- trocar DuckDB por Spark, Flink, Polars ou um data warehouse;
- introduzir dbt, Great Expectations ou orquestrador distribuído;
- alterar o schema público nesta RFC;
- publicar a camada canônica para consumidores finais;
- escolher agora a quantidade definitiva de buckets, codec intermediário ou
  biblioteca do registry;
- migrar joins sensíveis a OOM para Ibis sem comparar o plano físico;
- enfraquecer o roundtrip para ganhar alguns minutos.

## 7. Princípios

### 7.1 Preservar antes de transformar

Um ZIP só pode ser descartado localmente depois de existir uma cópia verificável
no Internet Archive. Upload concluído não basta: tamanho e checksum devem ser
comparados ou registrados num manifest de aquisição.

A aquisição e o mirror são parte do produto, não apenas preparação do ETL.

### 7.2 Ler a fonte como strings

O schema de origem descreve posição e nome das colunas, mas o leitor usa
`VARCHAR` para todas elas. Isso evita que inferência de tipos transforme uma
mudança ou valor irregular da Receita em corrupção silenciosa.

A conversão ocorre numa projeção explícita e versionada imediatamente antes da
escrita canônica.

### 7.3 Tipos refletem semântica, não micro-otimização

- identificadores e códigos com zeros à esquerda permanecem strings;
- datas válidas tornam-se `DATE`;
- flags tornam-se `BOOLEAN` quando a semântica for binária;
- capital e quantias tornam-se `DECIMAL` com escala documentada;
- inteiros só são usados quando o campo é semanticamente numérico;
- uma chave numérica auxiliar só entra se um benchmark no perfil real provar
  benefício líquido.

O objetivo da tipagem é tornar estados inválidos difíceis de representar, não
apenas reduzir alguns bytes.

### 7.4 Parquet canônico é a fronteira de checkpoint

Depois que um chunk foi convertido, validado e gravado como Parquet, nenhuma
etapa posterior deve voltar ao CSV correspondente no caminho normal.

Isso separa duas perguntas:

- “conseguimos interpretar fielmente o dump da Receita?”;
- “conseguimos construir corretamente os produtos do FICHA?”.

### 7.5 Processar em unidades limitadas

O pipeline prefere uma unidade por ZIP, parte ou bucket:

1. abrir ou extrair um ZIP;
2. parsear seu CSV;
3. projetar para o schema canônico;
4. validar o resultado;
5. escrever uma ou mais partes Parquet;
6. registrar métricas e linhagem;
7. liberar a tabela temporária e o CSV extraído.

O número de chunks não é uma constante arquitetural. Ele é escolhido para manter
memória e disco abaixo de limites explícitos.

### 7.6 Separar layout lógico de layout físico

O dataset canônico tem um schema lógico único. Fisicamente, ele pode ser escrito
em partes por ZIP ou em buckets determinísticos para permitir joins limitados.

Isso não conflita com o ADR 0011: consumidores continuam recebendo um Parquet por
produto. Buckets internos são uma técnica de execução, não uma API pública.

### 7.7 Cada regra tem um dono

- schema de origem: ordem, nomes, encoding e política de parse;
- schema canônico: tipos, casts, nulabilidade e chaves;
- transformação de produto: denormalização e campos derivados;
- publicação: nomes, checksums, URLs e manifest.

Uma regra de normalização não deve aparecer em cinco writers independentes.

### 7.8 SQL, Ibis e Python têm papéis diferentes

- DuckDB SQL controla `read_csv`, `COPY`, codecs, row groups, sort, spill e outras
  decisões físicas;
- Ibis pode expressar projeções, joins e agregações quando o plano compilado for
  equivalente e medido;
- Python coordena estágios, manifests, retries e métricas;
- Pydantic, dataclasses ou tipos equivalentes podem validar a definição do
  registry, mas não devem instanciar um objeto Python por linha.

A ferramenta é escolhida pelo trabalho, não por uniformidade estética.

### 7.9 Correção precede velocidade

Uma transformação com dados conflitantes não pode escolher arbitrariamente uma
linha apenas para restaurar cardinalidade. Duplicatas idênticas podem ser
colapsadas; duplicatas semanticamente diferentes exigem regra determinística,
quarentena ou falha com evidência.

### 7.10 Toda otimização carrega quatro números

Uma proposta de performance deve comparar, no mesmo perfil de execução:

1. wall-clock;
2. pico de memória;
3. pico de disco, incluindo spill e arquivos simultâneos;
4. tamanho dos artefatos.

Resultado de laptop com paralelismo diferente do runner é exploração, não decisão.

## 8. Registry declarativo de schemas

A implementação deve ter uma fonte central de metadados para cada tabela da RFB.
Ela não precisa ser uma biblioteca nova. O requisito é que o mesmo registro gere
leitura, casts, validações básicas, documentação e fixtures.

Exemplo conceitual:

```python
TableSpec(
    name="estabelecimento",
    source=CsvSpec(
        columns=("cnpj_basico", "cnpj_ordem", "cnpj_dv", ...),
        all_strings=True,
        delimiter=";",
        encoding_policy=("utf-8", "latin-1"),
    ),
    canonical=ParquetSpec(
        columns=(
            Column("cnpj_base", "VARCHAR", source="cnpj_basico", required=True),
            Column("cnpj_ordem", "VARCHAR", required=True),
            Column("data_inicio_atividade", "DATE", cast="try_strptime(...)"),
            Column("situacao_cadastral", "VARCHAR", required=True),
            ...
        ),
        primary_key=("cnpj_base", "cnpj_ordem", "cnpj_dv"),
        bucket_key="cnpj_base",
    ),
)
```

O registry deve conter, no mínimo:

- ordem e nomes das colunas de origem;
- política de encoding e linhas inválidas;
- nome, tipo e nulabilidade canônica;
- expressão de cast ou normalização;
- chaves e cardinalidades esperadas;
- política para cast inválido;
- colunas críticas para publicação;
- versão do schema;
- campos de linhagem necessários.

### 8.1 Política de erro

Cada coluna tipada declara uma das políticas:

- **fail:** qualquer valor inválido interrompe o estágio;
- **null-and-count:** vira `NULL`, incrementa métrica e respeita um limite;
- **preserve-as-string:** permanece textual por decisão semântica;
- **quarantine:** a linha vai para um artefato de diagnóstico com arquivo e
  motivo.

Valores inválidos nunca devem desaparecer sem contagem.

## 9. Arquitetura proposta

```text
Receita Federal
      │
      ▼
[0. discovery + aquisição]
      │  ZIPs + checksum + source manifest
      ├──────────────────────────────► Internet Archive (raw imutável)
      │
      ▼
[1. ingestão canônica, um ZIP por vez]
      │  CSV all-VARCHAR → casts explícitos → validação
      ▼
canonical/{tabela}/part-*.parquet
      │
      ▼
[2. validação canônica e estatísticas]
      │
      ▼
[3. builders de produtos, Parquet → Parquet]
      │
      ├── cnpjs.parquet
      ├── raizes.parquet
      ├── socios.parquet
      ├── pessoas.parquet
      ├── enderecos.parquet
      ├── cnpj_cnaes.parquet
      └── cnpj_contatos.parquet
      │
      ▼
[4. camada atômica]
      │
      ▼
[5. upload + verificação remota + manifest]
```

## 10. Estágio 0 — discovery, aquisição e mirror

Para cada arquivo esperado:

1. resolver a URL canônica da Receita;
2. registrar nome, URL, tamanho, ETag quando houver e data de descoberta;
3. transmitir para o Internet Archive sem depender de uma cópia inteira extra;
4. verificar a presença remota e registrar checksum/tamanho;
5. permitir que a ingestão use a cópia local já baixada ou o mirror como fallback.

O manifest de aquisição é imutável por snapshot. Se a Receita substituir um ZIP
sob o mesmo nome, o sistema deve detectar a divergência e tratar como nova revisão
da fonte, não sobrescrever silenciosamente a evidência anterior.

## 11. Estágio 1 — ingestão canônica

### 11.1 Unidade de trabalho

A unidade inicial é o ZIP publicado pela Receita. Como cada ZIP contém um CSV,
extraímos somente o ZIP em processamento, não todos os arquivos antes de começar.

### 11.2 Fluxo

Para cada ZIP:

1. validar que contém exatamente o arquivo esperado;
2. detectar/aplicar a política de encoding do registry;
3. ler todas as colunas como strings;
4. adicionar linhagem, por exemplo `_source_file` e `_source_snapshot`;
5. aplicar casts e normalizações canônicas numa projeção colunar;
6. escrever Parquet com row-group e codec explícitos;
7. validar contagem, chaves críticas, casts inválidos e schema;
8. persistir métricas do chunk;
9. remover a tabela temporária e o CSV extraído.

### 11.3 Partes e buckets

Duas formas físicas devem ser comparadas:

- **parte por ZIP:** menor complexidade e linhagem direta;
- **bucket determinístico por chave:** facilita joins limitados por `cnpj_base`.

Para os joins grandes, a hipótese preferencial é particionar internamente as
entidades por prefixo ou hash de `cnpj_base`, processando o mesmo bucket de
`estabelecimento`, `empresa` e `simples` em conjunto. Isso é a forma explícita de
um grace hash join: troca um grande hash global por vários joins pequenos e
previsíveis.

A quantidade de buckets será escolhida por benchmark. Cem prefixos são uma boa
hipótese inicial, não uma decisão desta RFC.

## 12. Estágio 2 — validação canônica

A validação raw → canônico acontece uma única vez e inclui:

- contagem total e por arquivo de origem;
- schema exato e versão;
- nulidade em colunas críticas;
- contagem de casts inválidos;
- unicidade e cardinalidade de chaves;
- detecção de duplicatas idênticas e conflitantes;
- distribuição básica e drift contra o snapshot anterior;
- amostra determinística comparando strings de origem com valores canônicos
  formatados de volta quando a transformação for reversível.

Essa etapa substitui a necessidade de cada produto reabrir o CSV para ter uma
“fonte independente”. O canônico é independente do builder de produto porque foi
gerado e validado numa etapa anterior, com contrato próprio.

## 13. Estágio 3 — derivação dos produtos

Todos os writers públicos passam a aceitar apenas Parquets canônicos.

### 13.1 Produtos de uma entidade

`cnpj_contatos`, `cnpj_cnaes` e `enderecos` leem somente as colunas necessárias
do canônico de `estabelecimento`. O Parquet permite projection pushdown e evita
reparse de CSV.

Não se presume que “um scan” seja sempre mais rápido que vários scans. A query
escolhida será a mais rápida no perfil real, desde que leia o mesmo canônico e
produza saída equivalente.

### 13.2 Produto com joins grandes

`cnpjs.parquet` deve ser construído por unidade limitada:

1. selecionar um bucket de `estabelecimento` canônico;
2. ler os buckets correspondentes de `empresa` e `simples`;
3. executar o join e projeção completos;
4. escrever uma parte transitória;
5. liberar as relações daquele bucket;
6. combinar as partes no Parquet público final com ordenação determinística.

Essa organização evita reler CSV e torna a memória máxima uma função do tamanho
do bucket, não do snapshot inteiro.

### 13.3 Produtos agregados

`raizes`, `socios` e `pessoas` seguem a mesma regra: Parquet canônico como entrada,
materializações explícitas onde benchmarks mostrarem que fusão piora spill ou
tempo.

### 13.4 Compressão

- artefatos duráveis tendem a ZSTD;
- artefatos transitórios podem usar LZ4 ou permanecer sem compressão;
- a escolha exige medir CPU, tamanho e pico de disco durante a coexistência de
  partes, spill e arquivo final.

“Codec mais rápido” não é vitória se o runner ficar sem disco.

## 14. Estágio 4 — validação de produtos

Cada produto tem três classes de gate:

### 14.1 Invariantes completos

- contagem esperada;
- chave não nula;
- unicidade quando aplicável;
- relações de cardinalidade;
- schema e tipos públicos.

### 14.2 Equivalência determinística

Uma amostra reproduzível do canônico é comparada ao produto numa única query ou
pequeno conjunto de queries. O builder e o oracle não compartilham a mesma
expressão: o primeiro deriva o produto; o segundo lê o canônico validado e
compara campos observáveis.

### 14.3 Estatísticas de regressão

- tamanho final;
- número e tamanho dos row groups;
- null percentages;
- min/max de chaves ordenadas;
- drift de contagem contra o snapshot anterior.

## 15. Estágio 5 — publicação

A publicação permanece atômica:

1. upload de todos os artefatos;
2. verificação remota de presença, tamanho e checksum;
3. construção da entrada do snapshot;
4. atualização do manifest somente depois de todos os gates.

A camada canônica pode ser:

- mantida apenas como checkpoint local durante a migração;
- armazenada como artefato de workflow para retries curtos;
- publicada no IA como artefato de build para backfills e reprocessamento.

A política de retenção remota exige benchmark de tamanho e upload e será uma ADR
separada. A arquitetura exige que o checkpoint seja serializável; não exige ainda
que ele seja público ou permanente.

## 16. Observabilidade obrigatória

Cada estágio e cada chunk registra, em formato humano e JSON:

- arquivos, bytes e linhas lidos;
- bytes e linhas escritos;
- tempo total;
- MB/s e linhas/s;
- pico de RSS do processo;
- pico de `duckdb_tmp`;
- pico do diretório de trabalho;
- tamanho de partes transitórias e finais;
- codec, row-group size, threads e memory limit;
- casts inválidos, linhas em quarentena e duplicatas;
- versão do código, DuckDB, Ibis e schema registry.

O benchmark deve aceitar um perfil `production` que replique os PRAGMAs do job
mensal: conexão file-backed, `threads=1`, `memory_limit`, `temp_directory` e
`preserve_insertion_order=false`.

A/Bs devem alternar a ordem das variantes e reportar distribuição, não só o menor
valor observado.

## 17. Idempotência e retomada

Cada estágio produz um pequeno manifest contendo:

- versão da entrada;
- versão do schema;
- checksums das partes;
- métricas;
- status dos gates;
- versão do código.

Antes de refazer trabalho, o orquestrador verifica esse manifest. Um estágio pode
ser reutilizado somente se entrada, schema e código forem compatíveis.

Arquivos finais são escritos em path temporário e renomeados depois da validação.
Partes incompletas de uma tentativa anterior não entram num merge posterior.

## 18. Plano incremental

### Fase 0 — baseline real

Executar o pipeline mensal atual com métricas de estágio, memória e disco. Este é
o ponto de comparação; nenhum ganho de laptop substitui esse run.

### Fase 1 — schema registry sem mudança de comportamento

Mover layouts, tipos-alvo, chaves e políticas para o registry. Gerar a leitura
atual a partir dele e provar que os testes continuam iguais.

### Fase 2 — canônico shadow de `estabelecimento`

Gerar Parquet canônico ao lado do pipeline atual, sem alimentar produtos.
Comparar contagem, amostra, throughput, tamanho e pico de disco.

Critério de avanço: equivalência aprovada e custo compatível com o runner.

### Fase 3 — canônico de todas as entidades

Adicionar `empresa`, `simples`, `socio` e lookups. Testar parte por ZIP versus
bucket interno. Ainda sem remover o caminho atual.

### Fase 4 — migrar produtos de baixo risco

Fazer `cnpj_contatos`, `cnpj_cnaes` e `enderecos` consumirem canônico. Comparar
saída completa ou hashes ordenados contra o caminho atual.

### Fase 5 — protótipo bucketed de `cnpjs`

Implementar atrás do harness, com o perfil de produção. Medir tempo, memória,
spill e disco simultâneo. Não substituir o caminho mensal até um run de escala
real terminar com equivalência.

### Fase 6 — virar o pipeline

Depois de um snapshot completo em shadow mode:

- produtos passam a consumir somente canônico;
- reloads de CSV são removidos;
- canônico vira checkpoint oficial do build;
- caminho antigo permanece por uma release como fallback manual.

### Fase 7 — camada atômica

Avaliar separadamente streaming Arrow, sharding do ZIP e paralelismo da geração de
protobuf. Não misturar essa decisão com a ingestão canônica.

## 19. Critérios de aceitação da arquitetura

A migração só é considerada concluída quando:

1. um snapshot real termina no runner padrão;
2. nenhum CSV é parseado mais de uma vez no caminho normal;
3. uma falha após ingestão pode retomar dos Parquets canônicos;
4. produtos passam todos os gates existentes e novos;
5. nenhuma mudança de schema público ocorre sem SemVer;
6. pico de memória respeita `memory_limit` com headroom do SO;
7. pico do filesystem fica abaixo de 80% da capacidade do runner;
8. todas as decisões físicas relevantes têm benchmark no perfil de produção;
9. o tempo total é igual ou menor que o baseline, ou a regressão é aceita por um
   ganho explícito de robustez;
10. os ZIPs raw e seus checksums estão preservados antes da publicação.

## 20. Alternativas consideradas

### 20.1 Continuar derivando direto dos CSVs

Menor mudança imediata, mas mantém parse repetido, acopla produtos à irregularidade
da fonte e torna retomadas caras.

### 20.2 Carregar tudo numa tabela DuckDB persistente e derivar dali

É próximo do canônico, mas o banco interno fica menos portátil, menos inspecionável
e mais difícil de reutilizar entre jobs que Parquet. DuckDB continua sendo o motor;
Parquet é o contrato entre etapas.

### 20.3 Streaming Python linha a linha

Reduz abstrações, mas transfere parsing, tipagem e serialização para objetos
Python, perdendo execução vetorizada. Python deve coordenar, não tocar 70 milhões
de linhas individualmente.

### 20.4 Polars ou PyArrow como motor principal

São ferramentas fortes, mas não removem a necessidade de planejamento de spill,
joins limitados e publicação em Parquet. Trocar o motor agora acrescenta risco sem
atacar a causa arquitetural dos parses repetidos.

### 20.5 Spark

Resolve escala horizontal que o projeto não possui e adiciona custo operacional
muito maior que o dataset e o produto justificam.

### 20.6 Validar cada linha com Pydantic

Pydantic é adequado para validar o registry e configurações, não dezenas de
milhões de registros. Tipagem de dados deve acontecer vetorialmente no DuckDB.

## 21. Consequências

### Positivas

- parse da Receita concentrado em uma única fronteira;
- tipos e regras deixam de ficar espalhados;
- produtos podem evoluir sem reabrir CSV;
- recuperação de falhas fica mais barata;
- benchmarks passam a medir estágios substituíveis;
- joins grandes ganham uma estratégia física explícita e limitada;
- roundtrip permanece forte sem reparse por produto.

### Custos

- mais arquivos intermediários e manifests;
- possível aumento temporário de armazenamento;
- schema registry vira componente crítico;
- migração exige shadow runs e dupla execução por um período;
- bucketização interna adiciona complexidade que precisa ser justificada por
  benchmark;
- retenção remota do canônico pode aumentar tempo de upload.

## 22. Questões abertas

1. O canônico deve ser retido no Internet Archive ou apenas durante o build?
2. Parte por ZIP é suficiente, ou `cnpj_base` precisa de bucketização já na
   ingestão?
3. Prefixo decimal ou hash produz melhor distribuição e simplicidade?
4. Qual codec minimiza tempo sem violar o teto de disco?
5. O registry deve ser dataclass própria, Pydantic para metadados ou outra forma
   mínima?
6. O pipeline continua num job único ou separa aquisição, ingestão e produtos em
   jobs retomáveis?
7. Quais transformações de normalização pertencem ao canônico e quais pertencem
   exclusivamente aos produtos?

Essas perguntas são intencionalmente deixadas para experimentos e ADRs menores.
A aprovação desta RFC aceita os princípios, as fronteiras e o roadmap — não cada
parâmetro físico antecipadamente.

## 23. Decisão solicitada

Revisores devem responder principalmente a quatro perguntas:

1. `raw → canônico → produtos` é a fronteira correta para o FICHA?
2. o canônico tipado deve ser o único input normal dos produtos?
3. joins por unidades limitadas devem substituir joins globais quando o benchmark
   mostrar risco de spill?
4. o roadmap incremental protege suficientemente a confiabilidade do snapshot
   mensal?

Depois da aprovação, as primeiras implementações propostas são apenas:

- schema registry;
- métricas de baseline no perfil real;
- canônico shadow de `estabelecimento`.

Nenhuma remoção do pipeline atual faz parte da primeira PR de implementação.
