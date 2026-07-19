# RFC 0001 — ETL V2: pipeline canônico tipado sob computação restrita

**Status:** Accepted  
**Data:** 2026-07-19  
**Escopo:** aquisição, ingestão, transformação, validação e publicação dos snapshots RFB

## 1. Resumo

Esta RFC propõe reorganizar o ETL do FICHA em torno de uma fronteira canônica:

1. preservar os ZIPs originais da Receita Federal no Internet Archive;
2. ler cada CSV como strings uma única vez por snapshot;
3. converter imediatamente para Parquets canônicos tipados e validados;
4. derivar todos os produtos públicos apenas desses Parquets canônicos;
5. executar joins e escritas em unidades limitadas por memória e disco;
6. medir tempo, throughput, memória, spill e espaço em disco em cada etapa;
7. manter validação independente entre fonte, camada canônica e produtos;
8. migrar em shadow mode fora do caminho crítico do job mensal até que a equivalência e o custo operacional estejam provados.

A proposta mantém DuckDB, Parquet, Internet Archive, os contratos públicos e as decisões já aceitas. O objetivo não é trocar de ferramenta: é mudar o formato da linha de produção para evitar parse repetido, materializações desnecessárias e otimizações baseadas em intuição.

## 2. Contexto

A Receita publica o CNPJ em ZIPs separados por tabela lógica e, nos conjuntos maiores, por vários arquivos da mesma tabela. O FICHA também tem um objetivo de preservação: os arquivos de origem devem ser espelhados no Internet Archive independentemente dos produtos derivados.

O ambiente de execução é propositalmente restrito. O pipeline precisa operar num runner com memória limitada, disco local finito e duração máxima de job. Incidentes anteriores já mostraram queries consumindo dezenas de GiB de spill e o filesystem de aproximadamente 70 GiB chegando à exaustão. Por isso, velocidade isolada não é a métrica de sucesso: uma mudança só é melhor se respeitar simultaneamente tempo, memória, disco e correção.

O pipeline atual já contém boas decisões locais — DuckDB, Parquet, escrita por chunks, materializações controladas e verificação de roundtrip — mas ainda usa o CSV de `estabelecimento` como fonte operacional em mais de uma etapa. No caminho normal com verificação, o maior dataset é parseado na carga inicial, novamente no writer chunked de `cnpjs` e novamente no roundtrip. Isso faz o custo de parse reaparecer e mistura ingestão com construção dos produtos finais.

Esta RFC propõe uma fronteira estável entre essas responsabilidades.

## 3. Decisões existentes preservadas

A RFC não substitui:

- [ADR 0004](../adr/0004-internet-archive-as-storage.md): Internet Archive como storage dos snapshots;
- [ADR 0006](../adr/0006-data-quality-pragmatic.md): validações simples em SQL, sem framework pesado;
- [ADR 0008](../adr/0008-three-parquet-architecture.md): produtos públicos especializados por padrão de acesso;
- [ADR 0009](../adr/0009-denormalization-and-roundtrip.md): liberdade para denormalizar, com equivalência como gate;
- [ADR 0011](../adr/0011-no-partitioning.md): não expor os Parquets públicos em layout Hive particionado;
- [ADR 0012](../adr/0012-ia-mirror-as-source-of-truth.md): mirror do IA como fonte durável;
- [ADR 0016](../adr/0016-backfill-strategy.md): snapshots históricos podem ser usados em execuções de backfill;
- [ADR 0017](../adr/0017-ibis-shared-analytical-layer.md): Ibis onde melhora expressão e compartilhamento, sem controlar obrigatoriamente toda execução física.

A camada canônica é interna ao build. Partes, buckets ou arquivos intermediários não alteram a decisão de publicar cada produto como um Parquet simples e estável.

## 4. Terminologia

### 4.1 Raw

ZIPs exatamente como publicados pela Receita, com nome, tamanho, checksum, origem e data de descoberta. São imutáveis e preservados no Internet Archive.

### 4.2 Canônico

Parquets internos, tipados e validados, ainda próximos das entidades da Receita: `empresa`, `estabelecimento`, `simples`, `socio` e lookups. São o contrato de entrada para todas as transformações posteriores.

A camada canônica não precisa copiar a representação textual da Receita. Ela preserva a semântica, a linhagem e as chaves necessárias para provar equivalência.

### 4.3 Produto

Artefatos públicos otimizados para consulta, como `cnpjs.parquet`, `raizes.parquet`, `socios.parquet`, `pessoas.parquet`, `enderecos.parquet`, `cnpj_cnaes.parquet` e `cnpj_contatos.parquet`.

### 4.4 Atômico

Fichas protobuf por raiz de CNPJ e o empacotamento usado para acesso pontual. Essa etapa consome dados tipados; sua estratégia de sharding fica fora do escopo desta RFC.

### 4.5 Shadow run

Execução experimental que produz o caminho novo e o compara ao caminho atual sem afetar o snapshot publicado. Um shadow run não é, por padrão, parte do job mensal.

## 5. Objetivos

1. **Parse único:** cada byte de CSV é parseado no máximo uma vez no caminho normal de um snapshot.
2. **Tipagem precoce:** strings existem na fronteira de ingestão; datas, números e flags assumem tipos canônicos antes das derivações.
3. **Working set limitado:** nenhuma etapa depende de manter todas as tabelas gigantes em memória.
4. **Recomeço barato:** uma falha após ingestão pode retomar dos Parquets canônicos válidos.
5. **Validação independente:** fonte valida canônico; canônico valida produtos.
6. **Determinismo:** mesma entrada e mesma versão produzem os mesmos registros, ordenação e decisões de deduplicação.
7. **Observabilidade:** cada estágio reporta linhas, bytes, tempo, throughput, memória e pico de disco.
8. **Migração incremental:** nenhuma reescrita big-bang do ETL mensal.
9. **Segurança operacional:** experimentos não podem colocar em risco o job mensal nem exigir coexistência de artefatos que exceda o filesystem do runner.

## 6. Não objetivos

- trocar DuckDB por Spark, Flink, Polars ou data warehouse;
- introduzir dbt, Great Expectations ou orquestrador distribuído;
- alterar o schema público nesta RFC;
- publicar a camada canônica para consumidores finais;
- escolher agora a quantidade definitiva de buckets, codec intermediário ou biblioteca do registry;
- migrar joins sensíveis a OOM para Ibis sem comparar o plano físico;
- enfraquecer o roundtrip para ganhar alguns minutos;
- executar necessariamente os dois pipelines completos no mesmo job mensal.

## 7. Princípios

### 7.1 Preservar antes de transformar

Um ZIP só pode ser descartado localmente depois de existir uma cópia verificável no Internet Archive. Upload concluído não basta: tamanho e checksum devem ser comparados ou registrados num manifest de aquisição.

A aquisição e o mirror são parte do produto, não apenas preparação do ETL.

### 7.2 Ler a fonte como strings

O schema de origem descreve posição e nome das colunas, mas o leitor usa `VARCHAR` para todas elas. Isso evita que inferência de tipos transforme irregularidade da Receita em corrupção silenciosa.

A conversão ocorre numa projeção explícita e versionada imediatamente antes da escrita canônica.

### 7.3 Tipos refletem semântica

- identificadores e códigos com zeros à esquerda permanecem strings;
- datas válidas tornam-se `DATE`;
- flags tornam-se `BOOLEAN` quando a semântica for binária;
- capital e quantias tornam-se `DECIMAL` com escala documentada;
- inteiros só são usados quando o campo é semanticamente numérico;
- uma chave numérica auxiliar só entra se benchmark no perfil real provar benefício líquido.

### 7.4 Parquet canônico é a fronteira de checkpoint

Depois que uma unidade foi convertida, validada e gravada como Parquet, nenhuma etapa posterior volta ao CSV correspondente no caminho normal.

Isso separa duas perguntas:

- conseguimos interpretar fielmente o dump da Receita?
- conseguimos construir corretamente os produtos do FICHA?

### 7.5 Processar em unidades limitadas

O pipeline prefere uma unidade por ZIP, parte ou bucket:

1. abrir ou extrair um ZIP;
2. parsear seu CSV;
3. projetar para o schema canônico;
4. validar o resultado;
5. escrever uma ou mais partes Parquet;
6. registrar métricas e linhagem;
7. liberar tabela temporária e CSV extraído.

O número de chunks não é constante arquitetural. Ele é escolhido para manter memória e disco abaixo de limites explícitos.

### 7.6 Separar layout lógico de layout físico

O dataset canônico tem schema lógico único. Fisicamente, pode ser escrito em partes por ZIP ou em buckets determinísticos para permitir joins limitados.

Isso não conflita com o ADR 0011: consumidores continuam recebendo um Parquet por produto. Buckets internos são técnica de execução, não API pública.

### 7.7 Cada regra tem um dono

- schema de origem: ordem, nomes, encoding e política de parse;
- schema canônico: tipos, casts, nulabilidade e chaves;
- transformação de produto: denormalização e campos derivados;
- publicação: nomes, checksums, URLs e manifest.

Uma regra de normalização não deve aparecer em vários writers independentes.

### 7.8 SQL, Ibis e Python têm papéis diferentes

- DuckDB SQL controla `read_csv`, `COPY`, codecs, row groups, sort, spill e decisões físicas;
- Ibis pode expressar projeções, joins e agregações quando o plano compilado for equivalente e medido;
- Python coordena estágios, manifests, retries e métricas;
- Pydantic, dataclasses ou tipos equivalentes validam a definição do registry, não um objeto por linha.

### 7.9 Correção precede velocidade

Uma transformação com dados conflitantes não pode escolher arbitrariamente uma linha apenas para restaurar cardinalidade. Duplicatas idênticas podem ser colapsadas; duplicatas semanticamente diferentes exigem regra determinística, quarentena ou falha com evidência.

### 7.10 Toda otimização carrega quatro números

Uma proposta de performance compara, no mesmo perfil de execução:

1. wall-clock;
2. pico de memória;
3. pico de disco, incluindo spill e arquivos simultâneos;
4. tamanho dos artefatos.

Resultado de laptop com paralelismo diferente do runner é exploração, não decisão.

## 8. Registry declarativo de schemas

A implementação deve ter uma fonte central de metadados para cada tabela da RFB. Ela não precisa ser uma biblioteca nova. O requisito é que o mesmo registro gere leitura, casts, validações básicas, documentação e fixtures.

Exemplo conceitual:

```python
TableSpec(
    name="estabelecimento",
    source=CsvSpec(
        columns=("cnpj_basico", "cnpj_ordem", "cnpj_dv", ...),
        all_strings=True,
        delimiter=";",
        quote='"',
        encoding_policy=("utf-8", "latin-1"),
        parallel=False,
        null_padding=True,
        strict_mode=False,
        max_line_size=16_777_216,
        invalid_row_policy="count-or-quarantine",
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
        bucket_key=None,  # decisão física opcional, não pressuposta pelo registry
    ),
)
```

O registry deve conter, no mínimo:

- ordem e nomes das colunas de origem;
- delimitador, quote e ausência de header;
- política de encoding, sniff e fallback;
- `parallel=false`, `null_padding=true`, `strict_mode=false` e `max_line_size` quando forem decisões load-bearing;
- política de linhas inválidas e sua contabilização;
- nome, tipo e nulabilidade canônica;
- expressão de cast ou normalização;
- chaves e cardinalidades esperadas;
- política para cast inválido;
- colunas críticas para publicação;
- versão do schema;
- campos de linhagem necessários;
- opções físicas opcionais, como bucket key, codec e row-group size.

### 8.1 Compatibilidade do reader

A primeira implementação do registry não pode mudar o comportamento do reader. O teste de aceitação da Fase 1 deve demonstrar que o registry gera SQL semanticamente idêntico ao `_create_table_from_csvs` atual, incluindo:

- sniff do primeiro MiB;
- escolha de UTF-8 ou Latin-1;
- fallback UTF-8 com política tolerante;
- `parallel=false` por causa de quoted newlines e `null_padding`;
- `max_line_size=16 MiB`;
- filtros de arquivos vazios;
- escaping de paths;
- schema all-`VARCHAR` na leitura.

Fixtures devem cobrir quoted newline, linhas ragged, encoding misto, arquivo vazio e path com apóstrofo. Só depois dessa equivalência o código manual pode ser removido.

### 8.2 Política de erro

Cada coluna tipada declara uma das políticas:

- **fail:** qualquer valor inválido interrompe o estágio;
- **null-and-count:** vira `NULL`, incrementa métrica e respeita limite;
- **preserve-as-string:** permanece textual por decisão semântica;
- **quarantine:** a linha vai para artefato de diagnóstico com arquivo e motivo.

Valores inválidos nunca devem desaparecer sem contagem. A política atual de tolerância a encoding deve evoluir de warning genérico para contagem verificável de linhas afetadas sempre que o motor permitir observá-la.

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
4. verificar presença remota e registrar checksum/tamanho;
5. permitir que a ingestão use a cópia local já baixada ou o mirror como fallback.

O manifest de aquisição é imutável por snapshot. Se a Receita substituir um ZIP sob o mesmo nome, o sistema trata como nova revisão da fonte.

### 10.1 Convivência com o cache de ZIPs

O processamento de um ZIP por vez não implica apagar o ZIP bruto imediatamente. A política `FICHA_DROP_ZIPS_AFTER_LOAD` continua pertencendo ao workflow:

- no job mensal, ZIPs podem permanecer para o `actions/cache` quando houver espaço;
- em runners pressionados por disco, o ZIP só é removido depois de o mirror remoto estar verificado;
- shadow runs preferem baixar ou reutilizar um snapshot histórico do IA sem competir com o cache do job mensal.

## 11. Estágio 1 — ingestão canônica

### 11.1 Unidade de trabalho

A unidade inicial é o ZIP publicado pela Receita. Como cada ZIP contém um CSV, extraímos somente o ZIP em processamento.

### 11.2 Fluxo

Para cada ZIP:

1. validar que contém exatamente o arquivo esperado;
2. aplicar a política de encoding do registry;
3. ler todas as colunas como strings;
4. adicionar `_source_file`, `_source_snapshot` e, quando necessário, número da revisão;
5. aplicar casts e normalizações canônicas numa projeção colunar;
6. escrever Parquet com row-group e codec explícitos;
7. validar contagem, chaves críticas, casts inválidos e schema;
8. persistir métricas do chunk;
9. remover tabela temporária e CSV extraído.

### 11.3 Partes e buckets

Duas formas físicas devem ser comparadas:

- **parte por ZIP:** menor complexidade e linhagem direta;
- **bucket determinístico por chave:** facilita joins limitados por `cnpj_base`.

Para joins grandes, a hipótese é processar o mesmo bucket de `estabelecimento`, `empresa` e `simples` em conjunto. A quantidade e a função de bucket serão escolhidas por benchmark. Cem prefixos são hipótese inicial, não decisão.

## 12. Estágio 2 — validação canônica

A validação raw → canônico acontece uma única vez e inclui:

- contagem total e por arquivo de origem;
- schema exato e versão;
- nulidade em colunas críticas;
- contagem de casts inválidos;
- unicidade e cardinalidade de chaves;
- detecção de duplicatas idênticas e conflitantes;
- distribuição básica e drift contra snapshot anterior;
- amostra determinística comparando strings de origem com valores canônicos formatados de volta quando reversível.

Essa etapa torna o canônico um oracle independente dos builders de produto. Porém, antes de remover o roundtrip que relê CSV, a migração deve provar a cadeia completa uma vez.

### 12.1 Gate triangular de migração

Durante a Fase 2 e novamente antes da virada da Fase 6, um snapshot completo deve executar comparação triangular:

```text
CSV raw ───────────────► canônico
   │                       │
   └────────► produto ◄────┘
```

O gate exige:

1. `raw → canônico`: contagens completas, casts e amostra determinística de campos reversíveis;
2. `raw → produto`: roundtrip atual, ainda relendo o CSV;
3. `canônico → produto`: novo oracle Parquet → Parquet;
4. concordância dos três caminhos para a mesma amostra determinística;
5. comparação completa por hashes ordenados ou invariantes quando viável.

O reparse do CSV só pode ser removido depois de pelo menos um snapshot real passar nesse gate. Depois da virada, o caminho normal mantém duas fronteiras independentes: raw valida canônico na ingestão; canônico valida produto na derivação.

## 13. Estágio 3 — derivação dos produtos

Todos os writers públicos passam a aceitar apenas Parquets canônicos.

### 13.1 Produtos de uma entidade

`cnpj_contatos`, `cnpj_cnaes` e `enderecos` leem somente as colunas necessárias do canônico de `estabelecimento`. Não se presume que um scan seja sempre mais rápido que vários; a query escolhida deve ser medida no perfil real.

### 13.2 Produto com joins grandes

`cnpjs.parquet` deve ser construído por unidade limitada:

1. selecionar uma parte ou bucket de `estabelecimento` canônico;
2. ler as partes correspondentes de `empresa` e `simples`;
3. executar join e projeção completos;
4. escrever parte transitória;
5. liberar relações daquela unidade;
6. combinar partes no Parquet final com ordenação determinística.

### 13.3 Produtos agregados

`raizes`, `socios` e `pessoas` seguem a mesma regra: Parquet canônico como entrada, materializações explícitas onde benchmarks mostrarem que fusão piora spill ou tempo.

### 13.4 Compressão

- artefatos duráveis tendem a ZSTD;
- transitórios podem usar LZ4 ou permanecer sem compressão;
- a escolha mede CPU, tamanho e pico de disco durante coexistência de partes, spill e arquivo final.

“Codec mais rápido” não é vitória se o runner ficar sem disco.

## 14. Estágio 4 — validação de produtos

### 14.1 Invariantes completos

- contagem esperada;
- chave não nula;
- unicidade quando aplicável;
- relações de cardinalidade;
- schema e tipos públicos.

### 14.2 Equivalência determinística

Uma amostra reproduzível do canônico é comparada ao produto numa única query ou pequeno conjunto de queries. Builder e oracle não compartilham a mesma expressão.

### 14.3 Estatísticas de regressão

- tamanho final;
- número e tamanho dos row groups;
- null percentages;
- min/max de chaves ordenadas;
- drift de contagem contra snapshot anterior.

## 15. Estágio 5 — publicação e retenção

A publicação permanece atômica:

1. upload de todos os artefatos públicos;
2. verificação remota de presença, tamanho e checksum;
3. construção da entrada do snapshot;
4. atualização do manifest somente depois de todos os gates.

A camada canônica pode ser:

- mantida como checkpoint local enquanto houver espaço;
- publicada no IA como artefato de build para backfills e reprocessamento;
- armazenada em storage temporário externo adequado ao seu tamanho.

GitHub Actions artifacts não são presumidos como solução para dezenas de GiB. Limites de quota, tamanho, retenção e custo do plano devem ser verificados antes; no free tier essa opção pode ser inviável. Artifacts servem, no máximo, para manifests, métricas, amostras, quarentenas pequenas ou checkpoints reduzidos.

A política de retenção do canônico exige ADR própria baseada em tamanho, upload, custo e frequência de retry. A arquitetura exige checkpoint serializável; não exige que ele seja público ou permanente.

## 16. Observabilidade obrigatória

Cada estágio e chunk registra, em formato humano e JSON:

- arquivos, bytes e linhas lidos;
- bytes e linhas escritos;
- tempo total;
- MB/s e linhas/s;
- pico de RSS;
- pico de `duckdb_tmp`;
- pico do diretório de trabalho;
- tamanho de partes transitórias e finais;
- codec, row-group size, threads e memory limit;
- casts inválidos, quarentena e duplicatas;
- versão do código, DuckDB, Ibis e registry.

O benchmark deve aceitar perfil `production` com conexão file-backed, `threads=1`, `memory_limit`, `temp_directory` e `preserve_insertion_order=false`.

A/Bs alternam ordem das variantes e reportam distribuição, não apenas o menor valor.

## 17. Idempotência e retomada

Cada estágio produz manifest contendo:

- versão da entrada;
- versão do schema;
- checksums das partes;
- métricas;
- status dos gates;
- versão do código.

Antes de refazer trabalho, o orquestrador verifica esse manifest. Arquivos finais são escritos em path temporário e renomeados depois da validação. Partes incompletas não entram num merge posterior.

## 18. Política operacional de shadow mode

Shadow mode é requisito de migração, mas não deve disputar disco, cache e prazo com o snapshot mensal.

Por padrão, shadow runs acontecem em workflow manual ou separado e usam uma destas formas:

1. **subconjunto representativo:** um ou mais ZIPs cobrindo casos de encoding, volume e cardinalidade;
2. **snapshot histórico completo:** mês já preservado no IA, executado sob demanda;
3. **runner com disco ampliado:** quando o gate exige coexistência completa dos dois caminhos;
4. **execuções separadas:** caminho antigo e novo rodam em jobs diferentes, com resultados comparados por manifests e hashes, evitando coexistência física.

O job mensal continua executando apenas o caminho estável até a Fase 6. Nenhuma fase exige esperar o mês seguinte: snapshots históricos permitem validar várias fases em dias, desde que cada gate use dados reais e perfil de produção.

Um shadow run completo só é válido quando registra pico de disco e demonstra margem. O alvo é permanecer abaixo de 80% do filesystem; ultrapassar isso bloqueia a virada mesmo com saída correta.

## 19. Plano incremental

### Fase 0 — baseline real

Executar o pipeline atual com métricas de estágio, memória e disco. O baseline pode vir do job mensal ou de backfill completo no mesmo perfil.

### Fase 1 — schema registry sem mudança de comportamento

Mover layouts, tipos-alvo, chaves e políticas para o registry. Gerar o mesmo SQL de leitura do reader atual e passar as fixtures load-bearing da seção 8.1.

### Fase 2 — canônico shadow de `estabelecimento`

Em workflow separado, gerar Parquet canônico sem alimentar produtos. Comparar contagem, amostra, throughput, tamanho e pico de disco. Executar o gate triangular CSV/canônico/produto pelo menos num snapshot real.

Critério de avanço: equivalência aprovada e custo compatível com o runner escolhido.

### Fase 3 — canônico de todas as entidades

Adicionar `empresa`, `simples`, `socio` e lookups. Testar parte por ZIP versus bucket interno. Ainda sem remover o caminho atual.

### Fase 4 — migrar produtos de baixo risco

Fazer `cnpj_contatos`, `cnpj_cnaes` e `enderecos` consumirem canônico. Comparar saída completa ou hashes ordenados contra o caminho atual, em jobs separados quando necessário.

### Fase 5 — protótipo de `cnpjs`

Implementar atrás do harness, com perfil de produção. Medir tempo, memória, spill e disco simultâneo. Não substituir o mensal até um run de escala real terminar com equivalência.

### Fase 6 — virar o pipeline

Depois de um snapshot completo em shadow mode e repetição do gate triangular:

- produtos passam a consumir somente canônico;
- reloads de CSV são removidos;
- canônico vira checkpoint oficial;
- caminho antigo permanece por uma release como fallback manual;
- job mensal passa a executar apenas o caminho novo.

### Fase 7 — camada atômica

Avaliar separadamente streaming Arrow, sharding do ZIP e paralelismo de protobuf.

## 20. Critérios de aceitação

A migração só é considerada concluída quando:

1. um snapshot real termina no runner padrão ou no runner oficialmente definido para produção;
2. nenhum CSV é parseado mais de uma vez no caminho normal;
3. uma falha após ingestão retoma dos Parquets canônicos;
4. produtos passam todos os gates existentes e novos;
5. o gate triangular passou antes da remoção do roundtrip raw → produto;
6. o registry reproduz as decisões load-bearing do reader atual;
7. nenhuma mudança de schema público ocorre sem SemVer;
8. pico de memória respeita `memory_limit` com headroom do SO;
9. pico do filesystem fica abaixo de 80% da capacidade;
10. decisões físicas relevantes têm benchmark no perfil de produção;
11. tempo total é igual ou menor que baseline, ou regressão é aceita por ganho explícito de robustez;
12. ZIPs raw e checksums estão preservados antes da publicação;
13. shadow runs não aumentam o risco operacional do job mensal.

## 21. Alternativas consideradas

### 21.1 Continuar derivando direto dos CSVs

Mantém parse repetido, acopla produtos à irregularidade da fonte e torna retomadas caras.

### 21.2 Carregar tudo numa tabela DuckDB persistente

É próximo do canônico, mas menos portátil e reutilizável entre jobs que Parquet. DuckDB continua motor; Parquet é contrato entre etapas.

### 21.3 Streaming Python linha a linha

Transfere parsing, tipagem e serialização para objetos Python e perde execução vetorizada.

### 21.4 Polars ou PyArrow como motor principal

Ferramentas fortes, mas não removem planejamento de spill, joins limitados e publicação. Trocar motor adiciona risco sem atacar a causa.

### 21.5 Spark

Resolve escala horizontal que o projeto não possui e adiciona custo operacional desproporcional.

### 21.6 Validar cada linha com Pydantic

Pydantic valida registry e configuração, não dezenas de milhões de registros. Tipagem acontece vetorialmente no DuckDB.

### 21.7 Rodar shadow completo dentro do job mensal

Rejeitado como padrão. Duplica artefatos no momento de maior pressão de disco e transforma uma migração experimental em risco de publicação. Só pode ocorrer se medição prévia provar margem suficiente e houver fallback operacional.

## 22. Consequências

### Positivas

- parse concentrado em uma única fronteira;
- tipos e regras deixam de ficar espalhados;
- produtos evoluem sem reabrir CSV;
- recuperação de falhas fica mais barata;
- benchmarks medem estágios substituíveis;
- joins grandes ganham estratégia limitada;
- roundtrip permanece forte com validação em duas fronteiras;
- shadow runs podem avançar com snapshots históricos sem aguardar cadência mensal.

### Custos

- mais arquivos intermediários e manifests;
- possível aumento temporário de armazenamento;
- registry vira componente crítico;
- migração exige shadow runs;
- bucketização adiciona complexidade se for adotada;
- retenção remota pode aumentar upload e custo;
- triangulação temporariamente mantém três caminhos de validação.

## 23. Questões abertas

1. O canônico deve ser retido no Internet Archive ou apenas durante o build?
2. Parte por ZIP é suficiente, ou `cnpj_base` precisa de bucketização?
3. Prefixo decimal ou hash produz melhor distribuição?
4. Qual codec minimiza tempo sem violar teto de disco?
5. O registry deve ser dataclass própria, Pydantic para metadados ou forma mínima?
6. O pipeline separa aquisição, ingestão e produtos em jobs retomáveis?
7. Quais normalizações pertencem ao canônico e quais aos produtos?
8. Qual runner e storage temporário suportam os gates completos de shadow mode?

Essas perguntas ficam para experimentos e ADRs menores. Aprovar esta RFC aceita princípios, fronteiras, gates e roadmap — não cada parâmetro físico.

## 24. Decisão solicitada

Revisores devem responder principalmente:

1. `raw → canônico → produtos` é a fronteira correta?
2. o canônico tipado deve ser o único input normal dos produtos?
3. joins por unidades limitadas devem substituir joins globais quando o benchmark mostrar risco?
4. o gate triangular é suficiente para remover posteriormente o reparse do CSV?
5. a política de shadow mode protege o job mensal e permite avançar usando backfills?

Depois da aprovação, as primeiras implementações propostas são apenas:

- métricas de baseline no perfil real;
- schema registry com equivalência exata do reader;
- canônico shadow de `estabelecimento` em workflow separado;
- gate triangular num snapshot histórico completo.

Nenhuma remoção do pipeline atual faz parte da primeira PR de implementação.
