# ADR 0019 — Per-lookup parquets ao lado do `lookups.json`

**Status:** Aceito
**Data:** 2026-07-14
**Contexto:** docs/perf-plan-2026-05.md §10

---

## Contexto

`lookups.json` (`write_lookups_json`) resolve o caso de render síncrono no boot
do site — código→descrição para linhas já carregadas. Também é útil para
resolução de vocabulário em memória. Para SQL/Ibis/notebooks, porém, é
conveniente que os mesmos vocabulários existam como relações consultáveis.

Os lookup Parquets são extremamente baratos: o maior, municípios, tem cerca de
5.500 linhas e o conjunto inteiro fica abaixo de ~1 MB. Portanto não há motivo
para removê-los só para evitar duplicação de bytes.

## Decisão

Emitir, além de `lookups.json`, um parquet por lookup em
`<snapshot>/lookups/<kind>.parquet` (`write_lookup_parquets`), para os seis kinds
em `_LOOKUP_KINDS` (cnaes, motivos, municipios, naturezas, paises,
qualificacoes):

| Coluna | Tipo | Notas |
|--------|------|-------|
| `codigo` | VARCHAR | chave do lookup |
| `descricao` | VARCHAR | descrição original |
| `descricao_normalizada` | VARCHAR | `UPPER(strip_accents(descricao))` |

**Sort:** `codigo`. Cada arquivo cabe num único row group e é efetivamente
memory-resident na primeira leitura.

`lookups.json` **não é depreciado**: JSON continua servindo UI/resolução em
memória; os Parquets servem composição relacional em DuckDB/Ibis.

### Princípio de não-dependência

Lookup Parquet é uma **projeção auxiliar**, nunca uma dependência obrigatória
para interpretar uma linha de outro Parquet.

Quando uma descrição pertence naturalmente ao resultado de um dataset grande,
ela viaja denormalizada nesse dataset também. Parquet comprime muito bem valores
categóricos repetidos, então evitamos introduzir joins de renderização apenas
para economizar uma coluna de descrição.

Exemplos:

- `cnpjs.parquet` mantém códigos + descrições inline (ADR 0009);
- `cnpj_cnaes.parquet` mantém `cnae_codigo + cnae_descricao`;
- `enderecos.parquet` mantém `municipio_codigo + municipio_nome`;
- `pessoas.parquet` mantém `qualificacao_codigo + qualificacao_descricao`;
- `socios.parquet` já segue a mesma regra para qualificação e país.

O lookup externo continua valioso para consultas sobre o vocabulário em si —
por exemplo resolver texto normalizado para códigos — mas o consumidor pode
ler/renderizar uma linha analítica sem fazer join com ele.

Como qualquer outro artefato publicado do snapshot, cada lookup parquet nasce
com identidade de bytes completa no manifest:

- `size` — tamanho exato;
- `sha1` — checksum operacional para comparação direta com o catálogo do Internet Archive;
- `sha256` — digest forte preservado para consumidores e auditoria independente.

Entradas históricas URL-only continuam legíveis apenas como compatibilidade.
Novas competências não devem produzir lookup parquet sem os dois hashes.

## Frontend / camada analítica

`attachLookups(db, manifest)` registra os seis arquivos e cria uma
`VIEW lookup_<kind>` por kind. Isso permite consultas relacionais quando forem
a melhor forma de expressar o problema, por exemplo filtrar o vocabulário CNAE
por descrição normalizada antes de consultar o índice `cnpj_cnaes`.

Essa composição é opcional: não deve ser necessária apenas para recuperar a
descrição correspondente a um código que já veio em uma linha de resultado.

## Consequências

- ✅ lookup relations continuam disponíveis para SQL/Ibis/notebooks;
- ✅ `lookups.json` continua ótimo para UI e resolução em memória;
- ✅ Parquets analíticos ficam autocontidos para interpretação/renderização;
- ✅ duplicação de descrições é aceita porque a compressão colunar torna o custo baixo;
- ✅ lookup parquet pode ser reconciliado com o IA por `size + sha1`;
- ✅ SHA-256 continua disponível aos consumidores;
- ⚠️ +6 writes no ETL, cada um trivial;
- ⚠️ descrições inline adicionam colunas aos Parquets analíticos, mas são mudanças aditivas de schema.
