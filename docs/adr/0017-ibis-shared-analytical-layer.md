# ADR 0017 — Ibis como camada analítica compartilhada (`ficha-py`)

**Status:** Proposed
**Data:** 2026-05-07

## Contexto

FICHA tem duas superfícies que falam com os mesmos dados parquet:

1. **ETL** (`etl/src/ficha_etl/transform.py`) — escreve `cnpjs.parquet`,
   `raizes.parquet`, `socios.parquet` via joins + selects denormalizados
   (ADR 0009) usando `con.execute(f"...")` com SQL inline em DuckDB.
2. **Frontend web** — lê os parquets via DuckDB-WASM dentro do navegador.
3. **Notebooks Google Colab pré-configurados (planejado)** — usuários
   analistas vão querer fazer perguntas em Python contra os mesmos parquets,
   provavelmente via `duckdb` em embedded mode lendo direto do IA via
   `httpfs`.

A vocabulário analítico (o que é "uma raiz", "sócios de X", "filiais por
UF" etc.) hoje vive em três lugares incompatíveis: SQL strings em
`transform.py`, queries em TypeScript em `web/src/lib/analytical.ts`, e
em nada ainda do lado dos notebooks. Adicionar uma terceira encarnação
desse vocabulário (em SQL string num notebook) garante divergência: o
"capital social total por UF" no notebook não vai ser bit-exato com o
mesmo conceito no frontend, e debugar isso é trabalho artesanal.

A questão prática: introduzir o vocabulário compartilhado **agora**
(antes de existir o notebook) custa um ADR + um pacote pequeno; introduzir
**depois** custa migração das queries da ETL e desfazer o lock-in mental
de "ETL escreve SQL, notebook escreve SQL, são coisas separadas".

## Decisão

Adotar **Ibis** como linguagem de expressões analítica compartilhada,
encapsulada num novo pacote Python `ficha-py` (publicado no PyPI) que
serve as três superfícies.

### Arquitetura

```
ficha-py/                    ← novo pacote, ~200 LOC pra começar
├── connect.py               ← helpers: connect_local(parquet_dir),
│                              connect_ia(month) → DuckDB+httpfs+IA URLs
├── tables.py                ← refs Ibis: cnpjs(con), raizes(con), socios(con)
├── views.py                 ← composables: filiais_de(t, cnpj),
│                              socios_de(t, cnpj_base), por_uf(t, uf)
└── README.md                ← uso em Colab + uso no ETL

etl/src/ficha_etl/transform.py
└── importa ficha_py.tables/views pra os JOINs/SELECTs
└── mantém SQL raw no I/O-pesado (read_csv com encoding/max_line_size/
    ignore_errors knobs e COPY TO PARQUET com sort/bloom)

notebooks/                   ← nova pasta com .ipynb pre-configurados
├── 01-comecando.ipynb       ← "instala ficha-py, conecta no IA, lê 1 CNPJ"
├── 02-busca-por-uf.ipynb
└── 03-grafo-de-socios.ipynb
```

### Divisão de responsabilidades

**Ibis (em `ficha-py`):**

- Definição das tabelas analíticas (refs com schema)
- Joins/selects compartilhados (a lógica de "denormalizar empresa +
  estabelecimento + simples num cnpjs.parquet" expressa uma vez)
- Helpers analíticos comuns (filiais, sócios bidirecional, agregações)
- Validação de schema via `ibis.Schema`

**SQL raw em DuckDB (mantido na ETL):**

- `read_csv(...)` com `encoding`/`ignore_errors`/`max_line_size` —
  Ibis não abstrai esses knobs e passar pra `con.raw_sql()` derrota o
  propósito.
- `COPY TO ... (FORMAT PARQUET, ROW_GROUP_SIZE 200000, SORT BY cnpj)` —
  o ADR 0008 prescreve sort + bloom específicos que vivem mais natural
  em SQL.
- Roundtrip-equivalence test (ADR 0009) — opera no parquet bruto.

### Backends suportados (via Ibis)

| Onde | Backend | Como `ficha-py` conecta |
|---|---|---|
| ETL (CI runner) | DuckDB local | `connect.local(parquet_dir)` |
| Notebook Colab | DuckDB + httpfs lendo IA | `connect.ia(month="2026-04")` |
| Frontend (futuro) | DuckDB-WASM | n/a hoje; expressões Ibis poderiam ser serializadas |
| BigQuery (hipotético) | Ibis BigQuery backend | trivial se alguém quiser republicar lá |

## Consequências

- ✅ **Um vocabulário, três superfícies.** O conceito de "filiais de uma
  raiz" tem uma definição executável no `ficha-py` que ETL e notebook
  importam. Divergência fica impossível por construção.
- ✅ **UX de notebook idiomático.** Analistas em Colab ganham
  `ficha.cnpjs(con).filter(_.uf == "SP").select(...)` em vez de
  copy-paste de SQL.
- ✅ **Type safety nos joins compartilhados.** Coluna mistypada falha
  na construção da expressão, não em runtime no meio da ETL.
- ✅ **Portabilidade real.** Mesma expressão roda contra parquet local
  ou contra parquet remoto via httpfs, sem mudar código.
- ⚠️ **Pacote novo pra manter.** `ficha-py` precisa CI próprio,
  versionamento, publicação no PyPI. Acoplado: `ficha-etl` passa a
  depender de `ficha-py`, então mudanças cross-package precisam
  coordenação de release.
- ⚠️ **Indireção pequena na ETL.** Onde antes tinha `con.execute("CREATE
  TABLE cnpjs AS SELECT ... JOIN ...")` agora tem `ficha_py.cnpjs_view(con)
  .compile().to_sql()` ou similar. ~10% mais linhas, mas o "porquê" do
  join vive num único lugar (o módulo `ficha-py`).
- ⚠️ **Dependência transitiva no Colab.** Notebook precisa
  `pip install ficha-py duckdb` na primeira célula (~30s na primeira
  execução do Colab). Aceitável.
- ❌ **Não resolve a dor da CSV reader da ETL** — que é DuckDB-bruto
  e continua sendo. Esse era o trade-off natural: encoding/max_line_size/
  ignore_errors são knobs que Ibis não abstrai.

## Alternativas consideradas e rejeitadas

- **Compartilhar SQL como arquivos `.sql` lidos por ambos.** Funciona pra
  ETL (já é SQL) mas obriga notebook a manter um wrapper `con.execute(open(...))`
  sem nenhuma type safety nem composabilidade. Reuso textual sem reuso
  semântico.
- **Notebook mantém biblioteca própria de queries SQL (independente da ETL).**
  Garantido drift na primeira mudança de schema. Esse é o cenário que o
  ADR está tentando evitar.
- **SQLAlchemy Core ao invés de Ibis.** SQL-builder de propósito geral,
  ergonomia de notebook é pior (não tem `_.col` shorthand), e o backend
  DuckDB no SQLAlchemy é menos maduro que no Ibis.
- **Polars LazyFrame compartilhado.** Bonito, mas não roda contra parquet
  remoto via httpfs sem pré-download (Polars streams parquets via pyarrow,
  não via DuckDB). Notebook ficaria forçado a baixar 3 GB pra primeira
  query. Mata a UX que motiva o ADR.
- **Não fazer nada agora, fazer quando o notebook chegar.** Custo:
  retrabalho da ETL pra extrair as expressões compartilhadas, e meses
  vivendo com vocabulário divergente entre ETL escrita e notebook
  escrito sem o pacote.

## Próximos passos

1. Mergir o bootstrap PR (#24) — esse ADR fica `Proposed` até o pacote
   nascer.
2. PR separado: `feat(ficha-py): bootstrap package with Ibis tables + 1 helper`.
   Inclui: estrutura do pacote, `connect.py`, `tables.py` mínimo (3 tables),
   1 helper como prova (`socios_de(t, cnpj_base)`), CI próprio, publicação
   no PyPI sob o mesmo namespace.
3. PR separado: `refactor(etl): use ficha-py for joins; keep raw SQL at I/O`.
   Migra `transform.py` pra construir `cnpjs`/`raizes`/`socios` via Ibis,
   mantendo `read_csv` e `COPY TO PARQUET` em SQL bruto.
4. PR separado: `docs(notebooks): scaffold Colab notebooks`. Adiciona
   `notebooks/01-comecando.ipynb` e 2-3 mais, todos com `pip install ficha-py`
   na primeira célula.
5. Promover este ADR pra `Accepted` quando o passo 2 mergir.
