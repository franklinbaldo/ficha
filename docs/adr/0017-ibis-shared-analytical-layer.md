# ADR 0017 — Ibis como camada analítica compartilhada (`ficha-py`)

**Status:** Accepted (parcial — ver "Estado da implementação" abaixo)
**Data:** 2026-05-07 (atualizado 2026-07-14)

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
- ⚠️ ~~**Não resolve a dor da CSV reader da ETL** — que é DuckDB-bruto
  e continua sendo. Esse era o trade-off natural: encoding/max_line_size/
  ignore_errors são knobs que Ibis não abstrai.~~ **Corrigido em
  2026-07-18 — ver "Atualização" abaixo:** o `con.read_csv()` do Ibis
  *repassa* esses knobs para o DuckDB. A razão para manter o read raw é
  outra (ausência de vocabulário compartilhado + fallback de encoding
  específico da ETL), não incapacidade do Ibis.

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

## Estado da implementação (2026-07-14)

Feito, com um desvio deliberado do plano original de "passo 3":

- ✅ **Pacote `ficha-py` cobre os 7 parquets + lookups.** `tables.py` tem refs
  para `cnpjs`, `raizes`, `socios`, `enderecos`, `pessoas`, `cnpj_cnaes`,
  `cnpj_contatos` e `lookup(con, kind)` genérico para os 6 lookups.
  `connect_local`/`connect_ia` registram o conjunto completo (fail-fast se
  faltar algum, espelhando o que `ficha_etl.manifest.build_snapshot_entry`
  já exige). Dois helpers em `views.py`: `socios_de` (original) e
  `filiais_de` (novo).
- ✅ **CI próprio.** Job `ficha-py` em `.github/workflows/ci.yml`
  (path-filtered, mesmo padrão do job `etl`).
- ✅ **ETL importa `ficha_py` de verdade** — mas só para `write_lookup_parquets`
  (`transform.py`), não para os joins pesados. `ficha-etl` depende de
  `ficha-py` via path local editável (`[tool.uv.sources]`), não PyPI.
- ✅ **Notebooks scaffold** em `notebooks/` (01-comecando, 02-busca-por-uf,
  03-grafo-de-socios), todos usando a API pública de `ficha_py` contra
  `connect_ia`.
- ❌ **Não publicado no PyPI.** Requer conta/credenciais que este trabalho
  não tinha acesso para provisionar. Fica como próximo passo caso o uso em
  notebooks externos (fora deste monorepo) se torne real.

### Por que os JOINs pesados (`cnpjs`/`raizes`) continuam em SQL bruto

O plano original (passo 3, "migra `transform.py` pra construir `cnpjs`/
`raizes`/`socios` via Ibis") **não foi executado como escrito** — decisão
tomada na integração, não um adiamento por preguiça. Entre a redação deste
ADR (2026-05-07) e sua aceitação, `docs/perf-plan-2026-05.md` documentou que
o join de `cnpjs`/`raizes` tem histórico real de OOM em produção (teto de
5.5 GiB por causa de `LIST(DISTINCT …)` não fazer spill), resolvido por uma
sequência de ajustes muito específicos: `threads=1` como brake deliberado,
chunk-per-ZIP com merge final, materialização de CTEs em `TEMP TABLE` na
ordem exata que evita o OOM (`transform.py:833` em diante). Reescrever esse
caminho via Ibis arriscaria reintroduzir exatamente o bug que várias PRs
levaram para consertar, sem garantia de que o plano gerado pelo compilador
Ibis preserve a mesma forma de execução.

Isso é, na prática, uma extensão do princípio que o próprio ADR já
reconhecia (SQL bruto fica no "I/O pesado" — `read_csv`/`COPY TO PARQUET`):
joins que carregam ajuste fino de memória documentado por incidente real
são tratados com a mesma cautela. `write_lookup_parquets` foi escolhido
como primeiro alvo real de integração exatamente por ser o oposto: sem
joins, sem histórico de OOM, baixo risco — prova que o padrão "ETL importa
`ficha_py.views`" funciona, sem apostar a estabilidade do pipeline mensal
nisso.

Se uma futura migração dos joins pesados for tentada, deve vir com
benchmark de memória comparando o plano gerado pelo Ibis contra o SQL
manual atual, não como troca direta.

## Atualização (2026-07-18)

Três avanços na direção do ADR, com evidência:

### 1. `socios.parquet` migrado para Ibis

`write_socios_parquet` (`transform.py`) agora compila a query via
`_socios_select_sql`, uma expressão Ibis sobre as tabelas brutas `socio` +
`lookup_qualificacoes` + `lookup_paises` — mesmo padrão de
`write_lookup_parquets` (query em Ibis, `COPY ... PARQUET` em SQL bruto).
Equivalência bit-a-bit com o SQL manual anterior verificada em fixtures com
casos de borda (datas vazias/`'0'`, códigos de lookup ausentes, PF/PJ/
estrangeiro); os 25 testes de `test_transform.py` seguem passando.

A expressão é **ETL-local de propósito**: ela lê o schema RFB bruto (`socio`),
que `ficha-py` deliberadamente não conhece — `ficha-py` só fala o shape
*publicado* (`socios`), conforme o comentário de fronteira em `ficha_py.tables`.
Por isso ela vive em `transform.py`, não em `ficha-py`, mesmo sendo Ibis.

### 2. Correção: o read_csv do Ibis **repassa** os knobs do DuckDB

A consequência marcada com ❌ acima ("Ibis não abstrai encoding/max_line_size/
ignore_errors") está **desatualizada** para o Ibis 12. Verificado: o
`con.read_csv(path, delim=…, null_padding=True, ignore_errors=True,
parallel=False, max_line_size=…, columns=…)` do backend DuckDB repassa esses
argumentos para o `read_csv` do DuckDB (inclusive o `parallel=False` que este
mesmo PR precisou para destravar o `cnpjs.parquet`).

Consequência prática: **o read raw da ETL continua raw, mas por outro motivo** —
não por incapacidade do Ibis, e sim porque (a) o loop de fallback de encoding
(`_create_table_from_csvs`) é I/O específico da ingestão RFB, sem vocabulário a
compartilhar com notebooks (que leem parquets prontos, nunca CSV bruto), e (b)
roteá-lo pelo Ibis seria churn de risco sobre um caminho load-bearing sem ganho.
A capacidade existe; o *value case* para migrar o read não.

### 3. Benchmark de memória do join `cnpjs` (o que o ADR pedia)

Rodado o benchmark que este ADR exige antes de migrar os joins pesados —
[`docs/ibis-cnpjs-benchmark-2026-07-18.md`](../ibis-cnpjs-benchmark-2026-07-18.md).
Resultado (6M estab, `threads=1`, sob teto de 1 GB para forçar spill): Ibis ≈
0.90× do tempo e 1.10× do spill do SQL manual, com saída idêntica. **Não há
penhasco de memória** — o medo hipotético registrado neste ADR não se
confirmou para o `cnpjs`.

Guidance atualizada:
- **`cnpjs`:** migração viável e de baixo risco pela evidência; confirmar com o
  harness em CI na escala de produção (`FICHA_BENCH_ESTAB_ROWS=70000000`) antes
  de trocar o código de produção.
- **`raizes`:** **continua sem migrar.** O OOM real de produção veio da agregação
  `LIST(DISTINCT)` do raizes, que **não** foi medida aqui. Precisa do seu próprio
  benchmark antes de qualquer troca — segue sendo o alvo arriscado.
