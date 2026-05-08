# FICHA performance audit — 2026-05-08

Scope: end-to-end, anchored on the bootstrap OOM that has blocked
`raizes.parquet`. References point to `main` at `6ca382c4`.

## Executive summary

1. **Phase 3 raizes OOM is structural, not tuning-related.** `LIST(DISTINCT
   est.uf)` over 50M groups in `transform.py:526–527` materializes all
   per-group state in memory; DuckDB's hash-aggregate cannot spill `LIST`
   accumulators. Replacing with a *pre-deduped* two-step aggregation
   (`SELECT DISTINCT cnpj_basico, uf` → `array_agg`) is the highest-leverage
   fix — eliminates the 5.5 GiB ceiling and unblocks bootstrap.
2. **Phase 3 should partition the joins by `cnpj_basico` prefix and merge.**
   With `threads=1` already accepted, splitting into 10 chunks turns one
   30 GB hash join into ten ~3 GB ones; peak temp spill drops ~5×, wall
   time roughly unchanged.
3. **Phase 2 wastes ~15 min on the encoding fallback** for
   estabelecimento (3× full-file reads). Pre-sniff the first 256 KB once
   to pick `latin-1` vs `utf-8+ignore_errors` directly.
4. **Phase 1 still writes 25 GB of intermediate CSVs to disk.** Reading
   the ZIP entry as a stream into DuckDB (`read_csv` over `/dev/stdin`
   via Python pipe, or `zipfile.open()` → temp FIFO) eliminates the
   `extract_dir` cleanup gymnastics in `transform.py:730–735`.
5. **Bootstrap-only: bump to `ubuntu-latest-4-cores` (16 GB / 4 vCPU,
   $0.016/min).** At ~6h end-to-end that's ~$6 per bootstrap. Cheaper
   than another week of OOM debugging; monthly cron stays free-tier.

---

## 1. ETL phase 3 (write_*.parquet)

### 1.1 LIST_DISTINCT cannot spill — `transform.py:526–527` *(critical)*
```sql
LIST(DISTINCT est.uf)                    AS ufs_atuacao,
LIST(DISTINCT est.cnae_fiscal_principal) AS cnaes_principais_distintos
```
DuckDB's hash-aggregate spills *groups* but not *list accumulators*; per-key
`LIST(DISTINCT …)` keeps an in-memory hash-set per group. With 50M
`cnpj_basico` groups × ~3 distinct UFs each ≈ 150M strings retained
simultaneously. Cap at 5.5 GiB matches what we observe.

**Fix:** pre-dedup in a flat aggregation that *does* spill:
```sql
CREATE TEMP TABLE _ufs AS
  SELECT DISTINCT cnpj_basico, uf FROM estabelecimento;
CREATE TEMP TABLE _ufs_agg AS
  SELECT cnpj_basico, list(uf) AS ufs_atuacao FROM _ufs GROUP BY 1;
```
Same for cnaes. Each step is a vanilla GROUP BY DuckDB spills cleanly.
**Estimated gain:** unblocks bootstrap; peak ~2 GiB. **Effort:** trivial.

### 1.2 Single agg pass mixes two memory regimes — `transform.py:519–530`
COUNT/COUNT FILTER spill to ~MB; LIST_DISTINCT does not. Splitting
counts into one TEMP TABLE and lists into another (per 1.1) lets DuckDB
pick a stream-friendly plan for the cheap half. **Gain:** ~30% faster
agg phase. **Effort:** trivial (rolls into 1.1).

### 1.3 Partitioned write + concat — `transform.py:394–498, 501–593`
DuckDB `COPY ... TO` won't append, but you can write
`output_dir/cnpjs/part_{0..9}.parquet` with `WHERE cnpj_basico LIKE '0%'`,
…, `'9%'`, then either:
- (a) leave as a **directory of parquets** — DuckDB-WASM's `read_parquet`
  globs work fine over HTTP if the manifest lists each part; or
- (b) merge with `COPY (SELECT * FROM read_parquet('part_*.parquet')) TO
  cnpjs.parquet` — second pass is sort-free and streams, ~5 min extra.

Each chunk's hash table is ~1/10th the size; threads can safely go back
to 2 inside a chunk. **Gain:** halves peak temp spill, allows `threads=2`
→ phase 3 wall time ~50% lower (60 → 30 min for cnpjs). **Effort:** 1
day. Note: option (a) requires touching `manifest.py` and
`web/src/lib/analytical.ts:33` (`registerFileURL` per part).

### 1.4 Sort-merge vs. hash join
DuckDB only has hash joins; sort-merge isn't a tuning knob here. The
`cnpj_basico` collocation in 1.3 is the practical equivalent.

### 1.5 `:memory:` vs on-disk DuckDB — `transform.py:679–683`
On-disk is correct. `:memory:` would still spill via `temp_directory`
but loses crash-safety on the load phase. Not worth changing.

### 1.6 Drop unused estabelecimento columns before phase 3
For `raizes.parquet` only `cnpj_basico, uf, cnae_fiscal_principal,
situacao_cadastral, identificador_matriz_filial, cnpj_ordem,
data_inicio_atividade, municipio` are read. The other 21 VARCHARs
(logradouro, complemento, etc.) sit in DuckDB's column store consuming
buffer pool. **Fix:** `CREATE TEMP TABLE estabelecimento_slim AS SELECT
<8 cols> FROM estabelecimento` before the raizes write; drop after.
**Gain:** ~30% memory headroom on the heaviest step. **Effort:** trivial.

---

## 2. ETL phase 2 (load)

### 2.1 Encoding fallback re-reads the file — `transform.py:223–266`
`read_csv` with `encoding='latin-1'` does a full pre-flight pass.
For estabelecimento (~15 GB) that's ~90 s wasted before falling to
utf-8 fail (~90 s more) before utf-8+ignore_errors finally loads.

**Fix:** sniff first MB on Python side. Note `bytes.decode('latin-1')`
*never* raises (every byte is valid latin-1), so the test must be
"is this strict utf-8?" with latin-1 as the fallback:
```python
with open(p, 'rb') as f: sample = f.read(1 << 20)
try:
    sample.decode('utf-8')
    enc, ie = 'utf-8', True   # whole-file may still have stray bytes
except UnicodeDecodeError:
    enc, ie = 'latin-1', False
```
Keep `ignore_errors=True` on the utf-8 branch because a 1 MB sample
can't prove the rest of a 15 GB file is clean — RFB occasionally emits
mixed-encoding rows mid-file (per the existing comment at
`transform.py:217–222`). RFB is reliably one-dominant-encoding per
snapshot, so sampling one of N partitioned CSVs is enough to pick the
right branch. **Gain:** saves ~3 min/snapshot. **Effort:** trivial.

### 2.2 `_create_table_from_csvs` loads all partitions in one statement
That's correct (DuckDB reads in parallel). Keep.

---

## 3. ETL phase 1 (fetch + extract)

### 3.1 Eliminate the intermediate CSV — `transform.py:114–124, 154–177`
`zf.extract()` materializes the CSV to disk just so `read_csv` can
re-read it. Two viable patterns:
- **POSIX FIFO:** Python thread does `zf.open(member)` →
  `os.write(fifo_fd)`, DuckDB reads from `/tmp/fifo` via `read_csv`.
  Works on Linux runners only — fine for us.
- **DuckDB `httpfs`+local files extension can't read inside ZIPs**, so
  FIFO is the path. Alternatively, decompress into a *single* CSV in
  the same loop and `unlink` immediately after `_create_table_from_csvs`
  per-kind, instead of after the whole load.

**Gain:** removes the ~25 GB cleanup at `transform.py:730–735` from the
critical path; bootstrap can run on a 50 GB disk (today needs 70 GB).
**Effort:** 1 day; non-trivial because of the encoding fallback in
2.1 wanting a seek-able input. Land 2.1 first.

### 3.2 Parallel extract
Extract is sequential (`transform.py:154`). With phase-1 download
already parallel-4, extract is the long pole at ~2 min. Not worth
parallelizing unless 3.1 lands and changes the cost shape.

---

## 4. Frontend (`web/`)

### 4.1 Search fans out two unindexable predicates — `SearchCNPJ.svelte:80–82`
```sql
WHERE cnpj LIKE ? OR razao_social ILIKE ?
```
With `%` prefix on both, neither hits the bloom filter on `cnpj` (ADR
0008). Every search downloads every row group of `cnpjs.parquet` (~1
GB compressed). On a typed-prefix workflow this is ~1 GB per query.

**Fix:** branch on `stripCNPJ(cnpj).length === 14` → exact match path
(`cnpj = ?`, hits bloom, downloads ~1 row group); else go through
`raizes.parquet` for name search (~150 MB, smaller column set).
**Gain:** 10× lower bytes/query on the dominant path. **Effort:** 1 day.

### 4.2 Detail vs summary parquet split — schema-level
`cnpjs.parquet` carries 40+ cols; the lâmina (`EmpresaFicha.svelte:65–105`)
shows ~10. Split into `cnpjs_summary.parquet` (cnpj, razao, uf, cnae,
capital, nome_fantasia, situacao — used by search list) and keep full
`cnpjs.parquet` for the detail view. Summary is ~5× smaller; first
search is ~5× faster cold cache. **Effort:** 1 day; needs schema doc
update. Defer until 4.1 lands and we measure.

### 4.3 `attachCnpjs` is one-shot — `analytical.ts:32–40`
Already correct: VIEW is created once at mount, parquet metadata is
fetched lazily by DuckDB-WASM and cached per `registerFileURL`. No
change.

---

## 5. Schema / cross-cutting

### 5.1 All-VARCHAR loading — `transform.py:181–184, 49–111`
Loading numeric/date columns as VARCHAR is fine. The tempting "make
`cnpj_basico` a BIGINT to shrink the join key" doesn't work: RFB ships
it zero-padded (`"00123456"`) and `write_cnpjs_parquet` at
`transform.py:408` concatenates `cnpj_basico || cnpj_ordem || cnpj_dv`
to form the 14-char CNPJ. BIGINT round-trip loses the padding and
produces wrong CNPJs; frontend schemas (`web/src/schemas/v1/{raiz,
estabelecimento,socio}.ts`) also expect `z.string()`. Skip this
optimization. (DuckDB's dictionary encoding on the join key already
captures most of the would-be win.)

### 5.2 Bloom filter without sort — `transform.py:485–494`
Bloom filter is per-row-group; effectiveness depends on
*distinct-cardinality-per-row-group*, not global ordering. With
unsorted writes and ROW_GROUP_SIZE=200k, each row group sees ~200k
random CNPJs out of 60M ≈ 0.3% of universe. Bloom rejects ~99.7% of
groups for an exact-cnpj lookup → still hits ~30 row groups out of
~300. **That's a 10× regression vs. sorted (~3 groups).** Worth
re-introducing a *partition-local* sort: with the partitioning in 1.3,
each part is small enough to sort within ~6 GiB. **Gain:** 10×
fewer bytes per exact-cnpj lookup in the frontend. **Effort:** rolls
into 1.3.

---

## 6. Bootstrap workflow runner

ubuntu-latest: 4 vCPU / 16 GB on free public repos as of 2026-01.
Current config caps at 6 GB and threads=1 due to *historical* 7 GB
runner. Verify in `.github/workflows/etl-bootstrap.yml` — if it's
already on `ubuntu-latest`, just bump `memory_limit='12GB'` and
`threads=2`. If still legacy 7 GB, `ubuntu-latest-4-cores` (16 GB,
$0.016/min) at ~6h = ~$6 for the one-shot bootstrap is trivial vs.
engineering cost. Monthly cron stays on free-tier.

**Gain:** unblocks bootstrap *today* without code changes; ~3× wall
time reduction. **Effort:** trivial.

---

## What I'd do first

**Land §1.1 in one PR.** Concretely, in
`etl/src/ficha_etl/transform.py:write_raizes_parquet`:

1. Before `_raizes_agg`, add:
   ```python
   con.execute("""CREATE TEMP TABLE _ufs AS
       SELECT DISTINCT cnpj_basico, uf FROM estabelecimento""")
   con.execute("""CREATE TEMP TABLE _cnaes AS
       SELECT DISTINCT cnpj_basico, cnae_fiscal_principal
       FROM estabelecimento WHERE cnae_fiscal_principal <> ''""")
   con.execute("""CREATE TEMP TABLE _ufs_agg AS
       SELECT cnpj_basico, list(uf) AS ufs_atuacao
       FROM _ufs GROUP BY cnpj_basico""")
   con.execute("""CREATE TEMP TABLE _cnaes_agg AS
       SELECT cnpj_basico, list(cnae_fiscal_principal)
              AS cnaes_principais_distintos
       FROM _cnaes GROUP BY cnpj_basico""")
   ```
2. Replace the LIST(DISTINCT …) lines in `_raizes_agg` (lines 526–527)
   with `JOIN _ufs_agg`/`JOIN _cnaes_agg` on `cnpj_basico`.
3. Drop the four temp tables at the end alongside `_raizes_agg`/`_raizes_matriz`
   (line 592–593).

This is mechanical, ~30 lines, and addresses the documented OOM
without touching workflow files, manifest schema, or the frontend.
Run it on the existing 7 GB runner before considering §6.

---

## Out of scope / deferred

- **Ibis migration (ADR 0017).** Useful refactor, not a perf win.
- **`raizes.parquet` razao_social secondary index.** Deferred until
  §4.1 measurements show name-search is the next bottleneck.
- **Switching parquet compression (ZSTD → SNAPPY) or row group size.**
  ZSTD at 200k is well-tuned; gains are <10%.
- **DuckDB-WASM upgrade tracking.** Out-of-scope; orthogonal.
- **Self-hosted runner.** §6's paid 4-core gets us to "good enough"
  without ops burden.
