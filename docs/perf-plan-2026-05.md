# FICHA performance & shape plan — 2026-05-08

Scope: a sequenced plan to (a) unblock the bootstrap OOM, (b) reshape
phase 3 for sustainable monthly runs, and (c) add new analytical
parquets that the architecture has been pointing toward.

References point to `main` at `6ca382c4`. Originally framed as an
audit; reframed as a plan because most findings translate directly
to deliverables and the dependency ordering matters.

## Status of the bootstrap OOM (read first)

The bootstrap workflow has **not completed end-to-end on `main`**.
PR #24 already attempted to fix the 5.5 GiB OOM by splitting
`LIST_DISTINCT` out of the JOIN into a temp table — see the
existing in-tree commentary at `transform.py:512–517`. That refactor
ships, but bootstrap has not been re-run to confirm whether it's
sufficient on its own.

W1.1 below proposes a *further* change (pre-dedup before
`list()`) on the hypothesis that the temp-table split alone is
insufficient because `LIST(DISTINCT)`'s per-group state still
doesn't spill regardless of which CTE/temp table it lives in. **Run
bootstrap on current `main` first** — if PR #24's split actually
holds, W1.1 becomes optional polish rather than a blocker. If it
still OOMs at 5.5 GiB on the same operator, W1.1 is required.
This document assumes the latter; verify before committing
implementation effort.

## Plan summary

| Milestone | Goal | Workstreams | Depends on |
|-----------|------|-------------|------------|
| **Phase 1** | Diagnose (no code) | runner check, simples cardinality, bootstrap re-run, FIFO probe | — |
| **M0** | Unblock bootstrap | W1.1, W1.6, W6 | Phase 1 |
| **M1** | Phase-3 reshape (sustainable monthly) | W1.3, W2.1, W3.1, W5.2 | M0 |
| **M2** | Frontend query perf | W4.1, W4.2 | M0 |
| **M3** | Correctness sweep | W9.3 (CNAE secundário), W13.1a (simples cardinality / LEFT JOIN bug) | Phase 1 (step 2 result) |
| **M4** | New analytical parquets | W10, W11, W12, W7, W8 | M0 |
| **M5** | Deferred — temporal & graph | W13.1b (cross-snapshot), W13.2 (graph) | M4 |

W-numbers map 1:1 to the prior §-numbering for traceability;
section headers retain both. Sequence below presents milestones in
priority order, not reading order.

## M0 — Unblock bootstrap

Top three findings that, together, get bootstrap to first complete
end-to-end run:

- **W1.1** — pre-dedup before `list()` (replaces `LIST(DISTINCT)`).
  Headline finding; details in §1.1. Implementation sketch in "What
  I'd do first" near the end of this doc.
- **W1.6** — `estabelecimento_slim` projection before phase 3
  (~30% memory headroom). Details in §1.6.
- **W6** — verify the runner tier. As of 2026-01 GitHub bumped
  free-tier `ubuntu-latest` to 4 vCPU / 16 GB; if
  `etl-bootstrap.yml` is still capping at 6 GB / threads=1 from the
  legacy 7 GB era, just bumping the PRAGMAs may suffice without
  paid runners. Take 30 seconds to check before designing around
  the old constraint. Details in §6.

**Acceptance:** bootstrap produces `cnpjs.parquet`, `raizes.parquet`,
`socios.parquet`, and `lookups.json` end-to-end on a single GitHub
Actions run. Runtime budget: < 350 min (the workflow ceiling).

## M1 — Phase-3 reshape

Once a baseline run exists, reshape for sustainable monthly cron
behavior:

- **W1.3** partition by `cnpj_basico` prefix → §1.3
- **W2.1** encoding sniff → §2.1
- **W3.1** stream-from-ZIP, eliminate intermediate CSV → §3.1
- **W5.2** restore partition-local sort + bloom efficacy → §5.2

**Acceptance:** monthly run finishes in < 4 h with peak temp spill
< 30 GB (vs. current 70 GB exhaustion).

## M2 — Frontend query perf

- **W4.1** length-14 cnpj branch (skip the `LIKE '%…%'` that
  defeats the bloom) → §4.1
- **W4.2** summary/detail parquet split → §4.2

**Acceptance:** typical search downloads < 100 MB on cold cache.

## M3 — Correctness sweep (independent of bootstrap)

Two findings that aren't perf but are correctness, surfaced by the
audit:

- **W13.1a — simples cardinality** *(silent LEFT JOIN bug if
  multi-row per cnpj_basico)*. Run the diagnostic query on a
  partial load:
  ```sql
  SELECT cnpj_basico, COUNT(*) FROM simples
  GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 10
  ```
  If non-empty, `write_cnpjs_parquet` at line 478 is silently
  multiplying estabelecimento rows. **The roundtrip-equivalence
  check at `transform.py:857–862` would catch this on a completed
  run** — but bootstrap hasn't completed, so the bug is unobserved
  rather than disproved. This is independent of the OOM; can
  run on any successful partial load.
- **W9.3 — populate `cnae_secundario_descricoes`** to match the
  denormalization pattern of every other lookup. Closes the TODO
  at `transform.py:441–442`. See §9 for the order-preserving
  query shape.

## M4 — New analytical parquets

These are *new feature designs*, not perf fixes — kept in this doc
because the audit surfaced them and the architectural pattern is
consistent (additive specialized parquets, ADR 0008 lineage). Each
deserves its own ADR before implementation:

- **W10** — per-lookup parquets (§10)
- **W11** — `cnpj_cnaes.parquet` position-aware association (§11)
- **W12** — `cnpj_contatos.parquet` reverse contact lookup (§12)
- **W7** — `enderecos.parquet` reverse address & município (§7)
- **W8** — `pessoas.parquet` person reverse-lookup (§8)

Sequencing within M4: W10 first (smallest, validates the lookup
attach pattern in the frontend), then W11/W12 (built from
`estabelecimento` only, no joins), then W7/W8 (slightly larger,
require normalization decisions).

**Don't start M4 until M0 lands.** Multiplying parquet outputs
while phase 3 is OOM-prone just multiplies the failure surface.

## M5 — Deferred

- **W13.1b** — cross-snapshot `simples_history.parquet` (§13.1).
  Requires a new ETL stage outside the monthly cron shape.
- **W13.2** — graph queries via self-join on `pessoas.parquet`
  (§13.2). No materialization needed initially; only revisit if
  recursive 2-hop traversal proves too slow in DuckDB-WASM.

## Implementation phases

The milestones above describe *what* to do; the phases below describe
*how to ship it* — sequenced into reviewable PRs with explicit
acceptance gates. Each phase produces a measurable outcome before
the next starts. Phases 1–5 are critical path; Phases 6–9 are
additive features that can run in parallel once Phase 4 lands.

### Phase 1 — Diagnose (1 day, no code changes)

Empirical questions whose answers reshape later phases. Don't write
code yet:

1. **Re-run bootstrap on current `main`** to see whether PR #24's
   temp-table split alone is sufficient. If it completes, W1.1
   becomes optional and Phase 2 collapses to `n/a`.
2. **Run the simples cardinality query** on a partial load:
   ```sql
   SELECT cnpj_basico, COUNT(*) FROM simples
   GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 10
   ```
   Result decides whether Phase 3 needs the silent-LEFT-JOIN fix.
3. **Check `etl-bootstrap.yml` runner tier.** If
   `runs-on: ubuntu-latest` already inherits the post-2026-01 4 vCPU
   / 16 GB tier, bumping `memory_limit='12GB'` is safe; bumping
   `threads` is *not* (see Phase 2 PR 2a — `threads=1` is currently
   load-bearing for LIST(DISTINCT) memory headroom per the in-tree
   comment at `transform.py:703–704`, not purely a legacy-runner
   artifact).
4. **FIFO probe** for W3.1 / Phase 4d: a 5-line script that opens
   `mkfifo /tmp/p`, writes a 100 MB CSV through it from one Python
   thread, and runs `con.execute("SELECT count(*) FROM
   read_csv('/tmp/p', …)")` from another. Confirms DuckDB's CSV
   reader doesn't try to seek the FIFO. Runs locally; doesn't need
   the runner.

**Gate:** documented findings in a follow-up issue or PR description.
**Risk:** none — observational.

### Phase 2 — Unblock bootstrap (M0)

Only run if Phase 1 confirms bootstrap still OOMs. One PR per item
to keep blast radius small:

- **PR 2a:** W6 — bump only `memory_limit` PRAGMA in
  `transform.py:690` if Phase 1 step 3 says runner has more RAM.
  **Do not bump `threads` here.** The in-tree comment at
  `transform.py:703–704` makes `threads=1` load-bearing for
  LIST(DISTINCT) memory headroom — bumping it before PR 2c lands
  re-triggers the OOM W6 hopes to sidestep. *Trivial; uncontroversial.*
- **PR 2b:** W1.6 — `estabelecimento_slim` projection before
  `write_raizes_parquet`. *Trivial; uncontroversial.*
- **PR 2c:** W1.1 — pre-dedup before `list()`. *The
  "What I'd do first" PR sketch at the bottom of this doc.* Once
  this lands, a follow-up PR 2d can experimentally restore
  `threads=2` if the new aggregation is genuinely spillable.

**Gate:** bootstrap produces all three parquets + `lookups.json`
end-to-end on a single workflow run within the 350-min ceiling.
**Risk:** medium on PR 2c (changes data shape of an in-flight
parquet); roundtrip-equivalence (`transform.py:842–902`) must pass.

### Phase 3 — Correctness sweep (M3, parallelizable with Phase 2)

Independent of OOM; can be in parallel review with Phase 2:

- **PR 3a:** W13.1a — if Phase 1 step 2 surfaced multi-row simples,
  fix `write_cnpjs_parquet`'s LEFT JOIN at `transform.py:478` to
  collapse via window function or subquery before the join.
  *Schema unchanged; correctness only.*
- **PR 3b:** W9.3 — populate `cnae_secundario_descricoes` per the
  order-preserving query in §9.3. Bumps `schema_version` and
  cascades to `web/src/schemas/v1/estabelecimento.ts`.

**Gate:** roundtrip-equivalence passes; frontend renders secondary
CNAE descriptions without client-side lookup.

### Phase 4 — Phase-3 reshape (M1)

The biggest sustained-perf wins. Order chosen to ship low-risk PRs
first and validate the partitioning approach last:

- **PR 4a:** W2.1 — encoding sniff on first 1 MB. *Low risk;
  removes ~3 min of phase-2 wall time.*
- **PR 4b:** W5.2 — partition-local sort restoration **only after
  4c lands** (sort-within-partition needs partitions).
- **PR 4c:** W1.3 — partition write by `cnpj_basico` prefix +
  manifest schema update. *Highest risk in phase 4: changes
  manifest shape; frontend must learn to glob row-group parts. Land
  behind a feature flag (`FICHA_PARTITIONED_WRITE=1`) so monthly
  cron can opt in after one successful run.*
- **PR 4d:** W3.1 — stream-from-ZIP (FIFO). *Eliminates the 25 GB
  cleanup; gated on Phase 1 step 4's FIFO probe result.*

**Gate:** monthly run < 4 h, peak temp spill < 30 GB, manifest
schema migration tested.

### Phase 5 — Frontend perf (M2)

After Phase 4 stabilizes the parquet shape:

- **PR 5a:** W4.1 — branch search by `length(cleanCNPJ) === 14` in
  `web/src/components/SearchCNPJ.svelte:60–93`. *Trivial; biggest
  cold-cache win.*
- **PR 5b:** W4.2 — split `cnpjs_summary.parquet` from full
  `cnpjs.parquet`. *Schema bump; coordinate with PR 4c if both
  land in the same release.*

**Gate:** Lighthouse / manual: cold-cache CNPJ search downloads
< 100 MB.

### Phase 6 — Per-lookup parquets (M4 entry point)

Smallest of the new-parquet workstreams; validates the
`attachLookups` frontend pattern that later phases reuse.

- **PR 6:** W10 — emit one parquet per lookup table; add
  `attachLookups()` to `web/src/lib/analytical.ts`. ADR for
  the dual JSON+parquet shape.

**Gate:** demo query "filter by município description prefix" works
in DuckDB-WASM with `JOIN lookup_municipios`.

### Phase 7 — Position-aware association parquets

Built only from `estabelecimento` (no joins, low memory pressure;
safe to add to phase 3):

- **PR 7a:** W11 — `cnpj_cnaes.parquet`. ADR.
- **PR 7b:** W12 — `cnpj_contatos.parquet`. ADR (privacy posture).

Can ship in parallel; same review reviewer.

**Gate:** reverse-CNAE and reverse-phone demo queries work.

### Phase 8 — Reverse-lookup parquets

Largest of the additive parquets; require normalization decisions
captured in their own ADRs:

- **PR 8a:** W7 — `enderecos.parquet` + abbreviation expansion.
  ADR documents which abbreviations are expanded and why no fuzzy
  dedup.
- **PR 8b:** W8 — `pessoas.parquet` + composite PK
  `(nome_normalizado, cpf_mascarado)`. ADR documents PII posture
  and the masked-CPF collision rate.

**Gate:** "CNPJs at this address" and "companies where this person
appears" demo queries work in DuckDB-WASM.

### Parallel work tracks

The phases above are sequential at the *milestone* level (M0 gates
M1/M2/M4) but most individual PRs within and across phases are
independent. Concrete parallelism opportunities, ordered by when in
the timeline they unlock:

#### Track A — Phase 1 (all 4 steps in parallel)
All four diagnostic steps are independent and can run on the same
afternoon:
- 1.1 bootstrap re-run (~6h, mostly idle GitHub Actions wait)
- 1.2 simples cardinality query (minutes, on a partial load)
- 1.3 runner tier check (30 seconds, just open
  `etl-bootstrap.yml`)
- 1.4 FIFO probe (~10 min local script)

Steps 1.2 and 1.3 inform PRs that can ship *while 1.1 is still
running*.

#### Track B — Phase 2 + Phase 3 in parallel (after Phase 1)
Once Phase 1's diagnostic results are in, four PRs can be in flight
simultaneously across two reviewer queues:

| PR | Workstream | Independent of |
|----|------------|----------------|
| 2a | `memory_limit` bump (W6) | 2b, 2c, 3a, 3b |
| 2b | `estabelecimento_slim` (W1.6) | 2a, 2c, 3a, 3b |
| 2c | pre-dedup (W1.1) | 2a, 2b, 3a, 3b |
| 3a | simples LEFT JOIN fix (W13.1a, *if Phase 1 step 2 surfaced it*) | all |
| 3b | populate `cnae_secundario_descricoes` (W9.3) | all |

The only sequential constraint inside this band is **PR 2d
(`threads=2`) waits on PR 2c** — but 2d isn't in M0; it's a
follow-up benchmark after W1.1 stabilizes.

#### Track C — Phase 4 fan-out (after M0)
Phase 4's four PRs split cleanly:

| PR | Parallel with | Sequential constraint |
|----|---------------|----------------------|
| 4a | encoding sniff | independent of everything in Phase 4 |
| 4c | partition write | independent of 4a, 4d |
| 4d | FIFO streaming | independent of 4a, 4c (different code path) |
| 4b | partition-local sort | **must wait on 4c** (sort needs partitions to live within) |

So after M0, you can land **4a + 4c + 4d in parallel**, then 4b. PR
4a (encoding sniff) is also safe to ship *before* M0 lands —
nothing about it touches phase 3 — moving it earlier reclaims ~3
min of phase-2 wall time during Phase 1's bootstrap re-run.

#### Track D — Phase 5 (after M0)
- 5a (length-14 branch) and 5b (summary parquet) are independent of
  each other; both gated only on `cnpjs.parquet` existing (i.e.
  M0). They can ship in parallel reviewer queues alongside Phase 4.
- 5b's schema bump should coordinate with PR 4c if both land in the
  same release — the manifest entry update is in the same file.

#### Track E — Phases 6/7/8 fan-out (after M0)
Once M0 lands, the analytical-parquet additions are mostly
independent of each other and of Phase 4/5:

| Phase | PRs that can be concurrent | Notes |
|-------|---------------------------|-------|
| 6 | PR 6 (W10 lookups) | smallest; ship first to validate `attachLookups` pattern |
| 7 | PR 7a + 7b (cnpj_cnaes + cnpj_contatos) | both built only from `estabelecimento`, no joins; trivially parallel |
| 8 | PR 8a + 8b (enderecos + pessoas) | both larger; each needs its own ADR; trivially parallel |

Each phase's PRs share a frontend `attachX` pattern; reuse the
review pattern established by PR 6 to compress later review cycles.

#### Maximum concurrent PR count
Realistic upper bound: **~6 PRs in flight** during the post-M0
period (4a/4c/4d, 5a, 6, 3b for example), assuming reviewer
bandwidth and a single integration branch. The dependency graph
admits more in theory; the constraint is human review throughput,
not topology.

#### Things that *must* be sequential
- 4c → 4b (sort needs partitions)
- 2c → 2d (threads bump needs spillable aggregation)
- M0 → M4 entry (don't multiply outputs while phase 3 is broken)
- 5b ↔ 4c (manifest schema coordination if same release)

Everything else is parallelizable.

---

### Phase 9 — Deferred (M5)

Open-ended; do not start without explicit ADR:

- W13.1b — cross-snapshot `simples_history.parquet`. Requires a
  new ETL workflow shape distinct from the monthly cron.
- W13.2 — graph traversal beyond 1-hop self-join. Only if
  measurement on `pessoas.parquet` shows recursive CTEs are too
  slow in DuckDB-WASM.

---

The numbered sections below are the original technical findings
referenced by the milestones above. Headers preserve both the
original § number and the M-milestone they belong to.

## Executive summary (workstream highlights)

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

### 4.1 Search fans out two unindexable predicates — `web/src/components/SearchCNPJ.svelte:80–82`
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

## 6. Bootstrap workflow runner *(verify via Phase 1 step 3)*

GitHub bumped free-tier `ubuntu-latest` to 4 vCPU / 16 GB for public
repos in 2026-01. Current config in `transform.py:690–705` caps at
6 GB / threads=1, originally tuned for the legacy 7 GB runner.

**Important:** `threads=1` is *not* purely a legacy artifact — the
in-tree comment at `transform.py:703–704` documents that it also
brakes LIST(DISTINCT) memory growth. So the workstream is:

1. Phase 1 step 3 confirms which runner tier the workflow inherits.
2. If 16 GB: PR 2a bumps **only** `memory_limit` (e.g. to `12GB`).
   Leave `threads=1` until W1.1 makes the aggregation spillable.
3. If still 7 GB: option of moving to `ubuntu-latest-4-cores`
   (16 GB, ~$0.016/min ≈ $6 for one-shot bootstrap) for cost-vs-
   engineering trivial. Monthly cron stays free-tier.
4. After W1.1 lands (Phase 2 PR 2c), follow-up PR 2d may try
   `threads=2` and benchmark.

**Gain:** unblocks bootstrap without code changes if step 2 alone
suffices; ~3× wall time reduction in the best case. **Effort:**
trivial.

---

## 7. Reverse lookup: CNPJs by address / município

Currently `cnpjs.parquet` is unsorted (per §5.2), with bloom only on
`cnpj`. Queries like "who's at Av. Paulista 1000?" or "all CNPJs in
município 7107" must full-scan ~1 GB. New use cases on the roadmap →
add a fourth specialized parquet, parallel to `raizes.parquet`.

### 7.1 `enderecos.parquet` — minimal viable shape
Columns: `uf, municipio_codigo, logradouro_normalizado, numero, cep,
bairro, cnpj`. Sort by `(uf, municipio_codigo,
logradouro_normalizado, numero)`. Bloom on
`logradouro_normalizado` and `municipio_codigo`.

This single layout serves three patterns:
- **Município lookup** (`WHERE uf=? AND municipio_codigo=?`) — sort
  prefix gives row-group min/max pruning; bloom on
  `municipio_codigo` rejects 99%+ of groups. ~5–50 MB downloaded
  depending on município size (São Paulo capital ≈ 5M rows is the
  worst case).
- **Address lookup** (`WHERE uf=? AND municipio_codigo=? AND
  logradouro_normalizado=? AND numero=?`) — same prefix, then
  bloom + range on logradouro lands a single row group. <1 MB.
- **Street prefix / typo-tolerant search** — within a (uf, município)
  range, `logradouro_normalizado LIKE 'PAULISTA%'` is a sequential
  scan over a few MB.

**Effort:** 2 days. New `write_enderecos_parquet` in
`transform.py`, manifest entry, frontend `attachEnderecos` mirroring
`analytical.ts:32–40`.

### 7.2 Address normalization — pragmatic, not perfect
RFB addresses are dirty (`R.`, `RUA`, `R`, accents, trailing
whitespace). Aim for *recall*, not canonical dedup:

```sql
UPPER(strip_accents(
  regexp_replace(
    regexp_replace(logradouro, '\s+', ' ', 'g'),
    '^(R|AV|TV|AL|PCA|PC|EST|ROD)\.?\s', <expansion>, 'i'
  )
)) AS logradouro_normalizado
```

Top ~10 abbreviations cover ≥90% of variation. Same-street-different-
spellings ("R DAS FLORES" vs "RUA DAS FLORES") collapse cleanly;
genuinely different streets stay distinct. **Don't** attempt fuzzy
dedup (Levenshtein/phonetic) in v1 — it's a separate project and
the parquet is small enough that frontend-side fuzzy match (e.g.
trigram on the loaded row group) is viable later.

### 7.3 Município as a separate parquet?
Not needed if §7.1 ships — `enderecos.parquet` already serves
município queries via the sort prefix. A standalone
`municipios.parquet` (just `cnpj, municipio_codigo, uf, situacao`)
would be ~6× smaller (~150 MB vs ~1 GB) and faster for
município-only queries that don't care about the address columns.
Worth it only if usage data shows município-list queries dominate.
Defer.

### 7.4 Schema cost
Adding `enderecos.parquet` adds one phase 3 write. Built from
`estabelecimento` alone (no joins), it's the cheapest of the four —
~10 min wall, ~2 GB peak memory. No interaction with the §1.1 OOM
fix.

---

## 8. Reverse lookup: companies by person (`pessoas.parquet`)

Currently `socios.parquet` is sorted/bloomed by `cnpj_base`, so
"sócios of company X" is fast but the inverse — "companies where
person X appears" — is a full scan. Two columns in
`_SOCIO_COLUMNS` (`transform.py:90–102`) carry person identity:

- `cnpj_cpf_socio` (masked CPF when `identificador_socio='2'`,
  full CNPJ when `'1'`)
- `representante_legal` (CPF of the legal rep — present *also* when
  the sócio itself is a PJ, since every PJ-sócio has a human rep)

So every `socio` row contributes 0–2 person identities. Externalize
both roles into one parquet:

### 8.1 Shape
Columns: `cpf_mascarado, nome_normalizado, nome_original, papel
(socio_pf|representante), cnpj_base, qualificacao_codigo,
data_entrada_sociedade, faixa_etaria`. Built by `UNION ALL` of:

```sql
-- sócios PF
SELECT cnpj_cpf_socio AS cpf_mascarado,
       UPPER(strip_accents(nome_socio_razao_social)) AS nome_normalizado,
       nome_socio_razao_social AS nome_original,
       'socio_pf' AS papel,
       cnpj_basico AS cnpj_base, qualificacao_socio,
       data_entrada_sociedade, faixa_etaria
FROM socio WHERE identificador_socio = '2'
UNION ALL
-- representantes legais (whether sócio é PF, PJ ou estrangeiro)
SELECT representante_legal,
       UPPER(strip_accents(nome_representante_legal)),
       nome_representante_legal,
       'representante',
       cnpj_basico, qualificacao_representante_legal,
       NULL, NULL
FROM socio
WHERE representante_legal IS NOT NULL AND representante_legal <> ''
```

Sort by `(cpf_mascarado, nome_normalizado)`. Bloom on both columns.

### 8.2 Identity: `(nome_normalizado, cpf_mascarado)` as composite PK
RFB exposes only the middle 6 digits of CPF (e.g. `***.123.456-**`)
— ~1M values across ~200M Brazilian CPFs, so masked-CPF *alone*
collides ~200×. Full name *alone* collides massively too (many
"JOSÉ DA SILVA"s). But the **pair** is essentially unique: two
distinct people sharing both an identical normalized name AND the
same middle-6 CPF digits is astronomically rare (back-of-envelope:
< 1 in 10⁶ for common names, far less for distinctive ones).

Treat `(nome_normalizado, cpf_mascarado)` as the composite primary
key of `pessoas.parquet`:

- Group by it to compute "this person appears in N companies."
- Use it as the URL slug for person-detail pages
  (`/pessoa/<cpf_mascarado>/<nome_slug>`).
- Sort the parquet by `(cpf_mascarado, nome_normalizado)` — keeps
  per-person rows contiguous, so a single row group serves the
  whole person.
- Bloom on `cpf_mascarado` for cheap "does this person exist" probe;
  bloom on `nome_normalizado` for name-search.

Document in the schema file (`web/src/schemas/v1/pessoa.ts`) that the
PK is composite and the residual false-positive rate is "two namesakes
sharing the same masked CPF" — small enough to surface counts honestly
("aparece em 7 empresas") without weaselly hedging.

### 8.3 Why union vs. two parquets
Two parquets (`socios_pf.parquet` + `representantes.parquet`) would
be fine, but a single `pessoas.parquet` lets one query catch both
roles ("everywhere this person appears"), which is the actual
transparency use case (catching the pattern where a person
represents company A and is sócio of company B). One bloom check,
not two.

### 8.4 Effort
~1 day. Pure read from already-loaded `socio` table; no joins,
~30M-row output, sorted output fits in 6 GiB cap because input is
small relative to estabelecimento. New
`write_pessoas_parquet` + manifest entry + frontend
`attachPessoas` mirroring `analytical.ts:32–40`.

### 8.5 What stays in `socios.parquet`
Don't deprecate it — `socios.parquet` keeps the cnpj_base→sócios
direction (forward lookup, denormalized with PJ sócios + país lookup
joined). `pessoas.parquet` is the inverse index; redundant but
cheap and serves a genuinely different access pattern, same way
`raizes.parquet` and `cnpjs.parquet` coexist (ADR 0008).

### 8.6 PJ-as-sócio and estrangeiro
`identificador_socio` has three values: `'1'` PJ, `'2'` PF, `'3'`
estrangeiro. `pessoas.parquet` deliberately excludes PJ (they're
companies, not persons). The parallel reverse query — "company X is
sócia of which other companies?" — doesn't need a new parquet:
**add a bloom filter on `cnpj_socio` to the existing
`socios.parquet`** at write time (`transform.py:606–643`). Same
file, one extra bloom column, lookup-by-PJ-sócio becomes free.
Estrangeiros (`'3'`) carry no CPF/CNPJ, only name + país; they
live in `socios.parquet` via the existing `nome_socio_razao_social`
column. A bloom on that column would enable reverse-lookup by
foreign-investor name, but cardinality is high and the use case
narrow — defer.

---

## 9. Resolve the `cnae_secundario_descricoes` TODO

`transform.py:441–442` carries an explicit TODO:

```python
-- TODO: descricoes resolvidas no client via lookups.json (v0.1)
[]::VARCHAR[] AS cnae_secundario_descricoes,
```

The column ships as an always-empty array of the wrong width — every
row pays parquet metadata + an empty list marker for nothing. Two
ways to close it; pick one:

### 9.1 Option A — drop the column (recommended)
Aligns with the comment's stated intent. Frontend already loads
`lookups.json`; resolving secondary CNAEs is one `Map.get()` call per
code. **Effort:** trivial — delete lines 437–442, bump
`schema_version` in `web/src/schemas/v1/estabelecimento.ts`, update
the renderer in `EmpresaFicha.svelte` to do `cnaes` lookup
client-side. **Gain:** removes a junk column; ~0 perf, +1 schema
hygiene.

### 9.2 Option B — actually populate it server-side
JOIN against `lookup_cnaes` per secondary code:
```sql
list_transform(
  list_transform(str_split(est.cnae_fiscal_secundaria, ','), x -> trim(x)),
  c -> COALESCE((SELECT descricao FROM lookup_cnaes WHERE codigo = c), '')
)
```
Self-contained rows, no client-side resolution. **Effort:** trivial
SQL, but the correlated subquery in a list_transform is expensive at
60M rows — would need a different shape (e.g. unnest → join → re-agg)
to stay cheap. **Gain:** none beyond consistency with how
`pais_nome`/`municipio_nome` are handled today.

### 9.3 Pick B — match the existing denormalization pattern
Every other lookup (`naturezas`, `qualificacoes`, `motivos`,
`municipios`, `paises`, *primary* `cnae`) is denormalized at write
time. Parquet's per-row-group dictionary encoding makes the
redundancy nearly free (~1 KB dict per row group + 1–2 bytes per
row for a small lookup). Forcing every query to JOIN to recover
the description is a worse trade than carrying the denormalized
column. So: populate `cnae_secundario_descricoes` server-side per
§9.2, using a shape that scales to 60M rows:

```sql
-- pre-explode → join → re-aggregate (avoids correlated subquery)
WITH expanded AS (
  SELECT cnpj_basico, cnpj_ordem, cnpj_dv,
         trim(s.value) AS cnae_codigo
  FROM estabelecimento, unnest(str_split(cnae_fiscal_secundaria, ',')) s
  WHERE cnae_fiscal_secundaria IS NOT NULL AND cnae_fiscal_secundaria <> ''
)
SELECT cnpj_basico, cnpj_ordem, cnpj_dv,
       list(cnae_codigo) AS cnae_secundario_codigos,
       list(COALESCE(c.descricao, '')) AS cnae_secundario_descricoes
FROM expanded LEFT JOIN lookup_cnaes c ON c.codigo = cnae_codigo
GROUP BY 1,2,3
```

Then LEFT JOIN this aggregate into the main `write_cnpjs_parquet`
SELECT, replacing lines 437–442. **Effort:** trivial. **Gain:**
closes the TODO; rows stay self-contained; no client-side lookup
machinery needed.

---

## 10. Per-lookup parquets for SQL composability

Today lookups ship as a single `lookups.json` (`transform.py:333–352`)
loaded synchronously by the frontend. That works for *render glue*
(codigo→descricao for already-fetched rows) but blocks SQL queries
that need to filter or aggregate *by description*: today the frontend
would have to do a JS-side name→codigo translation before issuing
the DuckDB query, and that machinery isn't built.

### 10.1 Shape
Emit one parquet per lookup, alongside the JSON:

```
output_dir/
├── cnpjs.parquet
├── raizes.parquet
├── socios.parquet
├── enderecos.parquet           # §7
├── pessoas.parquet             # §8
├── lookups.json                # keep for synchronous render
└── lookups/
    ├── cnaes.parquet
    ├── motivos.parquet
    ├── municipios.parquet
    ├── naturezas.parquet
    ├── paises.parquet
    └── qualificacoes.parquet
```

Each is `(codigo VARCHAR, descricao VARCHAR)`, sorted by `codigo`,
bloom on `codigo`, bloom on
`descricao_normalizada = UPPER(strip_accents(descricao))` for prefix
search. Largest (`municipios`, ~5500 rows) is < 100 KB; whole bundle
< 500 KB. Single row group each → effectively memory-resident on
first read.

### 10.2 Frontend pattern
Mount-time: `attachLookups(db)` registers all six in parallel,
creates `VIEW lookup_<kind>` per file (mirrors
`analytical.ts:32–40`). Queries that previously required JS
translation become single round-trips:

```sql
-- "companies in cities matching 'BRAS'"
SELECT c.cnpj, c.razao_social, m.descricao AS municipio
FROM cnpjs c
JOIN lookup_municipios m ON m.codigo = c.municipio_codigo
WHERE m.descricao_normalizada LIKE 'BRAS%'
LIMIT 50
```

### 10.3 Don't drop `lookups.json`
DuckDB-WASM cold-start is ~hundreds of ms. Search-result rows that
display `municipio_nome`/`pais_nome` need a synchronous JS map
*before* DuckDB is ready. `lookups.json` (still tiny, ~300 KB) keeps
that path instant. The two serve different layers: JSON for render
glue, parquet for SQL composition. Cost of duplication is < 1 MB
total; not worth optimizing away.

### 10.4 Don't use §10 to justify dropping `*_descricao` columns
The earlier draft of this audit suggested per-lookup parquets would
let us purge denormalized description columns from the big parquets.
Walked back: parquet dictionary encoding already deduplicates those
descriptions to near-zero cost, and self-contained rows beat JOIN-
on-every-query at the frontend layer. §10 stands on its own merit
(filter-by-description queries) — it's *additive*, not a stepping
stone to denormalization removal. §9.3 now recommends populating
the secondary-CNAE descriptions to *match* the denormalization
pattern, not undo it.

### 10.5 Effort
Trivial — one helper that loops `_LOOKUP_KINDS` and emits a parquet
per `lookup_<kind>` table already in DuckDB. ~20 lines in
`transform.py`. New `attachLookups` in `web/src/lib/analytical.ts`
~30 lines. Manifest entry under a `lookups:` map. Schema docs
need a sentence saying "lookups available both as JSON and as
parquet."

---

## 11. CNPJ↔CNAE association parquet (`cnpj_cnaes.parquet`)

Storing secondary CNAEs as an array in `cnpjs.parquet` preserves
order (§9.3 fix) but makes position-aware queries expensive — every
"CNPJs where X is the *primary*" or "rank-2 CNAEs of restaurants"
needs `unnest` + filter over 60M rows. Externalize the
many-to-many into its own parquet, same architectural pattern as
§7 (enderecos) and §8 (pessoas).

### 11.1 Shape
Columns: `cnpj, cnpj_base, cnae_codigo, posicao`.
- `posicao = 0` → primary CNAE (`cnae_fiscal_principal`)
- `posicao = 1, 2, …` → secondary in registration order from
  `cnae_fiscal_secundaria`

Sort by `(cnae_codigo, posicao, cnpj_base)`. Bloom on `cnae_codigo`
and `cnpj_base`. "Principal-only" queries use `WHERE posicao = 0` —
row-group min/max stats already prune efficiently because all
posicao=0 rows for each CNAE are contiguous; no separate
`is_principal` column needed (a bloom on a boolean is meaningless).
~60M estabelecimentos × avg ~3 CNAEs ≈ 180M rows; ~500 MB
compressed after dict encoding.

### 11.2 Use cases unlocked
- **Reverse lookup, any position:** "all CNPJs with CNAE 5611-2"
  (restaurants) — bloom + range on `cnae_codigo` lands a few row
  groups, ~MBs downloaded.
- **Reverse lookup, primary only:** add `WHERE posicao = 0` —
  row-group min/max stats prune secondary entries because the sort
  keeps primaries clustered.
- **Position analytics:** "for companies whose primary is 5611-2,
  what's the most common position-1 secondary?" Pure SQL aggregate
  over a small filtered set.
- **JOIN to `lookup_cnaes`:** "all CNPJs in *Atividades de
  restaurantes* by description prefix" — composes with §10's
  `lookup_cnaes.parquet`.

### 11.3 Build
From already-loaded `estabelecimento`:

```sql
COPY (
  -- primary
  SELECT
    cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
    cnpj_basico AS cnpj_base,
    cnae_fiscal_principal AS cnae_codigo,
    0::INTEGER AS posicao
  FROM estabelecimento
  WHERE cnae_fiscal_principal IS NOT NULL
    AND cnae_fiscal_principal <> ''
  UNION ALL
  -- secondary, with explicit position via generate_subscripts-like trick
  SELECT
    cnpj_basico || cnpj_ordem || cnpj_dv AS cnpj,
    cnpj_basico,
    trim(s.value) AS cnae_codigo,
    s.idx::INTEGER AS posicao
  FROM estabelecimento,
       LATERAL (
         SELECT idx, value
         FROM (
           SELECT generate_subscripts(arr, 1) AS idx, unnest(arr) AS value
           FROM (SELECT str_split(cnae_fiscal_secundaria, ',') AS arr) t
         )
       ) s
  WHERE cnae_fiscal_secundaria IS NOT NULL
    AND cnae_fiscal_secundaria <> ''
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)
```

(DuckDB has `unnest(... , recursive => true)` and `generate_subscripts`
support — exact syntax may need tweaking; the shape is what matters.)

### 11.4 Coexistence with §9.3
The denormalized array in `cnpjs.parquet` (`cnae_secundario_codigos`,
`cnae_secundario_descricoes`) stays — it's the cheap path for
displaying the lâmina without a JOIN. `cnpj_cnaes.parquet` is the
inverse/analytical index. Same pattern as `socios.parquet` (forward,
denormalized) coexisting with the proposed `pessoas.parquet`
(inverse).

### 11.5 Effort
1 day. New `write_cnpj_cnaes_parquet` in `transform.py`, manifest
entry, optional frontend `attachCnpjCnaes`. Memory budget: cheap —
read from `estabelecimento`, no joins beyond the `UNION ALL`.

---

## 12. CNPJ↔contatos association (`cnpj_contatos.parquet`)

`cnpjs.parquet` carries 7 columns of contact data per estabelecimento
(`ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
correio_eletronico` — `transform.py:459–460`). They're naturally a
multi-valued list collapsed into wide columns. Externalize:

### 12.1 Shape
Columns: `cnpj, cnpj_base, tipo, valor, posicao`.
- `tipo ∈ {'telefone', 'fax', 'email'}`
- For phones: `valor = ddd_N || telefone_N`, `posicao ∈ {1, 2}`
- For fax: `posicao = 0`
- For email: `posicao = 0`, one row per CNPJ when present

Sort by `(tipo, valor, cnpj)`. Bloom on `valor` and on
`split_part(valor, '@', 2)` (email domain) for the
"all CNPJs at @prefeitura.sp.gov.br" pattern flagged earlier.

### 12.2 Use cases
- **Reverse phone lookup** ("who owns this number?") — bloom on
  `valor` lands the row group; <1 MB downloaded.
- **Email-domain analytics** — bloom on the domain expression
  enables public-sector mapping.
- **Detect shared contacts** — `GROUP BY valor HAVING count(*) > 1`
  surfaces phone numbers shared across CNPJs (a real
  fraud-investigation signal).

### 12.3 Privacy posture
Phones and emails are PII but RFB publishes them publicly already.
Document in the schema file that this parquet is a re-shape of
publicly-available RFB data with no enrichment. No new exposure;
just better queryability of what's already public.

### 12.4 Effort
1 day. Pure read from `estabelecimento`, no joins, ~5 UNION ALL
arms. ~120M rows after exploding (most CNPJs have 1 phone + 1
email), ~300 MB compressed.

---

## 13. Deferred: temporal + graph layer

Beyond the per-snapshot externalizations above, two larger projects
are valuable but out of scope for unblocking bootstrap:

### 13.1 `simples_history.parquet` — and an empirical question first

The shape of Simples data needs verification before designing this
externalization. The schema at `transform.py:103–111` has only
`cnpj_basico` as a candidate key, and `write_cnpjs_parquet` at
line 478 does `LEFT JOIN simples s ON s.cnpj_basico = est.cnpj_basico`
— which assumes 1:1, but would silently multiply estabelecimento
rows if simples carries multiple rows per cnpj_basico. The
roundtrip-equivalence count check (`transform.py:857–862`) would
catch that on a completed run; bootstrap hasn't completed
end-to-end, so we have **no empirical confirmation** of which case
holds.

**Action before designing simples_history:**
1. Run `SELECT cnpj_basico, COUNT(*) FROM simples GROUP BY 1
   HAVING COUNT(*) > 1 LIMIT 10` on a partial load. The result
   tells us whether RFB emits one row per CNPJ (current
   assumption), one row per opção/exclusão event (multi-row
   per CNPJ within a snapshot), or has data-quality dupes.
2. If multi-row per snapshot: a within-snapshot
   `cnpj_simples.parquet` (`(cnpj_base, regime, evento, data,
   posicao)`) already captures the lifecycle; cross-snapshot
   accumulation is secondary. Also: the current
   `write_cnpjs_parquet` LEFT JOIN is buggy and silently
   inflating row counts — a real bug separate from the audit.
3. If 1:1 within snapshot: the cross-snapshot accumulator is
   the only path to lifecycle history, as previously framed.

Either way the parquet shape (`cnpj_base, regime, evento, data,
…`) is similar; the *source* of the multi-row data differs.
Don't design further until step 1 lands.

### 13.2 Graph traversal — start with self-joins, materialize only if needed
The earlier draft proposed a `socio_edges.parquet` of pre-computed
`(cnpj_a, cnpj_b, via_pessoa)` edges. Walking that back: with §8's
`pessoas.parquet` sorted by `(cpf_mascarado, nome_normalizado)` and
bloomed on both, the 1-hop "companies sharing a sócio with X"
query is a trivial self-join:

```sql
SELECT b.cnpj_base, b.nome_original, b.papel
FROM pessoas a
JOIN pessoas b USING (cpf_mascarado, nome_normalizado)
WHERE a.cnpj_base = ? AND b.cnpj_base <> a.cnpj_base
```

DuckDB-WASM serves this in MBs of bytes downloaded. No new parquet
needed. **Pre-materialize edges only if** one of these proves true
in practice:
- 2-hop / N-hop traversal (recursive CTE on `pessoas`) is too slow
  in the browser → consider a precomputed BFS layer like
  `(cnpj_base, related_cnpj_base, hop_distance)` with **degree
  caps** (drop hub-pessoas with >K connections so traversal doesn't
  fan out across holding-board pessoas with ~100k connections).
- Global graph analytics are required (connected components, hub
  detection, centrality) — those need a different shape entirely
  and probably a non-WASM compute path.

Either path needs benchmarking against `pessoas.parquet` self-joins
first; don't design until that's measured.

### 13.3 Orchestration note
§13.1 specifically needs cross-snapshot orchestration (read prior
month's IA item, diff against current). The current ETL is
single-month/single-input — a temporal layer needs a new workflow
distinct from `etl-monthly.yml`. §13.2 stays single-snapshot;
the self-join pattern works on data we already produce.

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
This is the W1.1 implementation. Run it after Phase 1 confirms it's
needed (i.e. bootstrap re-run on `main` still OOMs at 5.5 GiB on the
LIST(DISTINCT) operator).

---

## Out of scope / deferred

- **Ibis migration (ADR 0017).** Useful refactor, not a perf win.
- **`raizes.parquet` razao_social secondary index.** Deferred until
  §4.1 measurements show name-search is the next bottleneck.
- **Fuzzy address dedup** (Levenshtein, phonetic). §7.2 punts to
  frontend-side trigram search; revisit if accuracy complaints arrive.
- **Switching parquet compression (ZSTD → SNAPPY) or row group size.**
  ZSTD at 200k is well-tuned; gains are <10%.
- **DuckDB-WASM upgrade tracking.** Out-of-scope; orthogonal.
- **Self-hosted runner.** §6's paid 4-core gets us to "good enough"
  without ops burden.
