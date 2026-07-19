# ETL transform benchmark

Scaled, repeatable timing for the `ficha_etl.transform` stages, so each
performance change is defended by a *measured* number instead of intuition.
It runs the real stage functions on synthetic RFB-shaped data big enough to be
representative but small enough to iterate on a laptop.

## Run

```bash
# whole-pipeline stage timings (data cached under bench/.work by scale/chunks)
uv run python bench/benchmark.py --scale 500000 --chunks 10
uv run python bench/benchmark.py --scale 2000000 --chunks 10 --repeats 3 --json out.json

# pairwise A/Bs (need bench/.work/data populated by benchmark.py first)
uv run python bench/ab_contatos_cnaes.py --repeats 5
uv run python bench/ab_typed_keys.py --repeats 5
uv run python bench/ab_codecs.py --repeats 5
uv run python bench/dedup_evidence.py --repeats 5
```

`--scale` = number of empresas (unique `cnpj_basico`); establishments come out
~1.33× that (one matriz each, plus a filial for every third base). A small
fixed fraction (1 in 500 bases) get an injected duplicate empresa/simples row
with a conflicting payload, so `load_main_tables_into_duckdb`'s dedup path is
actually exercised, not bypassed. Numbers are wall-clock seconds on **this**
machine — compare a stage against itself across a code change, not against
another machine. `bench/.work/` is gitignored.

Decision-grade runs use 10 estabelecimento chunks, matching the RFB inventory
`Estabelecimentos0.zip` through `Estabelecimentos9.zip`; pass `--chunks 10`
explicitly for local runs. The Actions decision profile already defaults to 10.

`--repeats` controls how many times a stage sequence (`benchmark.py`) or A/B
pair (`ab_*.py`) runs — default 1 for `benchmark.py` (quick dev iteration), 5
for the `ab_*.py` scripts. **A genuine measurement decision needs `--repeats 3`
or more**; a single run is exploration, not a decision (RFC 0001 §7.10).

For the shared Actions runner and complete evidence bundle, use
`.github/workflows/etl-evidence.yml`. Pull requests run a tiny smoke profile;
`workflow_dispatch` defaults to the decision profile (500,000 scale, 10 chunks,
5 repetitions, fixed seed).

## Method notes

This harness went through a methodology review (2026-07) that found it was
measuring a different execution regime than the one that decides production
behavior. The findings and the fixes:

| Finding | Fix |
|---|---|
| `duckdb.connect()` (in-memory, default threads) — production forces `threads=1` + `memory_limit`/`temp_directory`/`preserve_insertion_order` | `bench/_profile.py:open_production_connection()` — file-backed, same PRAGMAs `transform_snapshot` uses, via the same `pick_memory_limit_gb`/`pick_threads` |
| No record of what configuration/machine actually ran | `bench/_profile.py:capture_environment()` — DuckDB version, effective threads/memory_limit/preserve_insertion_order/temp_directory (read back via `current_setting`, not just what was requested), platform, CPU count, git SHA, harness version, in every JSON result |
| A/B always ran OLD before NEW — warm-cache/CPU-throttle drift always favors the same side | `bench/_profile.py:run_ab()` — strictly alternates which variant runs first, seeded (fixed `SEED = 20260719`) only to pick the *starting* side, so the order is reproducible, not "randomized" |
| Reported the minimum of N runs — one lucky run can look like a real result | `ABResult.median_a`/`median_b` + `spread_a`/`spread_b` — median and spread reported together; `print_summary()` explicitly flags "WITHIN NOISE" when the spread is wider than the delta, and never declares an "X faster" winner on its own |
| Bypassed `load_main_tables_into_duckdb` (called `_create_table_from_csvs` per table by hand) — the dedup path (and its cost) was never measured, and synthetic empresa declared no duplicates | `benchmark.py`/`ab_typed_keys.py` call the real `load_main_tables_into_duckdb`; `generate()` injects a duplicate empresa/simples row (conflicting payload) for 1 in 500 bases |
| Typed-key A/B loaded whole tables once and cast via `ALTER TABLE`+`UPDATE` — production's chunked writer loads one estabelecimento CSV chunk at a time and semi-joins `empresa`/`simples` down to `_emp_c`/`_smp_c` for just that chunk | `ab_typed_keys.py` now runs the actual `write_cnpjs_parquet_chunked` inner-loop body (load chunk → semi-join materialize → project + COPY) on a fixed representative chunk, str vs. typed key |
| `bench/` wasn't linted or run by CI — a schema-registry refactor silently broke all three scripts (removed columns they still referenced) while lint+tests stayed green | `ci.yml`'s `etl` job now lints `bench/` too, and runs all three scripts as a smoke check |

A second review round (2026-07) on the first fix found the typed-key A/B still
wasn't measuring an independent, production-faithful state, and no A/B
verified its two sides actually agreed on the output. Those findings and
their fixes:

| Finding | Fix |
|---|---|
| `ab_typed_keys.py` still added the UINTEGER key via `ALTER TABLE`+`UPDATE` on the *shared* `empresa`/`simples`/`estabelecimento` tables — exactly the in-place-mutation shape the first review round removed elsewhere — and the varchar baseline ended up joining against tables that carried an unused key column production's varchar path never has | `empresa`/`simples`/`estabelecimento` stay pure VARCHAR for the varchar side; a separate `empresa_typed`/`simples_typed` (+ per-chunk typed `estabelecimento`) is materialized via `CREATE TABLE ... AS SELECT` instead — independent state per side, not a shared table mutated in place |
| The one-time `empresa`/`simples` key-materialization cost was measured but only surfaced as a side observation (`key_setup_seconds` in the JSON), never added into any reported total — a real per-snapshot cost the typed approach must pay was effectively invisible | The JSON reports the setup separately and an aggregate with setup included. That aggregate covers repeated measurements of one representative chunk, not a measured full snapshot; interpret it only as corroboration alongside median/spread. |
| `ab_typed_keys.py`'s per-chunk COPY used `COMPRESSION LZ4` — production's `write_cnpjs_parquet_chunked` uses `COMPRESSION ZSTD` | Switched to `ZSTD`, matching `transform.py` exactly |
| Neither A/B script checked that its two variants produced the *same* output | `_profile.py:assert_parquet_equivalent()` — schema + full-content diff (`EXCEPT ALL` both directions) run once, untimed, before the timed loop |
| The CI smoke check ran `--scale 200`, but duplicate injection is 1-in-500 bases | CI uses scale 500, guaranteeing the dedup path is exercised |
| `args.json.write_text(...)` assumed its parent directory already existed | All scripts create the JSON parent directory before writing |

Stage order in `benchmark.py` mirrors `transform_snapshot`: load lookups → one
`load_main_tables_into_duckdb` call (empresa/estabelecimento/simples/socio +
dedup) → contatos/cnaes/enderecos → drop table → chunked cnpjs → roundtrip
verification.

## Decision-grade findings (2026-07-19)

The durable report is
[`evidence/2026-07-19-production-profile.md`](evidence/2026-07-19-production-profile.md).
It records commit/run provenance, environment, medians, spread and resource
measurements. The resulting production decisions are:

| Candidate | Result | Decision |
|---|---|---|
| One-scan contatos (`LATERAL VALUES`) | 62.6% slower, well outside spread | Keep separate scans |
| One-scan CNAEs (`list_concat` + `UNNEST`) | 7.4% slower, outside spread | Keep current writer |
| Companion `UINTEGER` join key | No demonstrated speedup; setup cost and larger DB state | Keep `VARCHAR(8)` |
| LZ4 transient chunk parts | Timing within noise; parts 40.2% larger and state peak 14.4% larger | Keep ZSTD |

The dedup rerun found that the generic duplicate path has a real but bounded
cost (~24% at the fixture's 1-in-500 density), with no spill. Exact and
conflicting duplicate modes had effectively identical CPU medians; the small
wall-clock difference was inside spread. Therefore the fail/quarantine choice in
issue #76 is a data-integrity and unattended-operations decision, not a
performance trade-off.

## Historical exploratory findings

These pre-methodology-review runs are preserved as history. Where a candidate
appears in the decision table above, the current production-profile result
supersedes the old number.

| Change | Exploratory result (old harness) | Current status |
|---|---|---|
| Single-query `reservoir REPEATABLE` roundtrip verification | 23.4s → 1.1s at 300k (21×) | **Still exploratory:** not included in the 2026-07-19 evidence run |
| One-scan contatos | 2.04× slower | Rejected again under production profile |
| One-scan CNAEs | 1.16× slower | Rejected again under production profile |
| `UINTEGER` companion join key | ~0% plus setup cost | Rejected again under production profile |

The roundtrip-verification result remains the one outstanding evidence gap. A
21× exploratory gap is promising, but RFC 0001 §7.10 still forbids treating it
as settled until it is rerun under the current production profile.
