# ETL transform benchmark

Scaled, repeatable timing for the `ficha_etl.transform` stages, so each
performance change is defended by a *measured* number instead of intuition.
It runs the real stage functions on synthetic RFB-shaped data big enough to be
representative but small enough to iterate on a laptop.

## Run

```bash
# whole-pipeline stage timings (data cached under bench/.work by scale/chunks)
uv run python bench/benchmark.py --scale 500000 --chunks 8
uv run python bench/benchmark.py --scale 2000000 --chunks 16 --repeats 3 --json out.json

# pairwise A/Bs (need bench/.work/data populated by benchmark.py first)
uv run python bench/ab_contatos_cnaes.py --repeats 5
uv run python bench/ab_typed_keys.py --repeats 5
```

`--scale` = number of empresas (unique `cnpj_basico`); establishments come out
~1.33× that (one matriz each, plus a filial for every third base). A small
fixed fraction (1 in 500 bases) get an injected duplicate empresa/simples row
with a conflicting payload, so `load_main_tables_into_duckdb`'s dedup path is
actually exercised, not bypassed. Numbers are wall-clock seconds on **this**
machine — compare a stage against itself across a code change, not against
another machine. `bench/.work/` is gitignored.

`--repeats` controls how many times a stage sequence (`benchmark.py`) or A/B
pair (`ab_*.py`) runs — default 1 for `benchmark.py` (quick dev iteration), 5
for the `ab_*.py` scripts. **A genuine measurement decision needs `--repeats 3`
or more**; a single run is exploration, not a decision (RFC 0001 §7.10).

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
| The one-time `empresa`/`simples` key-materialization cost was measured but only surfaced as a side observation (`key_setup_seconds` in the JSON), never added into any reported total — a real per-snapshot cost the typed approach must pay was effectively invisible | `main()` now prints and reports an explicit end-to-end total per side, `uint total = key_cost + sum(chunk times)`, alongside the per-chunk median/spread |
| `ab_typed_keys.py`'s per-chunk COPY used `COMPRESSION LZ4` — production's `write_cnpjs_parquet_chunked` uses `COMPRESSION ZSTD` (LZ4 was never validated for production disk peak, see the dedup-fix history) — so the harness wasn't running the codec production actually runs | Switched to `ZSTD`, matching `transform.py` exactly |
| Neither A/B script checked that its two variants produced the *same* output — a rewritten query that's faster but wrong (dropped rows, different NULL handling, changed types) would read as a win | `_profile.py:assert_parquet_equivalent()` — schema + full-content diff (`EXCEPT ALL` both directions) run once, untimed, before the timed loop; both scripts now fail loudly if the sides disagree |
| The CI smoke check ran `--scale 200`, but `generate()`'s duplicate injection is 1-in-500 bases — the smoke check never actually exercised the dedup path it claimed to guard | `ci.yml` bumped to `--scale 500` (guarantees one injected duplicate at base=500) |
| `args.json.write_text(...)` assumed its parent directory already existed | All three scripts now `args.json.parent.mkdir(parents=True, exist_ok=True)` before writing |

Stage order in `benchmark.py` mirrors `transform_snapshot`: load lookups → one
`load_main_tables_into_duckdb` call (empresa/estabelecimento/simples/socio +
dedup — production wraps this in a single `load_duckdb` stage too, so this
harness's granularity matches production's, not an invented finer breakdown)
→ contatos/cnaes/enderecos (scan the in-memory table) → drop table → chunked
cnpjs (reloads CSV per chunk) → roundtrip verify (reloads CSV).

## Findings — exploratory, not yet re-verified under the current harness

The table below is the **pre-methodology-review** record. Every one of these
runs used the old in-memory/default-threads connection, always-OLD-before-NEW
ordering, and (for contatos/cnaes/typed-keys) never exercised the real loader's
dedup path — exactly the gaps the table above documents. **Kept as history, not
silently overwritten** — but none of these numbers should be treated as a
current production-profile measurement, and no conclusion here should gate a
merge decision until it's rerun under the harness described above. Re-running
these is deliberately a separate, later step from the methodology fix itself.

| Change | Stage | Exploratory result (old harness) |
|---|---|---|
| Single-query `reservoir REPEATABLE` verify | `verify_roundtrip` | 23.4s → 1.1s at 300k (21×); the old `ORDER BY random()` + 1000 point-lookups dominated the whole run |
| One-scan contatos (`LATERAL VALUES`) | `write_cnpj_contatos` | 2.04× slower (0.835s → 1.700s at 1M); rejected |
| One-scan cnaes (`list_concat`+`UNNEST`) | `write_cnpj_cnaes` | 1.16× slower (0.743s → 0.862s); rejected |
| `UINTEGER` companion join key vs `VARCHAR(8)` `cnpj_basico` | cnpjs empresa/simples join | ~0% (within noise) — min 3.228s vs 3.169s over 10 interleaved iters, plus ~1.9s key-materialization cost; rejected |

The verify-speedup direction is very likely still correct (a 21× gap is larger
than any methodology gap could plausibly close), but "very likely" is exactly
the kind of laptop-exploration conclusion RFC 0001 §7.10 says isn't a decision
— it should still be rerun under the production profile before being cited as
settled. The contatos/cnaes and typed-key rejections are lower-confidence:
they were closer calls (a couple-percent to ~2× difference) and the typed-key
run in particular never touched the real per-chunk join shape or the dedup
path at all.
