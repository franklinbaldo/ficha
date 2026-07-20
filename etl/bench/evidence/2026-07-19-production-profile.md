# ETL decision-grade evidence — 2026-07-19

This is the durable decision record for the production-profile rerun enabled by
PR #80. The GitHub Actions artifact expires after 30 days; the conclusions and
load-bearing numbers therefore live in the repository.

## Provenance

- Source commit: `3da5a3f3e2584ab1968ffb252139130d42988393`
- Workflow run: `29707601921`
- Profile: 500,000 unique `cnpj_basico`, 10 estabelecimento chunks, 5 repetitions
- Alternation seed: `20260719`
- Harness: `2026-07-profile-v3`
- Runtime: Python 3.11.15, DuckDB 1.5.2, Linux x86_64, 4 CPUs
- Effective DuckDB profile: `threads=1`, file-backed databases,
  `memory_limit=8.3 GiB`, `preserve_insertion_order=false`

All A/B tracks required equivalent output before accepting timings. Decisions
compare median differences against observed spread and include disk/resource
costs; a lower single timing is never treated as a winner by itself.

## Decisions

| Question | Production/current | Candidate | Evidence | Decision |
|---|---:|---:|---|---|
| Contatos: separate scans vs one-scan `LATERAL VALUES` | 1.2048 s median, 0.0184 s spread | 1.9595 s median, 0.0609 s spread | Candidate is 62.6% slower; the 0.7547 s delta is far above spread | Keep separate scans |
| CNAEs: current `UNION ALL` vs one-scan `list_concat` + `UNNEST` | 0.8593 s median, 0.0200 s spread | 0.9227 s median, 0.0164 s spread | Candidate is 7.4% slower; the 0.0634 s delta is above spread | Keep current writer |
| Join key: `VARCHAR(8)` vs companion `UINTEGER` | 0.9168 s median | 1.2775 s median plus 1.1387 s one-time key setup | Per-chunk spread is high, but the candidate demonstrates no speedup and peak candidate DB size was 19.8% larger | Keep `VARCHAR(8)`; do not add companion keys |
| Transient chunk codec: ZSTD vs LZ4, final output fixed at ZSTD | 11.9768 s median, 29.10 MB parts | 12.0690 s median, 40.79 MB parts | Timing delta is within noise; LZ4 parts are 40.2% larger and state peak is 14.4% larger | Keep transient ZSTD |

The typed-key JSON also reports 5.3356 s vs 7.8790 s under
`end_to_end_total_seconds`. That field is the aggregate of five repeated
measurements of the **same representative chunk**, with key setup added once on
the typed side. It is useful corroboration, not a measured full-snapshot total.
The decision rests on the absence of a demonstrated per-chunk benefit plus the
setup and resource penalties, not on pretending that aggregate is a production
snapshot duration.

These reruns confirm the earlier exploratory rejections, now under the same
file-backed, single-threaded profile used by production.

## Deduplication envelope

The real loader was measured with no duplicates, exact duplicates, and
conflicting duplicates. Each mode ran in a fresh child process/database.

| Mode | Wall median | Wall spread | CPU median | DB size | Duplicate rows |
|---|---:|---:|---:|---:|---:|
| none | 5.8860 s | 0.1912 s | 5.6468 s | 39,858,176 B | 0 |
| exact | 7.3038 s | 0.2967 s | 7.0341 s | 49,295,360 B | 2,000 |
| conflicting | 7.6394 s | 1.1240 s | 7.0340 s | 49,295,360 B | 2,000 |

The generic duplicate path costs about 24% wall/CPU at this fixture density and
increases the database footprint by about 24%. There was no DuckDB spill.

The difference between exact and conflicting inputs is **not decision-grade**:
wall-clock differs by 0.3355 s while the conflicting spread is 1.1240 s, and CPU
medians are effectively identical. Therefore issue #76 remains a data-integrity
and unattended-operations policy choice, not a performance choice. The evidence
does not justify keeping deterministic collapse merely to save resources.

`ru_maxrss` did not establish new high-water marks after the post-lookup
baseline in this large run, so RSS deltas are zero by the documented cumulative
semantics. This does not mean the loader used no memory; it means setup had
already established a higher process-lifetime peak.

## Gap at the time of this run — closed 2026-07-20

The historical `verify_roundtrip` change (`ORDER BY random()` plus point lookups
→ `reservoir REPEATABLE`) was not one of the four tracks in this run, so it was
correctly left unsettled here. The follow-up
[`2026-07-20-roundtrip-verifier.md`](2026-07-20-roundtrip-verifier.md) closed that
gap under the same production profile: the current single-query verifier measured
0.1492 s median versus 14.8044 s for the normalized historical multi-query shape
(99.2×), with matching samples and both correctness directions verified.
