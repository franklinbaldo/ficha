# Historical canonical estabelecimento shadow

This is the transport/checkpoint slice after the single-part writer. It runs one
`EstabelecimentosN.zip` from a historical `ficha-YYYY-MM` Internet Archive item
through the canonical writer, still outside the monthly pipeline and public
products.

## Manual workflow

Use **Canonical Shadow — historical estabelecimento** (`canonical-shadow-history.yml`).
Inputs are:

- `month`: historical IA item suffix (`YYYY-MM`);
- `part`: one partition from `0` through `9`;
- `sample_size`: deterministic reversible sample size (default `1000`);
- `force`: ignore a matching cached checkpoint and rebuild.

The workflow defaults to `2026-04 / part 0`, the already-bootstrapped historical
snapshot used as the conservative first real target. Pull requests never touch
the network: they run the same orchestration against a generated local ZIP.

## Checkpoint and resume

The orchestration root contains:

```text
raw/EstabelecimentosN.zip
canonical/part-N.parquet
evidence/part-N.quality.json
evidence/part-N.metrics.json
evidence/part-N.history.json
```

`part-N.history.json` records checksums for:

- the orchestration, writer and registry source files;
- the retained source ZIP and extracted CSV;
- canonical Parquet, quality report and metrics envelope.

A cached checkpoint is reused only when month, part, sample size, source URL,
code fingerprints, source ZIP and every output checksum still match. A changed
writer/registry, tampered output or different sample size forces a rebuild.

GitHub Actions caches the full checkpoint root by snapshot/part/schema/code
fingerprint. The downloadable artifact intentionally contains only the canonical
part and evidence; raw and extracted RFB data are not duplicated into artifacts.

## Failure behavior

The ZIP must contain exactly one file. Download/extraction/writer failures create
`part-N.history.failure.json` with all fixity information available at the point
of failure. The underlying writer continues to preserve its own quality and
resource evidence and never replaces a prior good Parquet on a failed gate.

## First real historical run (2026-04, part 0)

The gate described below was exercised for real on 2026-07-20 via
[PR #91](https://github.com/franklinbaldo/ficha/pull/91) (an operational,
non-merging PR that dispatches this workflow and forwards its artifact for
inspection). GitHub Actions artifacts expire, so the load-bearing numbers are
recorded here instead of only living in the run.

- Source commit: `c45f9a35ce7f7115ffeafc435e0ac5294def6481` (PR #90 merge)
- Workflow run: [`29712182515`](https://github.com/franklinbaldo/ficha/actions/runs/29712182515) (35m27s)
- Target: `2026-04 / Estabelecimentos0.zip` (part 0 of 10), sample size 1,000
- Source ZIP: 2,055,198,713 bytes, `sha256:57bad2dc...b73ee2`
- Extracted CSV: 6,543,517,709 bytes, `sha256:2af6c722...097b9`
- Canonical Parquet: 1,384,174,658 bytes, `sha256:98c291c6...58e1e6`

| Metric | Value |
|---|---:|
| Rows read / written | 27,795,908 / 27,795,908 |
| Wall time | 344.2 s |
| Throughput | 3.84 MB/s, 80,759 rows/s |
| RSS peak | 8,840.8 MiB (Δ 8,757.3 MiB) |
| Filesystem peak | 43,655.7 / 147,718.6 MiB (29.55%) |
| Required-key failures (`cnpj_basico`/`cnpj_ordem`/`cnpj_dv`) | 0 |
| Duplicate keys | 0 |
| Invalid casts (`null-and-count`, `data_situacao_cadastral`) | 49,960 (0.18% of rows) |
| Sample fingerprint mismatches (n=1,000, seed 42) | 0 |
| Schema match | yes |

The 49,960 invalid-cast rows are not dropped or quarantined: `data_situacao_cadastral`
uses the registry's `null-and-count` policy (`try_strptime(...)::DATE`, which
returns `NULL` on a malformed source value rather than raising), so those rows
are written normally with just that one field `NULL`, and the count is carried
in `quality.json`/`metrics.json`. This is a plausible rate of malformed dates in
the RFB source, not evidence of a writer bug -- but see the note in RFC 0001
§8.2 about the `null-and-count` policy's undeclared threshold.

This is one part's worth of evidence, not yet the ten-part snapshot or the
triangular raw/canonical/product comparison described below.

## Deliberate boundary

This workflow processes one part and publishes only a temporary GitHub Actions
artifact. It does not merge ten parts, upload canonical data to Internet Archive,
feed a product, or decide the final physical layout. The next gate is a successful
real historical run -- satisfied once, above, for part 0 of 10 -- after which the
workflow can expand to the remaining nine estabelecimento parts and, per RFC 0001
Fase 3, to `empresa`/`simples`/`socio`/lookups (none of which have a canonical
schema yet -- tracked in [issue #97](https://github.com/franklinbaldo/ficha/issues/97)).
Only after all entities have canonical coverage does triangular raw/canonical/product
validation become meaningful.
