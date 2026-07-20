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

## Deliberate boundary

This workflow processes one part and publishes only a temporary GitHub Actions
artifact. It does not merge ten parts, upload canonical data to Internet Archive,
feed a product, or decide the final physical layout. The next gate is a successful
real historical run; only then should the workflow expand to the ten-part snapshot
and triangular raw/canonical/product validation.
