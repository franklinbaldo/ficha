# W2.1 — Encoding sniff in `_create_table_from_csvs`

## Context

`etl/src/ficha_etl/transform.py:_create_table_from_csvs` (lines ~187–270)
has an encoding fallback chain that retries `read_csv` with three
combinations: `(latin-1, False)`, `(utf-8, False)`, `(utf-8, True)`.

For RFB's `estabelecimento` partition (~15 GB on disk), this means
~3 full-file reads on the failing case before the right config is
picked. PR #24's monthly run logs show ~280 s for estabelecimento on
the utf-8+ignore_errors fallback, vs. ~60 s for empresa where
latin-1 is correct first try.

This is wasteful. Pre-sniff the first 1 MB on the Python side and
pick the right encoding directly.

## Scope

In `etl/src/ficha_etl/transform.py`, modify `_create_table_from_csvs`
(only this function — do not touch any other function or file).

Replace the three-attempt loop with:

1. Sniff the first 1 MB of the *first non-empty CSV* in the path list.
2. Try `bytes.decode('utf-8')` strict on the sample.
3. If utf-8 succeeds: use `(encoding='utf-8', ignore_errors=True)`. We
   keep `ignore_errors=True` because a 1 MB sample can't prove the
   rest of a 15 GB file is clean — RFB occasionally emits
   mixed-encoding rows mid-file.
4. If utf-8 raises `UnicodeDecodeError`: use `(encoding='latin-1',
   ignore_errors=False)`.
5. Run `read_csv` once with the chosen config. If it fails, fall
   through to the existing `(utf-8, True)` last-resort branch as a
   safety net (that one is byte-tolerant by design).

**Important constraints:**
- Keep `latin-1` as the latin-1 branch (it never raises on `decode`,
  so we MUST test utf-8 first — see Codex review on PR #26 for the
  pitfall: `bytes.decode('latin-1')` always succeeds because every
  byte is a valid latin-1 codepoint).
- Preserve the existing log warnings on fallback so production
  logs continue to surface encoding decisions.
- Leave the function signature unchanged.
- Update or extend tests in `etl/tests/test_transform.py` to cover
  both the utf-8-detected path and the latin-1-detected path. There
  are existing tests `test_load_lookup`, `test_load_lookup_preserves_iso_encoding`
  to study for fixture style.

## Acceptance criteria

- `uv run pytest etl/tests/` — all tests pass
- `uv run --directory etl ruff check src tests && uv run --directory etl ruff format --check src tests` — clean
- Reading a known-utf-8 file picks `(utf-8, ignore_errors=True)` on
  the first try (visible in test or via log inspection).
- Reading a known-latin-1 file picks `(latin-1, ignore_errors=False)`
  on the first try.
- Estimated phase-2 wall-time saving: ~3 minutes per snapshot (we
  can't measure on a unit test; just confirm the sniff correctly
  picks the right branch).

## Plan reference

`docs/perf-plan-2026-05.md` §2.1 / W2.1 / Phase 4 PR 4a / M1 (post-M0).

This PR is independent of M0 — encoding sniff doesn't touch
`write_*_parquet` and is safe to merge before the OOM unblock lands.

## Branch + PR

- Start from `claude/ficha-perf-plan-v2` (the active perf-plan
  working branch).
- Open PR against `claude/ficha-perf-plan-v2` titled
  `perf(etl): W2.1 encoding sniff in _create_table_from_csvs (Phase 4 PR 4a)`.
- PR body should reference this prompt file and link
  `docs/perf-plan-2026-05.md` §2.1.

## Out of scope

Do **not**:
- Touch `write_cnpjs_parquet`, `write_raizes_parquet`,
  `write_socios_parquet`, or `transform_snapshot`.
- Modify the encoding choices for *lookup* tables (they're already
  fine: lookups are small and unambiguously latin-1).
- Add new dependencies.
- Refactor the function beyond what the encoding-sniff change
  requires.
