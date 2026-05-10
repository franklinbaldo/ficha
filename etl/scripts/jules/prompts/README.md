# Jules prompt roster

Prompts under this directory are self-contained delegation specs for
parallel workstreams from `docs/perf-plan-2026-05.md`. Fire each
via `claude-jules.yml`:

```
gh workflow run claude-jules.yml \
  -f title="<short title>" \
  -f prompt_file=<filename without .md> \
  -f starting_branch=main \
  -f target_pr=31 \
  -f automation_mode=AUTO_CREATE_PR \
  -f require_plan_approval=false
```

(Or via the Actions UI under "Claude — Jules session".)

## Currently ready

| Prompt | Workstream | Plan ref | Branches from | Independent of |
|--------|------------|----------|---------------|----------------|
| `w21_encoding_sniff` | Encoding sniff in `_create_table_from_csvs` | §2.1 / Phase 4 PR 4a / M1 | `main` | M0, frontend, all other ETL changes |
| `w41_search_length14_branch` | Frontend length-14 branch in `SearchCNPJ.svelte` | §4.1 / Phase 5 PR 5a / M2 | `main` | M0, ETL changes — frontend only |
| `w10_per_lookup_parquets` | Per-lookup parquets + `attachLookups` | §10 / Phase 6 / M4 | `claude/ficha-perf-plan-v2` | M0 (additive); validates the `attachLookups` pattern reused by later M4 parquets |

All three can run **simultaneously** — independent files, no
overlapping touches in `transform.py` (W2.1 modifies
`_create_table_from_csvs` only; W10 adds `write_lookup_parquets`;
W4.1 is web/ only).

## How to add a new prompt

1. Pick a workstream from `docs/perf-plan-2026-05.md` that's
   parallelizable (see Track A–E and "Parallel work tracks").
2. Write a self-contained `<id>.md` here with: context, scope,
   acceptance criteria, plan reference, branch+PR convention,
   out-of-scope guardrails.
3. Add a row to the table above.

Self-containment matters because Jules has no conversation
context with this repo — the prompt file IS the brief.
