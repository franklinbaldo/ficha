# Verne — compact activity summary by default

## Context

You are working on `franklinbaldo/verne`, the unofficial CLI for
the Google Jules API.

`verne sessions show <id> --activities` (and `--json`) currently
dumps the FULL activity payload for every activity:
- `bashOutput.output` — kilobytes of stdout/stderr per command
- `changeSet.gitPatch.unidiffPatch` — entire diff text
- `progressUpdated.description` — multi-paragraph reasoning

For a 60-activity session this produces hundreds of KB of
output. When that output is consumed by an LLM (Claude reading
inspect-workflow comments on a PR) or a CI pipeline, the noise
crowds out the *signal* — what type of activity, what step, what
file, what verdict.

## Scope

Make activity output compact by default; require `--full` to get
the firehose.

### Default rendering — one line per activity

Format:
```
[ISO-timestamp] originator type: short_subject  (id_prefix)
```

Where `short_subject` is type-specific:
- `planGenerated` → `N steps`
- `planApproved` → `(by user|agent)`
- `progressUpdated` → first 80 chars of `title`
- `agentMessage` → first 80 chars of `text`
- `userMessage` → first 80 chars of `prompt`
- `sessionCompleted` → `(no body)`
- unknown types → list of present keys

For each activity, attach a single-line **artifact summary** when
present:
- `bashOutput` → `$ <command, ≤80 chars>` and `→ exit=N` (or
  `→ <output ≤60 chars>...` if no exit code)
- `changeSet` → `<file_count> files, +<add>/-<del>` (parse the
  unidiffPatch header for stats; if no patch, list source ID)
- `media` → `<mime_type> N bytes`

Optional flags:
- `--full` — current verbose behavior (full patches, full
  bash output, full descriptions). Same JSON shape as today
  but only when `--json` is also set.
- `--summary-only` — even more compact: hide artifacts entirely,
  one line per activity with just type + 40-char subject.
- `--limit N` — show only the latest N activities (newest
  first). Default: unlimited (existing behavior).
- `--type X` — filter by activity type (`planGenerated`,
  `progressUpdated`, etc.). Repeatable.

### `--json` interaction

`verne sessions show --activities --json` should output a
JSON object with TWO fields:
```json
{
  "session": {...},
  "activities": [...]
}
```
By default, each activity in the array has the **same shape** as
today but with these fields elided when not `--full`:
- `bashOutput.output` → replaced with first 200 chars +
  `... [truncated N bytes]`
- `changeSet.gitPatch.unidiffPatch` → kept as a stat header
  only (`@@ N files, +X/-Y @@`); full patch dropped
- `progressUpdated.description` → first 500 chars +
  `... [truncated N bytes]`

A new field `_truncated: true` is added to any activity whose
content was elided, so consumers can detect compactness.

`--full` restores the original full payload.

### Tests

In `src/features/cli/`:
- New feature: activity output is one-line-per-activity by
  default
- `--full` flag restores verbose
- `--limit 5` shows only 5 newest
- `--json` without `--full` includes `_truncated: true` markers
  on truncated activities
- `--json --full` does NOT include `_truncated` markers

Mock fixture: a session with 30+ activities including
bashOutput (50 KB) and unidiffPatch (200 KB).

## Acceptance criteria

- `verne sessions show <id> --activities` of a typical session
  fits in <2 KB of output (was ~hundreds of KB).
- `verne sessions show <id> --activities --full` is identical
  to today's output.
- `--json` round-trip: parse → re-dump → identical to before
  for `--full` mode; smaller for default mode.
- README updated with the new flag matrix.

## Plan reference

This complements `verne_wait_until_attention.md` (the
companion prompt adding `verne sessions wait`). Both improve
verne's usability for cross-repo orchestration like ficha's
`poll_all_sessions.py`.

## Branch + PR

- Start from `main`.
- Open PR against `main` titled
  `feat(cli): compact activity summary by default; --full opt-in`.
- PR body should reference this prompt and explain the LLM /
  CI motivation: dumps hundreds of KB into pipeline contexts
  by default, drowning the signal in patch text.

## Out of scope

Do **not**:
- Change the underlying Jules API client (still fetch full
  activities; truncation happens at render/output time).
- Add a config file for default truncation thresholds (just
  hardcode reasonable limits — 200/500/80 chars per field).
- Refactor unrelated commands.
- Change non-activity output (e.g., `verne sessions list`
  stays as-is).
