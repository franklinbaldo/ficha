# Verne — `verne sessions wait` command (block until state change)

## Context

You are working on `franklinbaldo/verne`, the unofficial CLI for
the Google Jules API.

When orchestrating Jules sessions from external automation (CI
pipelines, scheduled scripts), there's a recurring need: **block
until a session reaches a state worth acting on.** Today,
operators have to script their own polling loop — list activities,
diff against last seen, sleep, repeat — duplicating logic that
belongs in the CLI itself.

This use case showed up in `franklinbaldo/ficha`, where I built an
ad-hoc poller (`etl/scripts/jules/poll_all_sessions.py`) doing
exactly this: polls every 60s, exits when any session reaches a
notable state. Verne should expose this as a primitive so other
projects don't reinvent it.

## Scope

Add a new subcommand: **`verne sessions wait`**

```
verne sessions wait <session_id> [options]
```

### Behavior

Block, polling Jules at `--interval` seconds, until the session
enters one of the **stop states**:

- `--on completion` — exit on TERMINAL_STATES (`COMPLETED`,
  `FAILED`, `PAUSED`)
- `--on attention` — exit on ATTENTION_STATES
  (`AWAITING_PLAN_APPROVAL`, `AWAITING_USER_INPUT`,
  `AWAITING_USER_FEEDBACK`, `FAILED`, etc.)
- `--on any` (default) — exit on any of the above
- `--on idle <minutes>` — also exit if no new activity for N
  minutes (heuristic; useful when state transitions don't fire)

State enums already defined in `src/verne/cli/display.py`
(`ACTIVE_STATES`, `ATTENTION_STATES`, `TERMINAL_STATES`).

### Options

```
--interval <seconds>       Poll interval. Default: 60.
--max-wait <minutes>       Hard timeout. Default: 240.
--on <mode>                completion | attention | any | idle:<min>. Default: any.
--quiet                    Suppress per-poll progress lines.
--json                     On exit, emit final session JSON to stdout.
```

### Output

- Default: print one line per poll iteration with state +
  activity count + elapsed time.
- On exit: print the final state and (if `--json`) the full
  session dump.
- Exit code:
  - `0` — exited because session reached the requested stop
    state.
  - `1` — exited because of `--max-wait` timeout.
  - `2` — exited because of `--on idle` timeout (when explicitly
    chosen as a stop state, that's still a "matched" exit, so
    `0`; this `2` is reserved for "wall-time exhausted while
    actively polling").

### Implementation notes

- Reuse the existing `JulesClient` for `get_session()` /
  `list_activities()`.
- Stop states need to be detected via `session.state` — if that
  field is absent for some sessions (older sessions or older API
  versions), fall back to inspecting activities for
  `sessionCompleted`.
- For `--on idle <min>`, track the last activity ID seen
  per-iteration and the time it changed.
- Idempotent re-runs: don't make assumptions about state from a
  previous invocation — a fresh `wait` call should always
  re-baseline.

### Tests

Add a Gherkin feature `src/features/cli/sessions_wait.feature`
covering:
- Wait exits on COMPLETED state
- Wait exits on AWAITING_USER_INPUT state when `--on attention`
- Wait exits on `--max-wait` timeout
- Wait exits on idle timeout when `--on idle:5` and no activity
- `--json` output is RFC-strict (this also fixes the related
  bug where verne emits non-strict JSON; see prompt
  `verne_strict_json.md` if it lands first)

Mock the Jules API in tests — pytest-bdd with httpx mocks.

## Acceptance criteria

- `uv run verne sessions wait --help` documents all options.
- `uv run pytest tests/cli/` passes including the new feature.
- The command works against a real session ID (smoke test
  scripted in PR description).
- README updated with a "Watch a session until something
  notable happens" example, replacing or augmenting the existing
  `verne sessions watch` doc if applicable.

## Branch + PR

- Start from `main`.
- Open PR against `main` titled
  `feat(cli): verne sessions wait — block until state change`.
- PR body should reference this prompt and explain the use case
  (cross-repo orchestration; see `franklinbaldo/ficha`'s
  `poll_all_sessions.py` for prior art that this command
  obsoletes).

## Out of scope

Do **not**:
- Refactor `verne sessions watch` (it's a different surface;
  watch tails activities, wait blocks for state).
- Add a `--watch` flag to other commands.
- Change existing commands' behavior or output.
- Add new dependencies.
