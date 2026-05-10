# Verne — emit strict JSON from `--json` flags

## Context

You are working on `franklinbaldo/verne`, the unofficial CLI for
the Google Jules API.

I'm using verne from a separate repo (`franklinbaldo/ficha`) via
`uvx --from "git+https://github.com/franklinbaldo/verne" verne
sessions show <id> --activities --json`. The output is intended
to be piped into `python3 -c "import json; …"`, but
`json.load(...)` fails:

```
parse error: Invalid control character at: line 14 column 73 (char 517)
```

The issue: verne emits JSON where string values contain *literal*
control characters (raw `\n`, `\t`, etc.) instead of escape
sequences (`\\n`, `\\t`). This violates RFC 8259 §7 ("string"
production: characters must be escaped). Strict parsers reject
the output; only Python's `json.loads(..., strict=False)` accepts
it.

The most likely culprits are activity payloads that wrap bash
output or unidiff patches — those naturally contain newlines and
the dumping path is feeding them into `print()` or
`json.dumps(..., indent=...)` without re-encoding the strings.

## Scope

Find every place in `src/verne/` (and any related output module)
where the CLI emits JSON and ensure the output is RFC-strict.

Search starting points:
- `src/verne/cli/errors.py` — `json_output()` helper (likely the
  central path used by every `--json` command)
- Anywhere that calls `print(json.dumps(...))` or
  `typer.echo(json.dumps(...))`

The likely fix is one of:
1. Pass `ensure_ascii=False` is fine — that's not the bug. The
   bug is upstream: somewhere a raw string is being inserted
   into the output without going through `json.dumps`.
2. The `json.dumps(..., indent=2)` shape *should* always escape
   control chars. If it doesn't, the input may have already been
   pre-formatted (e.g., concatenated as a string with raw
   newlines).
3. Check that the activity dump from the API is being treated as
   an opaque dict that's then `json.dumps`'d wholesale — not
   rebuilt by string-interpolating values.

## Repro

```bash
uvx --from "git+https://github.com/franklinbaldo/verne" \
  verne sessions show 15401189606457262357 --activities --json \
  | python3 -c "import json, sys; json.load(sys.stdin)"
```

Expected: silent success.
Actual: `json.decoder.JSONDecodeError: Invalid control character`.

## Acceptance criteria

- The repro command above runs cleanly with default `json.load`
  (no `strict=False`).
- All existing `--json` outputs across `verne sessions list`,
  `verne sessions show`, `verne sessions status`, etc., remain
  RFC-strict — add a regression test if there's an obvious place
  to put one (likely a small pytest that runs the command and
  feeds output to `json.loads` strict).
- No behavior change beyond strictness: existing field names,
  ordering, and shape are preserved.

## Branch + PR

- Start from `main`.
- Open PR against `main` titled
  `fix(cli): emit strict JSON from --json flags (escape control chars)`.
- Reference this prompt file in the PR body. Mention that the
  bug was discovered while integrating verne into a different
  repo's CI pipeline (`franklinbaldo/ficha`) where strict
  parsers refused the output.

## Out of scope

Do **not**:
- Refactor the CLI structure beyond the strict-JSON fix.
- Change the JSON schema or field names.
- Add new dependencies.
- Add new `--json` outputs to commands that don't have them.
