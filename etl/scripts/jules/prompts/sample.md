# Sample Jules prompt

Replace this file with the actual prompt for whichever workstream
you're delegating. Long Markdown lives here so we don't fight YAML
escaping in `claude-jules.yml` inputs.

Example invocation:

```
gh workflow run claude-jules.yml \
  -f title="W2.1 — encoding sniff" \
  -f prompt_file=w21_encoding_sniff \
  -f starting_branch=main \
  -f target_pr=31
```

Then create `etl/scripts/jules/prompts/w21_encoding_sniff.md` with the
actual instructions.
