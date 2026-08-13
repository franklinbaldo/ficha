#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p web/src/generated
OKF=(uvx --python 3.12 --from okf-parser==0.42.0 okf-parser)
"${OKF[@]}" check knowledge --relational-schema okf.schema.sql
"${OKF[@]}" schema knowledge --format zod --spec-template 'knowledge/types/{slug}.md' > web/src/generated/ficha-okf.zod.ts
"${OKF[@]}" schema knowledge --format json --spec-template 'knowledge/types/{slug}.md' > web/src/generated/ficha-okf.schema.json
TMP_DB="$(mktemp -t ficha-okf-XXXXXX.duckdb)"
trap 'rm -f "$TMP_DB"' EXIT
"${OKF[@]}" duckdb knowledge "$TMP_DB" okf --overwrite --spec-template 'knowledge/types/{slug}.md' >/dev/null
uv run --with duckdb python scripts/export-okf-views.py "$TMP_DB" > web/src/generated/ficha-okf.views.ts
