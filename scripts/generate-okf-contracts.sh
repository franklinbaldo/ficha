#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p web/src/generated
OKF=(uvx --python 3.12 --from okf-parser==0.42.0 okf-parser)
SPEC_TEMPLATE='types/{slug}.md'
DATA_ONLY=(--exclude 'types/**' --exclude 'views/**' --exclude 'README.md')

"${OKF[@]}" check knowledge --relational-schema okf.schema.sql

# O frontend consome apenas os tipos das linhas publicadas. TypeSpec/View/README
# pertencem ao próprio modelo OKF e não devem aparecer como schemas de dados.
# O template é bundle-relative: o root passado ao parser já é `knowledge/`.
"${OKF[@]}" schema knowledge --format zod --spec-template "$SPEC_TEMPLATE" "${DATA_ONLY[@]}" > web/src/generated/ficha-okf.zod.ts
"${OKF[@]}" schema knowledge --format json --spec-template "$SPEC_TEMPLATE" "${DATA_ONLY[@]}" > web/src/generated/ficha-okf.schema.json

TMP_DB="$(mktemp -t ficha-okf-XXXXXX.duckdb)"
trap 'rm -f "$TMP_DB"' EXIT
"${OKF[@]}" duckdb knowledge "$TMP_DB" okf --overwrite --spec-template "$SPEC_TEMPLATE" >/dev/null
uv run --with duckdb python scripts/export-okf-views.py "$TMP_DB" > web/src/generated/ficha-okf.views.ts
