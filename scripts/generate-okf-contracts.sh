#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p web/src/generated

OKF=(uvx --python 3.12 --from okf-parser==0.42.0 okf-parser)

"${OKF[@]}" check knowledge --relational-schema okf.schema.sql
"${OKF[@]}" schema knowledge --format zod --infer-types > web/src/generated/ficha-okf.zod.ts
"${OKF[@]}" schema knowledge --format json --infer-types > web/src/generated/ficha-okf.schema.json

echo "validated knowledge/okf.schema.sql"
echo "generated web/src/generated/ficha-okf.zod.ts"
echo "generated web/src/generated/ficha-okf.schema.json"
