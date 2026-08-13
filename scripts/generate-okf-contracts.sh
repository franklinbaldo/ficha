#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p web/src/generated

uvx --from okf-parser==0.41.3 okf-parser check knowledge
uvx --from okf-parser==0.41.3 okf-parser schema knowledge --format zod --infer-types > web/src/generated/ficha-okf.zod.ts
uvx --from okf-parser==0.41.3 okf-parser schema knowledge --format json --infer-types > web/src/generated/ficha-okf.schema.json

echo "generated web/src/generated/ficha-okf.zod.ts"
echo "generated web/src/generated/ficha-okf.schema.json"
