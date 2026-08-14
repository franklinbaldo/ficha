#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
TMP_DIR="$(mktemp -d -t ficha-okf-generated-XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

OKF_OUT_DIR="$TMP_DIR" bash scripts/generate-okf-contracts.sh

diff -u web/src/generated/ficha-okf.zod.ts "$TMP_DIR/ficha-okf.zod.ts"
diff -u web/src/generated/ficha-okf.views.ts "$TMP_DIR/ficha-okf.views.ts"

echo 'OKF generated contracts are up to date.'
