#!/usr/bin/env node
/**
 * Regenerate src/generated/company.pb.{js,d.ts} from the shared proto.
 *
 * The committed output is what gets bundled — this script only runs when
 * a contributor updates `proto/ficha/v1/company.proto` and needs to refresh
 * the JS/TS bindings.  Usage:
 *
 *   cd web && bun run gen-proto
 */
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(__dirname, '..', '..');
const PROTO = resolve(REPO_ROOT, 'proto', 'ficha', 'v1', 'company.proto');
const OUT_DIR = resolve(__dirname, '..', 'src', 'generated');
const OUT_JS = resolve(OUT_DIR, 'company.pb.js');
const OUT_DTS = resolve(OUT_DIR, 'company.pb.d.ts');

if (!existsSync(PROTO)) {
  console.error(`[gen-proto] missing ${PROTO}`);
  process.exit(1);
}
mkdirSync(OUT_DIR, { recursive: true });

function run(bin, args) {
  console.log(`[gen-proto] ${bin} ${args.join(' ')}`);
  const r = spawnSync('npx', [bin, ...args], { stdio: 'inherit' });
  if (r.status !== 0) process.exit(r.status ?? 1);
}

// --force-number coerces 64-bit fields (cnpj_socio) to plain numbers —
// safe for CNPJs (14 digits ≪ Number.MAX_SAFE_INTEGER) and avoids the
// `Long` runtime dep on the consumer side.
run('pbjs', [
  '--target', 'static-module',
  '--wrap', 'es6',
  '--es6',
  '--keep-case',
  '--force-number',
  PROTO,
  '-o', OUT_JS,
]);

run('pbts', ['-o', OUT_DTS, OUT_JS]);

console.log(`[gen-proto] wrote ${OUT_JS} + ${OUT_DTS}`);
