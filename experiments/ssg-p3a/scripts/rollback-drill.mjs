import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  cpSync, existsSync, lstatSync, mkdtempSync, mkdirSync, readFileSync,
  readdirSync, renameSync, rmSync, writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { verifyBuiltShell } from '../tests/smoke-built.mjs';

const MAX_BYTES = 32 * 1024 * 1024;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

/** SHA-256 of sorted relative paths and file bytes; reject symlinks and oversized inputs. */
export function inventory(root) {
  assert.ok(existsSync(root) && lstatSync(root).isDirectory(), 'artefato ausente');
  const entries = [];
  let bytes = 0;
  function walk(dir, prefix = '') {
    for (const name of readdirSync(dir).sort()) {
      const path = join(dir, name);
      const st = lstatSync(path);
      assert.ok(!st.isSymbolicLink(), `symlink proibido: ${prefix}${name}`);
      if (st.isDirectory()) walk(path, `${prefix}${name}/`);
      else {
        assert.ok(st.isFile(), `tipo de arquivo proibido: ${prefix}${name}`);
        bytes += st.size;
        assert.ok(bytes <= MAX_BYTES, 'artefato acima do limite de 32 MiB');
        entries.push([`${prefix}${name}`, sha(readFileSync(path))]);
      }
    }
  }
  walk(root);
  assert.ok(entries.some(([path]) => path === 'index.html'), 'index.html ausente no artefato');
  return { sha256: sha(JSON.stringify(entries)), files: entries.length, bytes };
}

/** Offline rehearsal only: replace a sandbox slot then restore the exact previous bytes. */
export function rehearseRollback(previousDir, candidateDir, options = {}) {
  const previous = resolve(previousDir);
  const candidate = resolve(candidateDir);
  assert.ok(previous !== candidate && !previous.startsWith(candidate + sep)
    && !candidate.startsWith(previous + sep), 'artefatos sobrepostos');
  assert.match(options.expectedPreviousSha256 ?? '', /^[a-f0-9]{64}$/,
    'hash anterior independente obrigatório');
  const prior = inventory(previous);
  assert.equal(prior.sha256, options.expectedPreviousSha256, 'hash anterior divergente: abortar');
  verifyBuiltShell(candidate);
  const next = inventory(candidate);
  const sandbox = mkdtempSync(join(tmpdir(), 'ficha-p3a-rollback-'));
  const active = join(sandbox, 'active');
  const staged = join(sandbox, 'candidate');
  const saved = join(sandbox, 'previous');
  const retired = join(sandbox, 'retired');
  let switched = false;
  let injectedFailureCaught = false;
  try {
    cpSync(previous, active, { recursive: true });
    cpSync(candidate, staged, { recursive: true });
    if (options.afterStage) options.afterStage(staged); // test-only fault injection
    assert.equal(inventory(active).sha256, prior.sha256, 'cópia anterior divergente');
    assert.equal(inventory(staged).sha256, next.sha256, 'candidato adulterado antes da troca');
    try {
      renameSync(active, saved);
      renameSync(staged, active);
      switched = true;
      assert.equal(inventory(active).sha256, next.sha256, 'slot candidato divergente');
      if (options.simulateFailure) throw new Error('SIMULATED_POST_SWITCH_FAILURE');
    } catch (error) {
      if (error.message !== 'SIMULATED_POST_SWITCH_FAILURE' || !options.simulateFailure) throw error;
      injectedFailureCaught = true;
    } finally {
      if (existsSync(saved)) {
        if (existsSync(active)) renameSync(active, retired);
        renameSync(saved, active);
      }
    }
    const restored = inventory(active);
    assert.equal(restored.sha256, prior.sha256, 'rollback não restaurou os bytes anteriores');
    assert.equal(inventory(previous).sha256, prior.sha256, 'origem anterior foi alterada');
    assert.equal(inventory(candidate).sha256, next.sha256, 'origem SSG foi alterada');
    return {
      switchedInSandbox: switched,
      simulatedPostSwitchFailure: injectedFailureCaught,
      previousSha256: prior.sha256,
      candidateSha256: next.sha256,
      restoredSha256: restored.sha256,
      filesPrevious: prior.files,
      filesCandidate: next.files,
    };
  } finally {
    rmSync(sandbox, { recursive: true, force: true });
  }
}

const here = fileURLToPath(import.meta.url);
if (process.argv[1] && resolve(process.argv[1]) === here) {
  const fixture = mkdtempSync(join(tmpdir(), 'ficha-p3a-legacy-fixture-'));
  try {
    const candidate = resolve(process.argv[2] ?? join(dirname(here), '../dist/ficha'));
    let previous = process.argv[3] && resolve(process.argv[3]);
    let expected = process.env.FICHA_PREVIOUS_TREE_SHA256;
    const mode = previous ? 'supplied-artifact' : 'synthetic-previous';
    if (!previous) {
      previous = join(fixture, 'previous');
      mkdirSync(previous);
      writeFileSync(join(previous, 'index.html'), '<!doctype html><html lang="pt-BR"><title>LEGACY FIXTURE ONLY</title></html>');
      writeFileSync(join(previous, 'manifest.json'), '{"current":"FIXTURE_ONLY","snapshots":[]}');
      expected = inventory(previous).sha256; // fixture-only, not an independent production hash
    }
    const result = rehearseRollback(previous, candidate, {
      expectedPreviousSha256: expected,
      simulateFailure: true,
    });
    console.log('SSG_ROLLBACK_REHEARSAL_PASS ' + JSON.stringify({ mode, ...result,
      noDeploy: true, productionRollbackVerified: false }));
  } catch (error) {
    console.error('SSG_ROLLBACK_REHEARSAL_FAIL ' + error.message);
    process.exitCode = 1;
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
}
