import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

// finalize.mjs copies this canonical file byte-for-byte to /ficha/manifest.json.
// Audit the served catalogue source, not an old Astro output or synthetic fixture.
const canonical = new URL('../../web/public/manifest.json', import.meta.url);
const load = () => JSON.parse(readFileSync(canonical, 'utf8'));

function assertArchiveBindings(manifest) {
  if (!Array.isArray(manifest?.snapshots)) throw Error('snapshots invalid');
  const dates = new Set();
  let checked = 0;
  for (const snapshot of manifest.snapshots) {
    if (dates.has(snapshot.date)) throw Error('duplicate snapshot date');
    dates.add(snapshot.date);
    const shards = snapshot.files?.companies?.shards;
    if (shards === undefined) continue; // A month may lack the atomic layer.
    if (!Array.isArray(shards) || shards.length !== 100) {
      throw Error('incomplete shard list for ' + snapshot.date);
    }
    const slots = new Set();
    for (const entry of shards) {
      if (!/^\d{2}$/.test(entry?.shard ?? '') || slots.has(entry.shard)) {
        throw Error('duplicate or invalid shard slot');
      }
      slots.add(entry.shard);
      const expected = 'https://archive.org/download/ficha-' +
        snapshot.date + '/companies-' + entry.shard + '.zip';
      if (entry.url !== expected) {
        throw Error('archive mismatch for ' + snapshot.date + '/' + entry.shard);
      }
      checked++;
    }
    for (let i = 0; i < 100; i++) {
      if (!slots.has(String(i).padStart(2, '0'))) throw Error('missing shard slot');
    }
  }
  if (!dates.has(manifest.current)) throw Error('current snapshot missing');
  return checked;
}

function july(catalog) {
  return catalog.snapshots.find(s => s.date === '2026-07').files.companies.shards;
}

test('SSG catalogue source has all 300 month/shard/IA bindings', () => {
  const catalog = load();
  assert.ok(assertArchiveBindings(catalog) >= 300);
  for (const date of ['2026-05', '2026-06', '2026-07']) {
    assert.equal(catalog.snapshots.find(s => s.date === date).files.companies.shards.length, 100);
  }
});

test('swapping distinct, otherwise unique IA URLs cannot change shard identity', () => {
  const catalog = load();
  const shards = july(catalog);
  [shards[0].url, shards[1].url] = [shards[1].url, shards[0].url];
  assert.equal(new Set(shards.map(s => s.url)).size, 100);
  assert.throws(() => assertArchiveBindings(catalog), /archive mismatch/);
});

test('a URL for another competence cannot masquerade as a current shard', () => {
  const catalog = load();
  july(catalog)[37].url = 'https://archive.org/download/ficha-2026-06/companies-37.zip';
  assert.throws(() => assertArchiveBindings(catalog), /2026-07\/37/);
});

test('query string and empty question mark change archival identity', () => {
  for (const query of ['?download=1', '?']) {
    const catalog = load();
    july(catalog)[0].url += query;
    assert.throws(() => assertArchiveBindings(catalog), /2026-07\/00/);
  }
});

test('a missing or duplicate shard cannot certify completeness', () => {
  const missing = load();
  july(missing).pop();
  assert.throws(() => assertArchiveBindings(missing), /incomplete shard list/);
  const duplicate = load();
  july(duplicate)[1].shard = '00';
  assert.throws(() => assertArchiveBindings(duplicate), /duplicate or invalid shard slot/);
});
