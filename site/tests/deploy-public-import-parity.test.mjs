import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import test from 'node:test';

const deploy = readFileSync(new URL('../../.github/workflows/deploy.yml', import.meta.url), 'utf8');

test('SSG build and published Pages verify preserve all unbundled company/history imports', () => {
  const buildModules = deploy.match(/for module in ([^\n]+); do/)?.[1]?.split(/\s+/) ?? [];
  for (const module of ['pages', 'history', 'page-helpers']) {
    assert.ok(buildModules.includes(module),
      module + ' must be copied byte-for-byte by the SSG build check');
    assert.ok(deploy.includes('curl -fsSL --retry 3 "$BASE/js/' + module + '.mjs" -o ' + module + '.mjs'),
      module + ' must be fetched from deployed Pages');
    assert.ok(deploy.includes('cmp site/static/js/' + module + '.mjs ' + module + '.mjs'),
      module + ' must be compared with the checkout at the deployed SHA');
  }
});
