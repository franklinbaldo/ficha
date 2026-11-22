import test from 'node:test';
import assert from 'node:assert/strict';
import {fetchManifest} from '../static/js/catalog.mjs';

const manifest = {
  current: '2026-07',
  snapshots: [{date: '2026-07', files: {}}],
};

test('manifesto sem Range só aceita HTTP 200 integral', async () => {
  const calls = [];
  let decoded = 0;
  const fetchImpl = async (url, options) => {
    calls.push([url, options]);
    return {status: 200, ok: true, json: async () => { decoded++; return manifest; }};
  };
  assert.deepEqual(await fetchManifest({fetchImpl}), manifest);
  assert.deepEqual(calls, [['/ficha/manifest.json', {cache: 'no-store'}]]);
  assert.equal(decoded, 1);
});

for (const status of [201, 206]) {
  test('HTTP ' + status + ' com JSON válido não pode virar manifesto integral', async () => {
    let decoded = 0;
    await assert.rejects(fetchManifest({fetchImpl: async () => ({
      status, ok: true,
      json: async () => { decoded++; return manifest; },
    })}), new RegExp('HTTP ' + status));
    assert.equal(decoded, 0, 'a resposta inadequada deve falhar antes do parser');
  });
}

test('HTTP 200 com Content-Range falha antes de decodificar JSON válido', async () => {
  let decoded = 0;
  await assert.rejects(fetchManifest({fetchImpl: async () => ({
    status: 200,
    headers: {get: name => name.toLowerCase() === 'content-range' ? 'bytes 0-19/100' : null},
    json: async () => {decoded++; return manifest;},
  })}), /Content-Range inesperado/);
  assert.equal(decoded, 0, 'resposta contraditória não pode ser aceita como catálogo');
});

test('HTTP 404 de manifesto continua sendo indisponibilidade, não catálogo vazio', async () => {
  await assert.rejects(fetchManifest({fetchImpl: async () => ({
    status: 404, ok: false, json: async () => manifest,
  })}), /HTTP 404/);
});
