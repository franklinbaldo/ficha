import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');

test('copy pública distingue manifesto de custódia de arquivos', () => {
  const home = read('../content/index.md');
  const config = read('../config.toml');
  const acervo = read('../content/acervo.md');
  const historia = read('../content/historia.md');
  const readme = read('../../README.md');

  assert.doesNotMatch(home, /preservados no Internet Archive/i);
  assert.doesNotMatch(config, /preservados no Internet Archive/i);
  assert.doesNotMatch(acervo, /arquivos preservados/i);
  assert.match(home, /Destino de arquivamento/);
  assert.match(acervo, /arquivos declarados no manifesto/);
  assert.match(historia, /competências declaradas no manifesto/);
  assert.match(readme, /custódia de cada objeto exige conferência independente/);
});
