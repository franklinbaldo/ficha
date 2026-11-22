import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');

test('copy pública distingue manifesto de custódia de arquivos', () => {
  const home = read('../content/index.md');
  const config = read('../config.toml');
  const acervo = read('../content/acervo.md');
  const historia = read('../content/historia.md');
  const sobre = read('../content/sobre.md');
  const readme = read('../../README.md');

  assert.doesNotMatch(home, /preservados no Internet Archive/i);
  assert.doesNotMatch(config, /preservados no Internet Archive/i);
  assert.doesNotMatch(acervo, /arquivos preservados/i);
  assert.match(home, /Destino de arquivamento/);
  assert.match(acervo, /arquivos declarados no manifesto/);
  assert.match(historia, /competências declaradas no manifesto/);
  assert.match(sobre, /organizados em competências declaradas no catálogo/);
  assert.match(sobre, /## O que o sistema organiza/);
  assert.doesNotMatch(sobre, /preservados por competência/i);
  assert.match(readme, /custódia de cada objeto exige conferência independente/);
});

test('descrição PWA gerada, config e sentinelas de build coincidem', () => {
  const home = read('../content/index.md');
  const config = read('../config.toml');
  const description = home.match(/^description: (.+)$/m)?.[1];
  assert.ok(description, 'a home precisa declarar description no frontmatter');
  const configured = config.match(/^site_description = "([^"]+)"$/m)?.[1];
  assert.equal(description, configured, 'PWA SSG usa a descrição da home');
  for (const path of [
    '../scripts/finalize.mjs',
    '../scripts/remove-ssg-manifest.mjs',
    '../scripts/finalize.test.mjs',
    './manifest.test.mjs',
  ]) {
    const script = read(path);
    assert.ok(
      script.includes(`description:'${description}'`) ||
      script.includes(`description: '${description}'`),
      path + ': sentinela PWA divergente da descrição gerada',
    );
  }
});
