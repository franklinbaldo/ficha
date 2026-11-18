import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8');
test('prefixo, rotas e versão explícitos', async () => {
  const config = await read('../config.toml');
  assert.match(config, /base_url = "https:\/\/franklinbaldo\.github\.io\/ficha\/"/);
  assert.match(config, /output_dir = "dist\/ficha"/);
  assert.match(await read('../content/sobre.md'), /\/ficha\/sobre\//);
});
test('sem busca fake, astro, svelte ou views no esqueleto', async () => {
  const home = await read('../content/index.md');
  assert.match(home, /ainda não executa consultas por CNPJ/);
  for (const p of ['../templates/template.html', '../templates/index.html', '../templates/page.html', '../templates/post.html']) {
    const html = await read(p);
    assert.match(html, /\{\{!content\}\}/);
    assert.doesNotMatch(html, /<form|<script|astro-island|svelte|\/views\//i);
  }
});
test('sobre informa camadas sem prometer disponibilidade live', async () => {
  const about = await read('../content/sobre.md');
  assert.match(about, /DuckDB-WASM/);
  assert.match(about, /Protobuf\/ZIP/);
  assert.match(about, /não é prova de acesso remoto/);
});
