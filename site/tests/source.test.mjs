import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const read = path => readFile(new URL(path, import.meta.url), 'utf8');
test('prefixo, rotas e versão explícitos', async () => {
  const config = await read('../config.toml');
  assert.match(config, /base_url = "https:\/\/franklinbaldo\.github\.io\/ficha\/"/);
  assert.match(config, /output_dir = "dist\/ficha"/);
  assert.match(await read('../content/sobre.md'), /\/ficha\/sobre\//);
});
test('home com formulário, módulo real e aviso de paridade pendente', async () => {
  const home = await read('../content/index.md');
  for (const fragment of ['id="busca-form"', 'id="busca-termo"', 'id="consulta-lista"',
    'não passou pelos testes de paridade']) assert.ok(home.includes(fragment), fragment);
  const template = await read('../templates/index.html');
  assert.match(template, /type="module" src="\/ficha\/js\/boot\.mjs"/);
  assert.match(template, /\{\{!content\}\}/);
  for (const p of ['../templates/template.html', '../templates/page.html', '../templates/post.html']) {
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
