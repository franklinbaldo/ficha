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

test('aviso público distingue falha de transporte de ausência cadastral', async () => {
  const home = await read('../content/index.md');
  const sobre = await read('../content/sobre.md');
  assert.match(home, /A busca por empresa ainda é experimental/);
  assert.match(home, /bloqueio CORS/);
  assert.match(home, /não há consulta remota funcional comprovada/);
  assert.match(home, /Uma falha de leitura não significa ausência da empresa/);
  assert.match(home, /\/ficha\/acervo\//);
  assert.match(sobre, /Chromium 152/);
  assert.match(sobre, /não.*a integridade do arquivo inteiro/);
});


test('botão Pesquisar tem nome acessível português explícito na fonte SSG', async () => {
  const home = await read('../content/index.md');
  assert.match(home, /<button\b[^>]*\bid="busca-enviar"[^>]*\baria-label="Pesquisar"[^>]*>Pesquisar\s*<span aria-hidden="true">↗<\/span><\/button>/);
  assert.doesNotMatch(home, /aria-label="north-east-arrow"/);
});


test('botão Abrir ficha tem nome acessível explícito na fonte SSG', async () => {
  const empresa = await read('../content/empresa.md');
  assert.match(empresa, /<button\b[^>]*\bid="empresa-enviar"[^>]*\baria-label="Abrir ficha"[^>]*>Abrir ficha<\/button>/);
  assert.doesNotMatch(empresa, /aria-label="abrir-ficha"/);
});
