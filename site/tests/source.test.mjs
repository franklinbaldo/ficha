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
test('home com formulário, módulo real e limites da busca legada', async () => {
  const home = await read('../content/index.md');
  for (const fragment of ['id="busca-form"', 'id="busca-termo"', 'id="consulta-lista"',
    'A busca por nome e a comparação histórica não foram resolvidas']) assert.ok(home.includes(fragment), fragment);
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
  assert.match(home, /Busca experimental nesta página/);
  assert.match(home, /falhou por CORS/);
  assert.match(home, /essa rota independente foi exercitada em navegador com registros reais/);
  assert.match(home, /Isso não significa ausência cadastral/);
  assert.match(home, /A busca por nome e a comparação histórica não foram resolvidas/);
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

test('páginas de ficha e história não prometem pesquisa remota já funcional', async () => {
  const empresa = await read('../content/empresa.md');
  const historia = await read('../content/historia.md');
  assert.match(empresa, /consulta remota ainda é experimental/i);
  assert.match(empresa, /^description: .*experimental\./m);
  assert.match(empresa, /erro de leitura não significa que a empresa esteja ausente/i);
  assert.match(empresa, /Parquet sujeito a CORS/);
  assert.match(historia, /leitura remota ainda experimental/i);
  assert.match(historia, /^description: .*experimental\./m);
  assert.match(historia, /Isso não demonstra ausência de registros/);
  assert.match(historia, /diferenças entre arquivos não determinam a data exata/);
  for (const page of [empresa, historia]) {
    assert.match(page, /href="\/ficha\/acervo\/"/);
    assert.match(page, /https:\/\/github\.com\/franklinbaldo\/ficha\/issues\/208/);
  }
});
