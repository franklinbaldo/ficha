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
    'id="busca-ajuda"']) assert.ok(home.includes(fragment), fragment);
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
  assert.match(home, /A busca por nome é experimental/);
  assert.match(home, /Um CNPJ numérico completo abre a ficha por Protobuf/);
  assert.match(home, /A busca por nome e por CNPJ alfanumérico usa Parquet e pode falhar por CORS/);
  assert.match(home, /Em caso de erro de rede, a presença do registro continua indeterminada/);
  assert.match(home, /\/ficha\/acervo\//);
  assert.match(sobre, /Chromium 152/);
  assert.match(sobre, /não.*a integridade do arquivo inteiro/);
});


test('botão Pesquisar tem nome acessível português explícito na fonte SSG', async () => {
  const home = await read('../content/index.md');
  assert.match(home, /<button\b[^>]*\bid="busca-enviar"[^>]*\baria-label="Pesquisar"[^>]*>Pesquisar<\/button>/);
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
  assert.match(historia, /comparação de CNPJ numérico.*Protobuf v1/i);
  assert.match(historia, /CNPJs alfanuméricos e meses sem essa camada/i);
  assert.match(historia, /a comparação legada por Parquet não é acionada nesta página/i);
  assert.match(historia, /não há fallback para Parquet/i);
  assert.match(historia, /^description: .*experimental\./m);
  assert.match(historia, /Isso não demonstra ausência de registros/);
  assert.match(historia, /diferenças entre arquivos não determinam a data exata/i);
  for (const page of [empresa, historia]) {
    assert.match(page, /href="\/ficha\/acervo\/"/);
    assert.match(page, /https:\/\/github\.com\/franklinbaldo\/ficha\/issues\/208/);
  }
});


test('História Protobuf publicada: aviso da ficha e teclado móvel não induzem a rota Parquet', async () => {
  const empresa = await read('../content/empresa.md');
  const historia = await read('../content/historia.md');
  assert.match(empresa, /comparação de CNPJ numérico nas competências Protobuf v1 usa registros menores/);
  assert.match(empresa, /A busca por nome ainda usa Parquet sujeito a CORS/);
  assert.doesNotMatch(empresa, /a comparação histórica ainda dependem do Parquet/i);
  assert.match(historia, /<label for="historia-cnpj">CNPJ numérico completo \(14 dígitos\)<\/label>/);
  assert.match(historia, /<input\b[^>]*id="historia-cnpj"[^>]*\binputmode="numeric"[^>]*>/);
});

test('avisos públicos não insinuam fallback alfanumérico após a História numérica #379', async () => {
  const [home, empresa, historia] = await Promise.all([
    read('../content/index.md'),
    read('../content/empresa.md'),
    read('../content/historia.md'),
  ]);
  assert.match(home, /História compara CNPJs numéricos entre competências com Protobuf v1/);
  assert.match(home, /CNPJs alfanuméricos e competências sem Protobuf v1 compatível não podem ser comparados/);
  assert.match(home, /não há fallback automático para Parquet/);
  assert.match(empresa, /CNPJs alfanuméricos e competências sem essa camada não são comparáveis pela página História/);
  assert.match(empresa, /Não há fallback para Parquet/);
  assert.match(empresa, /a comparação alfanumérica ainda não está disponível/);
  assert.match(historia, /CNPJs alfanuméricos e meses sem essa camada não são comparáveis por esta interface/);
  assert.match(historia, /não há fallback para Parquet/);
});
