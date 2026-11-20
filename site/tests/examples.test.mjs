import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { mountExamplePills, pickExamplePills } from '../static/js/examples.mjs';

function cnpjValid(value) {
  if (!/^\\d{14}$/.test(value)) return false;
  const digits = [...value].map(Number);
  const digit = (size, weights) => {
    const remainder = digits.slice(0, size).reduce((sum, number, index) =>
      sum + number * weights[index], 0) % 11;
    return remainder < 2 ? 0 : 11 - remainder;
  };
  return digit(12, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]) === digits[12] &&
    digit(13, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]) === digits[13];
}

test('sugestões são CNPJs completos, distintos, com DV válido e link direto à ficha', async () => {
  const home = await readFile(new URL('../content/index.md', import.meta.url), 'utf8');
  const links = [...home.matchAll(/<a class="example-pill" data-cnpj-example href="\/ficha\/empresa\/\?cnpj=(\d{14})">/g)]
    .map(match => match[1]);
  assert.equal(links.length, 4);
  assert.equal(new Set(links).size, 4);
  assert.ok(links.every(cnpjValid));
  assert.match(home, /id="example-pills"/);
  assert.match(home, /Exemplos públicos: a ficha abre por Protobuf/);
  assert.ok(home.indexOf('id="busca-form"') < home.indexOf('id="example-pills"'));
  assert.ok(home.indexOf('id="example-pills"') < home.indexOf('class="hero-bottom"'));
});

test('pills alternam sem duplicar CNPJ e limitam a três escolhas', () => {
  const items = ['BB', 'Petrobras', 'Caixa', 'Correios'];
  const first = pickExamplePills(items, () => 0);
  const last = pickExamplePills(items, () => .999999);
  assert.equal(first.length, 3);
  assert.equal(last.length, 3);
  assert.equal(new Set(first).size, 3);
  assert.equal(new Set(last).size, 3);
  assert.notDeepEqual(first, last);
  assert.deepEqual(items, ['BB', 'Petrobras', 'Caixa', 'Correios']);
});

test('montagem só rearranja links locais e mantém href de ficha', () => {
  const links = [0, 1, 2, 3].map(index => ({ href: '/ficha/empresa/?cnpj=' + index }));
  const container = {
    items: links,
    querySelectorAll() { return this.items; },
    replaceChildren(...items) { this.items = items; },
  };
  mountExamplePills({ getElementById: id => id === 'example-pills' ? container : null }, () => 0);
  assert.equal(container.items.length, 3);
  assert.ok(container.items.every(item => item.href.startsWith('/ficha/empresa/')));
  assert.equal(new Set(container.items).size, 3);
  assert.doesNotThrow(() => mountExamplePills({ getElementById: () => null }));
});
