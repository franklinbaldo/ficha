import test from 'node:test';
import assert from 'node:assert/strict';
import { renderCountSeries, renderExploreAssets } from '../static/js/pages.mjs';

class FakeElement {
  constructor(tag = 'div') {
    this.tag = tag;
    this.children = [];
    this.textContent = '';
    this.style = {};
    this.attributes = {};
  }
  append(node) { this.children.push(node); }
  replaceChildren() { this.children = []; }
  setAttribute(key, value) { this.attributes[key] = value; }
}
const doc = { createElement: tag => new FakeElement(tag) };

test('contagem zero é desenhada com largura zero, não como 1% positivo', () => {
  const manifest = { current: '2026-07', snapshots: [
    {date: '2026-06', files: {}, row_counts: {cnpjs: 0}},
    {date: '2026-07', files: {}, row_counts: {cnpjs: 100}},
  ] };
  const parent = new FakeElement();
  renderCountSeries(doc, parent, manifest, 'cnpjs');
  const bars = parent.children.map(row => row.children[1].children[0].style.width);
  assert.deepEqual(bars, ['0%', '100%']);
  assert.equal(parent.children[0].children[0].children[1].textContent, '0 linhas');
});

test('arquivo vazio e tamanho ausente não recebem barra positiva', () => {
  const manifest = { current: '2026-07', snapshots: [{
    date: '2026-07', files: {
      a_zero: {size: 0, url: 'https://archive.org/a'},
      b_unknown: {url: 'https://archive.org/b'},
      c_nonzero: {size: 200, url: 'https://archive.org/c'},
    },
  }] };
  const parent = new FakeElement();
  renderExploreAssets(doc, parent, manifest, '2026-07');
  const rows = parent.children.slice(1);
  assert.deepEqual(rows.map(row => row.children[1].children[0].style.width),
    ['0%', '0%', '100%']);
  assert.equal(rows[0].children[0].children[1].textContent, '0 B');
  assert.equal(rows[1].children[0].children[1].textContent, 'Tamanho não declarado');
});
