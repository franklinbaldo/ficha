import test from 'node:test';
import assert from 'node:assert/strict';
import { renderArchiveSelection } from '../static/js/pages.mjs';

class Element {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.textContent = '';
  }
  append(child) { this.children.push(child); }
  replaceChildren() { this.children = []; }
}
const documentRef = {createElement: tag => new Element(tag)};
const descendants = node => [node, ...node.children.flatMap(descendants)];

test('Acervo chama URL do Internet Archive de arquivo indicado no catálogo, não origem institucional', () => {
  const url = 'https://archive.org/download/ficha-2026-07/companies-00.zip';
  const manifest = { current: '2026-07', snapshots: [{
    date: '2026-07',
    files: {
      companies: {shards: [{shard: '00', url, size: 123}]},
      cnpjs: {url: 'javascript:alert(1)', size: 5},
    },
  }] };
  const root = new Element('section');
  renderArchiveSelection(documentRef, root, manifest, '2026-07');
  const nodes = descendants(root);
  const links = nodes.filter(node => node.tag === 'a');
  assert.equal(links.length, 1, 'a URL insegura não deve virar link');
  assert.equal(links[0].textContent, 'Abrir arquivo indicado no catálogo');
  assert.equal(links[0].href, url);
  assert.equal(links[0].target, '_blank');
  assert.equal(links[0].rel, 'noopener noreferrer');
  assert.ok(nodes.some(node => node.tag === 'span' && node.textContent === 'Link não declarado'));
  assert.ok(nodes.every(node => !/repositório de origem/i.test(node.textContent)));
});
