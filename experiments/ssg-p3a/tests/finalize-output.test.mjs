import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { finalizeOutput } from '../scripts/finalize-output.mjs';

const stub = { display:'standalone', name:'Static Site', short_name:'Site', start_url:'/' };
const widget = '<!-- SSG Search Widget --><div id="ssg-search-widget"><div id="ssg-search-overlay"></div><script src="/ficha/_csp/abc123.js"></script></div>';
const html = '<!doctype html><html><head><link rel="stylesheet" href="/ficha/_csp/def456.css"></head><body><main id="main-content"><form id="search-form"></form></main>' + widget + '</body></html>';

function fixture(edit = () => {}) {
  const root = mkdtempSync(join(tmpdir(), 'ficha-ssg-finalize-'));
  const out = join(root, 'dist/ficha');
  mkdirSync(join(out, '_csp'), { recursive: true });
  writeFileSync(join(out, 'index.html'), html);
  writeFileSync(join(out, 'manifest.json'), JSON.stringify(stub));
  writeFileSync(join(out, '_csp/abc123.js'), 'const x = "ssg-search-overlay"');
  writeFileSync(join(out, '_csp/def456.css'), '#ssg-search-btn { display: flex }');
  edit(out);
  return { root, out };
}
function run(edit, verify) {
  const {root, out} = fixture(edit);
  try { verify(root, out); } finally { rmSync(root, {recursive:true, force:true}); }
}
test('remove only SSG widget and isolate fake manifest', () => run(() => {}, (root,out) => {
  assert.deepEqual(finalizeOutput(root), {widgetRemoved:true, manifestIsolated:true});
  assert.ok(!existsSync(join(out,'manifest.json')));
  assert.ok(existsSync(join(out,'ssg-generated-webmanifest.json')));
  assert.doesNotMatch(readFileSync(join(out,'index.html'),'utf8'), /ssg-search/);
  assert.match(readFileSync(join(out,'index.html'),'utf8'), /search-form/);
  assert.deepEqual(finalizeOutput(root), {widgetRemoved:false, manifestIsolated:true});
}));
test('negative: never touch a true RFB manifest', () => run(out => {
  writeFileSync(join(out,'manifest.json'), JSON.stringify({current:'2026-07',snapshots:[]}));
}, (root,out) => {
  assert.throws(() => finalizeOutput(root), /manifesto RFB/);
  assert.ok(existsSync(join(out,'manifest.json')));
  assert.match(readFileSync(join(out,'index.html'),'utf8'), /ssg-search-widget/);
}));
test('negative: unknown widget structure aborts unchanged', () => run(out => {
  writeFileSync(join(out,'index.html'), html.replace('id="ssg-search-overlay"', 'id="other-dialog"'));
}, (root,out) => {
  assert.throws(() => finalizeOutput(root), /estrutura do widget/);
  assert.ok(existsSync(join(out,'manifest.json')));
  assert.match(readFileSync(join(out,'index.html'),'utf8'), /ssg-search-widget/);
}));
test('negative: missing generated widget CSS aborts unchanged', () => run(out => {
  rmSync(join(out,'_csp/def456.css'));
}, (root,out) => {
  assert.throws(() => finalizeOutput(root));
  assert.ok(existsSync(join(out,'manifest.json')));
}));
test('negative: unknown generated web manifest aborts unchanged', () => run(out => {
  writeFileSync(join(out,'manifest.json'), JSON.stringify({...stub, name:'Another App'}));
}, (root,out) => {
  assert.throws(() => finalizeOutput(root), /manifesto SSG desconhecido/);
  assert.match(readFileSync(join(out,'index.html'),'utf8'), /ssg-search-widget/);
}));
