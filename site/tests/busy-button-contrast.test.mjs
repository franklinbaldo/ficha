import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

// This busy button stays keyboard-focusable with aria-disabled. Although inactive
// controls are exempt from WCAG text contrast, keep its label legible while users
// wait for an unpredictable network request.
const css = readFileSync(new URL('../static/css/site.css', import.meta.url), 'utf8');
const block = pattern => {
  const body = css.match(pattern)?.[1];
  assert.ok(body, 'regra CSS de estado ocupado obrigatória');
  return body;
};
const prop = (body, name) => body.match(new RegExp('(?:^|;)\\s*' + name + '\\s*:\\s*([^;]+)'))?.[1]?.trim();
const root = block(/:root\s*\{([^}]+)\}/);
const busy = block(/button\[aria-disabled="true"\]\s*\{([^}]+)\}/);
const base = block(/\.search-controls button,\.button-primary\s*\{([^}]+)\}/);
function parseHex(hex) {
  assert.match(hex, /^#[\da-f]{3}(?:[\da-f]{3})?$/i);
  const v = hex.slice(1);
  const wide = v.length === 3 ? [...v].map(c => c + c).join('') : v;
  return [0, 2, 4].map(i => parseInt(wide.slice(i, i + 2), 16) / 255);
}
function luminance(hex) {
  const [r, g, b] = parseHex(hex).map(v => v <= .04045 ? v / 12.92 : ((v + .055) / 1.055) ** 2.4);
  return .2126 * r + .7152 * g + .0722 * b;
}
const ratio = (a, b) => {
  const [bright, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (bright + .05) / (dark + .05);
};
test('botão de busca ocupado preserva texto legível e não reduz opacidade', () => {
  assert.equal(prop(busy, 'opacity'), '1');
  const bgToken = prop(busy, 'background')?.match(/^var\((--[\w-]+)\)$/)?.[1];
  assert.ok(bgToken, 'cor de estado ocupado deve referenciar token do tema');
  const bgColor = prop(root, bgToken);
  const fgColor = prop(base, 'color');
  assert.ok(ratio(fgColor, bgColor) >= 4.5,
    'contraste da legenda ocupado = ' + ratio(fgColor, bgColor).toFixed(2) + ':1');
});
