/** Shared DOM/selection helpers for SSG product pages. */
import {competenceLabel,safeExternalURL,snapshotsDescending} from './catalog.mjs';

export const BASE = '/ficha/';
export const byId = (doc, id) => doc.getElementById(id);
export function create(doc, tag, text = '', className = '') {
  const node = doc.createElement(tag);
  if (text !== '') node.textContent = text;
  if (className) node.className = className;
  return node;
}
export function add(doc, parent, tag, text = '', className = '') {
  const child = create(doc, tag, text, className);
  parent.append(child);
  return child;
}
export function anchor(doc, parent, label, url, external = false) {
  const safe = external ? safeExternalURL(url) : url;
  if (!safe) return add(doc, parent, 'span', 'Link não declarado');
  const link = add(doc, parent, 'a', label);
  link.href = safe;
  if (external) { link.target = '_blank'; link.rel = 'noopener noreferrer'; }
  return link;
}
export function option(doc, select, value, label) {
  const item = create(doc,'option',label);
  item.value = value;
  select.append(item);
}
export function status(node, message, error = false) {
  node.textContent = message;
  node.dataset.kind = error ? 'error' : 'info';
}
export function setParams(params) {
  const url = new URL(globalThis.location.href);
  for (const [key, val] of Object.entries(params)) {
    if (val === null || val === '') url.searchParams.delete(key);
    else url.searchParams.set(key, val);
  }
  globalThis.history.replaceState(null, '', url);
}
export function params() { return new URLSearchParams(globalThis.location.search); }
/** Revision guard: a selection changed A→B→A is still a new request. */
export function watchSelection(controls, state, output, pendingMessage, queryKeys) {
  let revision = 0;
  let pending = false;
  let terminal = false;
  let announced = false;
  const edited = () => {
    revision++;
    // An edited selection must not leave a shareable URL for the prior result.
    if (queryKeys.some(key => params().has(key))) {
      setParams(Object.fromEntries(queryKeys.map(key => [key, null])));
    }
    if (pending) {
      if (!announced) {
        announced = true;
        output.replaceChildren();
        status(state, pendingMessage);
      }
    } else if (terminal) {
      terminal = false;
      output.replaceChildren();
      status(state, 'Seleção alterada. Pesquise novamente.');
    }
  };
  for (const [node, event] of controls) node.addEventListener(event, edited);
  return {
    begin() { pending = true; terminal = false; announced = false; return revision; },
    current(version) { return revision === version; },
    finish() { pending = false; terminal = true; announced = false; },
    markTerminal() { terminal = true; },
  };
}

export function populateDateSelect(doc, select, manifest, selected) {
  select.replaceChildren();
  for (const snap of snapshotsDescending(manifest)) {
    option(doc, select, snap.date,
      competenceLabel(snap.date) + ' · ' + snap.date + (snap.date === manifest.current ? ' · referência' : ''));
  }
  if ([...select.options].some(o => o.value === selected)) select.value = selected;
}
