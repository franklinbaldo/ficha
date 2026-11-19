import { fetchCurrentDataset, probeParquetRange } from './dataset.mjs';
import { attachAnalyticalFiles, planCompanyQuery, queryCompanies, stripCnpj } from './query-core.mjs';
import { createDuckDBEngine } from './duckdb-runtime.mjs';

/** Render only text nodes; public CNPJ/empresa data must never become HTML. */
export function renderCompanies(documentRef, list, rows) {
  list.replaceChildren();
  for (const company of rows) {
    const item = documentRef.createElement('li');
    const heading = documentRef.createElement('h3');
    heading.textContent = company.razao_social || 'Razão social não informada';
    const details = documentRef.createElement('p');
    const location = [company.municipio_nome, company.uf].filter(Boolean).join(' / ');
    details.textContent = [String(company.cnpj ?? ''), location].filter(Boolean).join(' · ');
    const canonical = stripCnpj(String(company.cnpj ?? ''));
    if (/^[A-Z0-9]{12}[0-9]{2}$/.test(canonical)) {
      const link = documentRef.createElement('a');
      link.href = '/ficha/empresa/?cnpj=' + encodeURIComponent(canonical);
      link.textContent = 'Abrir ficha';
      // Repeated result actions need an individual accessible name.
      link.setAttribute('aria-label', 'Abrir ficha do CNPJ ' + canonical);
      // A text separator keeps the CNPJ/location from touching the link label.
      details.append(' · ', link);
    }
    item.append(heading, details);
    list.append(item);
  }
}

/** No request on module import. Data and WASM load only on a valid submission. */
export function mountSearch({
  documentRef,
  fetchDataset = fetchCurrentDataset,
  probeRange = probeParquetRange,
  createEngine = createDuckDBEngine,
  attach = attachAnalyticalFiles,
  query = queryCompanies,
} = {}) {
  const form = documentRef.getElementById('busca-form');
  const input = documentRef.getElementById('busca-termo');
  const submit = documentRef.getElementById('busca-enviar');
  const status = documentRef.getElementById('consulta-estado');
  const list = documentRef.getElementById('consulta-lista');
  if (![form, input, submit, status, list].every(Boolean)) {
    throw new Error('Formulário de consulta SSG incompleto');
  }

  let sessionPromise;
  let active = false;
  let inputVersion = 0;
  let terminalFeedback = false;
  let pendingEditAnnounced = false;
  const setStatus = (message, kind = 'info') => {
    status.textContent = message;
    status.dataset.kind = kind;
  };
  // Completed feedback belongs to the previous term. During an in-flight
  // request, notify on the first edit only: repeated live-region updates
  // would announce the same message at each keystroke.
  input.addEventListener('input', () => {
    inputVersion++;
    if (active) {
      if (!pendingEditAnnounced) {
        pendingEditAnnounced = true;
        list.replaceChildren();
        setStatus('Termo alterado durante a consulta. Aguarde a conclusão para pesquisar novamente.');
      }
    } else if (terminalFeedback) {
      terminalFeedback = false;
      list.replaceChildren();
      setStatus('Termo alterado. Pesquise novamente para atualizar os resultados.');
    }
  });
  const openSession = () => {
    if (!sessionPromise) {
      sessionPromise = (async () => {
        const dataset = await fetchDataset();
        if (!dataset) {
          // A competência pode ser publicada depois: não fixar HTTP 404 na sessão.
          sessionPromise = undefined;
          return null;
        }
        // Fail before loading WASM when the archival origin cannot satisfy
        // browser CORS + byte-range reads. Never rewrite archival URLs here.
        await probeRange({url: dataset.files.cnpjs.url, size: dataset.files.cnpjs.size});
        const engine = await createEngine();
        try {
          await attach(engine.db, dataset, engine.duckdb);
        } catch (error) {
          await engine.db.terminate?.();
          throw error;
        }
        return { ...engine, dataset };
      })().catch(error => {
        sessionPromise = undefined; // permit a later retry after a network error
        throw error;
      });
    }
    return sessionPromise;
  };

  async function onSubmit(event) {
    event.preventDefault();
    if (active) return;
    const term = input.value.trim();
    if (!planCompanyQuery(term)) {
      terminalFeedback = true;
      list.replaceChildren();
      setStatus('Informe um CNPJ completo ou ao menos três caracteres da razão social.', 'error');
      return;
    }
    const requestVersion = inputVersion;
    const stillCurrent = () => requestVersion === inputVersion && input.value.trim() === term;
    const discardedMessage = 'Termo alterado durante a consulta. Pesquise novamente.';
    active = true;
    terminalFeedback = false;
    pendingEditAnnounced = false;
    submit.disabled = true;
    list.replaceChildren(); // never leave stale results visible during failures
    setStatus('Preparando consulta à competência publicada…');
    try {
      const session = await openSession();
      if (!stillCurrent()) {
        terminalFeedback = true;
        setStatus(discardedMessage);
        return;
      }
      if (!session) {
        terminalFeedback = true;
        setStatus('Os dados ainda não foram publicados (manifesto ausente).', 'error');
        return;
      }
      setStatus('Consultando a base analítica…');
      const rows = await query(session.db, term);
      if (!stillCurrent()) {
        terminalFeedback = true;
        setStatus(discardedMessage);
        return;
      }
      renderCompanies(documentRef, list, rows);
      terminalFeedback = true;
      setStatus('Competência ' + session.dataset.date + ': ' +
        (rows.length === 0 ? 'nenhum resultado encontrado.' :
          rows.length + (rows.length === 1 ? ' resultado encontrado.' : ' resultados encontrados.')));
    } catch (error) {
      terminalFeedback = true;
      if (stillCurrent()) {
        setStatus('Consulta indisponível: ' + (error?.message || 'erro desconhecido'), 'error');
      } else {
        setStatus(discardedMessage);
      }
    } finally {
      active = false;
      pendingEditAnnounced = false;
      submit.disabled = false;
    }
  }
  form.addEventListener('submit', onSubmit);
  return { onSubmit };
}
