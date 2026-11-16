export function normalizeCnpj(input) {
  return String(input).toUpperCase().replace(/[^A-Z0-9]/g, '');
}
export function validateSearch(kind, input) {
  const term = String(input).trim();
  if (!['cnpj','pessoa','endereco','cnae'].includes(kind)) return {ok:false,message:'Tipo de busca desconhecido.'};
  if (!term) return {ok:false,message:'Informe um termo antes de consultar.'};
  if (kind === 'cnpj') {
    const canonical = normalizeCnpj(term);
    if (!/^[A-Z0-9]{12}[0-9]{2}$/.test(canonical)) return {ok:false,message:'CNPJ deve conter 14 caracteres. Dígito verificador não é testado nesta PoC.'};
    return {ok:true,term:canonical};
  }
  return {ok:true,term};
}
export function createLookupController(lookup) {
  let version = 0, active = null;
  function invalidate(){version += 1; active?.abort(); active = null;}
  async function submit(kind,raw) {
    invalidate();
    const id = version, input = validateSearch(kind,raw);
    if (!input.ok) return {state:'invalid',message:input.message};
    active = new AbortController();
    try {
      const found = await lookup({kind,term:input.term,signal:active.signal});
      if (id !== version) return {state:'stale'};
      if (found?.state === 'found' && typeof found.label === 'string') return {state:'found',message:found.label};
      if (found?.state === 'not-found') return {state:'not-found',message:'Ausência confirmada pela fonte.'};
      return {state:'unavailable',message:'Consulta indisponível; ausência não confirmada.'};
    } catch {
      if (id !== version) return {state:'stale'};
      return {state:'error',message:'Falha na consulta. Tente novamente mais tarde.'};
    } finally { if (id === version) active = null; }
  }
  return {invalidate,submit};
}
