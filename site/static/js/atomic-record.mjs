/** Protobuf-first record boundary for the numeric v1 archival layer.
 * No network is attempted until a valid, manifest-declared target is selected.
 * The caller supplies the canonical generated protobuf decoder, bundled locally.
 */
export const MAX_COMPANY_BYTES = 32 * 1024 * 1024;

const MONTH = /^\d{4}-(0[1-9]|1[0-2])$/;
const NUMERIC = /^\d{14}$/;
const VALID_CNPJ = /^[A-Z0-9]{12}\d{2}$/;

/** A rejected CNPJ is a local input error, never a remote outage. */
export class CnpjInputError extends Error {
  constructor(message) { super(message); this.name = 'CnpjInputError'; }
}

/** Check digits for numeric CNPJ (RFB módulo 11); reject repetitive placeholders. */
export function validNumericCnpj(cnpj) {
  if (!/^\d{14}$/.test(cnpj) || /^(\d)\1{13}$/.test(cnpj)) return false;
  const digits = [...cnpj].map(Number);
  const calculate = weights => {
    const sum = weights.reduce((acc, weight, i) => acc + digits[i] * weight, 0);
    const remainder = sum % 11;
    return remainder < 2 ? 0 : 11 - remainder;
  };
  return digits[12] === calculate([5,4,3,2,9,8,7,6,5,4,3,2]) &&
    digits[13] === calculate([6,5,4,3,2,9,8,7,6,5,4,3,2]);
}

export function planAtomicRecord(manifest, date, value) {
  const cnpj = String(value ?? '').trim().replace(/[.\/\-\s]/g, '').toUpperCase();
  if (!VALID_CNPJ.test(cnpj)) throw new CnpjInputError('Informe um CNPJ completo de 14 caracteres');
  if (!NUMERIC.test(cnpj)) throw new CnpjInputError('CNPJ alfanumérico exige índice Protobuf 1.1 explícito');
  if (!validNumericCnpj(cnpj)) throw new CnpjInputError('CNPJ inválido: confira os dígitos verificadores');
  if (!MONTH.test(date) || !Array.isArray(manifest?.snapshots)) {
    throw new Error('Competência inválida ou manifesto ausente');
  }
  const snapshots = manifest.snapshots;
  const dates = snapshots.map(snapshot => snapshot?.date);
  if (new Set(dates).size !== dates.length) throw new Error('Competência duplicada no manifesto');
  const found = snapshots.filter(snapshot => snapshot?.date === date);
  if (found.length !== 1) throw new Error('Competência não publicada');
  const layer = found[0].files?.companies;
  if (layer?.shard_by !== 'cnpj_base_prefix_2' ||
      (layer.schema_version != null && layer.schema_version !== '1.0.0') ||
      !Array.isArray(layer.shards)) {
    throw new Error('Camada Protobuf numérica v1 não declarada nesta competência');
  }
  const root = cnpj.slice(0, 8);
  const shard = root.slice(0, 2);
  const archiveUrl = `https://archive.org/download/ficha-${date}/companies-${shard}.zip`;
  const candidates = layer.shards.filter(entry => entry?.shard === shard);
  if (candidates.length !== 1 || candidates[0].url !== archiveUrl) {
    throw new Error('Shard ausente, duplicado ou URL divergente da competência');
  }
  const member = `${root.slice(0, 2)}/${root.slice(2, 5)}/${root.slice(5)}.pb`;
  return Object.freeze({cnpj, date, root, archiveUrl, memberUrl: `${archiveUrl}/${member}`, wire: '1.0.0'});
}

/** Stream into bounded memory. Never call unbounded response.arrayBuffer(). */
export async function readBoundedMember(response, limit = MAX_COMPANY_BYTES) {
  if (!Number.isSafeInteger(limit) || limit <= 0) throw new Error('Orçamento de leitura inválido');
  const header = response.headers?.get?.('content-length');
  const encoding = response.headers?.get?.('content-encoding');
  if (header != null && /^\d+$/.test(header) && Number(header) > limit) {
    // Early rejection must also stop a pending response body (no chunk is read).
    // Do not await transport teardown before returning the size error.
    try { void response.body?.cancel?.().catch(() => {}); } catch { /* Already unusable. */ }
    throw new Error('Membro Protobuf excede orçamento de leitura');
  }
  if (!response.body?.getReader) throw new Error('Resposta sem stream binário');
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      if (!(part.value instanceof Uint8Array)) throw new Error('Chunk Protobuf não binário');
      total += part.value.byteLength;
      if (total > limit) throw new Error('Membro Protobuf excede orçamento de leitura');
      // Readers may reuse and mutate a Uint8Array on the next pull. Preserve
      // the chunk bytes immediately, before awaiting the following read.
      chunks.push(new Uint8Array(part.value));
    }
  } catch (error) {
    // A transport may never settle its cancellation promise. Keep the original
    // byte-budget error observable; cleanup is deliberately best-effort.
    try { void reader.cancel().catch(() => {}); } catch { /* Already unusable. */ }
    throw error;
  } finally {
    reader.releaseLock();
  }
  // Fetch normally decodes Content-Encoding before exposing the body stream;
  // Content-Length then measures encoded bytes and cannot certify its length.
  // For identity responses, detect a short or overlong body before decoding PB.
  if (header != null && /^\d+$/.test(header) &&
      (!encoding || encoding.trim().toLowerCase() === 'identity') &&
      total !== Number(header)) {
    throw new Error('Content-Length incompatível com o membro Protobuf recebido');
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}

function numericDate(value) {
  if (!Number.isSafeInteger(value) || value === 0) return null;
  const s = String(value);
  if (!/^\d{8}$/.test(s)) throw new Error('Data numérica do Protobuf inválida');
  const y = Number(s.slice(0, 4)), m = Number(s.slice(4, 6)), d = Number(s.slice(6, 8));
  const date = new Date(Date.UTC(y, m - 1, d));
  if (date.getUTCFullYear() !== y || date.getUTCMonth() + 1 !== m || date.getUTCDate() !== d) {
    throw new Error('Data impossível no Protobuf');
  }
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6)}`;
}

/** Decode only with the generated canonical decoder supplied by the caller.
 * Proto v1 collapses false/unknown Simples-MEI and has a legacy porte enum bug:
 * don't infer a negative answer or label from these bytes.
 */
export async function fetchAtomicRecord(manifest, date, input, {
  fetchImpl = fetch, decodeCompany, signal,
} = {}) {
  const target = planAtomicRecord(manifest, date, input);
  if (typeof decodeCompany !== 'function') throw new Error('Decoder Protobuf canônico obrigatório');
  const response = await fetchImpl(target.memberUrl, {mode:'cors', signal});
  const shared = {cnpj:target.cnpj, date:target.date, source:target.memberUrl,
    archiveUrl:target.archiveUrl, wire:target.wire, partial:true};
  // A 404 can mean a missing ZIP, shard or member; it cannot prove CNPJ absence.
  if (response.status === 404) return {...shared, data:null,
    state:'artifact-unavailable', httpStatus:404};
  // A non-Range GET must receive the complete member, not a successful
  // partial response (206), which can still contain decodable Protobuf.
  if (response.status !== 200) throw new Error(`Consulta Protobuf indisponível: HTTP ${response.status}`);
  // HTTP 200 from a non-Range GET cannot have partial-content semantics.
  // Reject an unexpected Content-Range even if the body is decodable and
  // its Content-Length matches, before interpreting any CNPJ as found/absent.
  if (response.headers?.get?.('content-range') != null) {
    try { void response.body?.cancel?.().catch(() => {}); } catch { /* Best-effort teardown. */ }
    throw new Error('Resposta Protobuf HTTP 200 com Content-Range inesperado');
  }
  const bytes = await readBoundedMember(response);
  let company;
  try { company = decodeCompany(bytes); }
  catch { throw new Error('Resposta Protobuf inválida ou incompatível'); }
  if (!company || company.cnpj_base_text ||
      String(company.cnpj_base ?? 0).padStart(8, '0') !== target.root) {
    throw new Error('Raiz Protobuf divergente ou wire textual incompatível');
  }
  if (company.snapshot_yyyymm !== Number(date.replace('-', ''))) {
    throw new Error('Competência divergente ou ausente no Protobuf');
  }
  const establishments = company.estabelecimentos;
  if (!Array.isArray(establishments)) throw new Error('Lista de estabelecimentos inválida');
  // pack.py serializes the raizes aggregate derived by COUNT from cnpjs.
  // A decoded root with fewer members cannot certify an establishment's absence.
  if (!Object.hasOwn(company, 'qtd_estabelecimentos') ||
      !Number.isSafeInteger(company.qtd_estabelecimentos) ||
      company.qtd_estabelecimentos !== establishments.length) {
    throw new Error('Quantidade de estabelecimentos inconsistente ou não comprovada no Protobuf');
  }
  if (establishments.some(est => !est || !Number.isSafeInteger(est.cnpj_ordem) ||
      est.cnpj_ordem < 0 || est.cnpj_ordem > 9999 ||
      !Number.isSafeInteger(est.cnpj_dv) || est.cnpj_dv < 0 || est.cnpj_dv > 99)) {
    throw new Error('Identificador de estabelecimento inválido no Protobuf');
  }
  if (establishments.some(est => est?.cnpj_ordem_text) ||
      company.socios?.some(socio => socio?.cnpj_socio_text)) {
    throw new Error('Wire textual 1.1 indevido no índice numérico v1');
  }
  // A count matching list length is not sufficient if two rows have the
  // same establishment ID. Reject an inconsistent root before claiming absence
  // of another CNPJ inside it.
  const seen = new Set();
  for (const est of establishments) {
    const id = String(est.cnpj_ordem).padStart(4, '0') +
      String(est.cnpj_dv).padStart(2, '0');
    if (!validNumericCnpj(target.root + id)) {
      throw new Error('Dígitos verificadores de filial inválidos no Protobuf');
    }
    if (seen.has(id)) throw new Error('Estabelecimento duplicado no agregado Protobuf');
    seen.add(id);
  }
  const exact = establishments.filter(est => est &&
    target.root + String(est.cnpj_ordem ?? 0).padStart(4, '0') +
    String(est.cnpj_dv ?? 0).padStart(2, '0') === target.cnpj);
  if (exact.length > 1) throw new Error('CNPJ duplicado dentro da raiz Protobuf');
  if (exact.length === 0) return {...shared, data:null, state:'absent-in-snapshot'};
  const est = exact[0];
  return {...shared, state:'found', data:{
    cnpj:target.cnpj,
    razao_social:company.razao_social || null,
    nome_fantasia:est.nome_fantasia || null,
    uf:est.uf || null,
    municipio_codigo:est.municipio_codigo || null,
    cnae_principal_codigo:est.cnae_principal_codigo || null,
    capital_social:Object.hasOwn(company, 'capital_social') &&
      Number.isFinite(company.capital_social) ? company.capital_social : null,
    // A versão legada não distingue false da ausência original em Simples/MEI.
    opcao_simples:est.opcao_simples === true ? true : null,
    opcao_mei:est.opcao_mei === true ? true : null,
    // Não usar ingenuamente o enum: RFB 01=ME virou PB NAO_INFORMADO=1.
    porte_empresa:null,
    data_opcao_simples:numericDate(est.data_opcao_simples),
    data_exclusao_simples:numericDate(est.data_exclusao_simples),
    data_opcao_mei:numericDate(est.data_opcao_mei),
  }};
}
