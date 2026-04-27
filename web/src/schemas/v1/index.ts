/**
 * Schema v1 — layout FICHA.
 *
 * Três Parquets (estabelecimento, raiz, sócio) + lookups.json + manifest.json.
 * Ver ADR 0008 e 0009.
 *
 * NUNCA editar campos publicados desta versão. Mudanças quebrantes → criar v2/.
 */
export { EstabelecimentoSchema, type Estabelecimento } from './estabelecimento';
export { RaizSchema, type Raiz } from './raiz';
export { SocioSchema, type Socio } from './socio';
export { LookupsSchema, type Lookups } from './lookups';
export { ManifestSchema, SnapshotEntrySchema, type Manifest, type Snapshot } from './manifest';

export const VERSION = '1.0.0' as const;
