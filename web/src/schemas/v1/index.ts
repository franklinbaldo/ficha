/**
 * Schema v1 — layout RFB vigente em 2024.
 * NUNCA editar campos publicados. Para mudanças, criar v2/ ao lado.
 */
export { EmpresaSchema, EstabelecimentoSchema, SocioSchema } from './rfb';
export type { Empresa, Estabelecimento, Socio } from './rfb';

export const VERSION = '1.0.0' as const;
