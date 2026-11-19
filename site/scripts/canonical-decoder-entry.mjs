/** Single browser entrypoint for the checked-in canonical generated Protobuf.
 * Bundled by esbuild at publication time; never serve node_modules to visitors.
 */
import { ficha } from '../../web/src/generated/company.pb.js';

export function decodeCompany(bytes) {
  const decoded = ficha.v1.Company.decode(bytes);
  // Retain snake_case field names and omit unset fields. Numeric 64-bit values
  // are safe for the fields consumed by the numeric v1 reader.
  const company = ficha.v1.Company.toObject(decoded,
    {longs:Number, enums:Number, defaults:false});
  // Proto3 scalar uint32 defaults to zero when the field is omitted on wire.
  // Python's protobuf serializer omits zero DV values; preserving undefined
  // would reject an otherwise intact root (observed in Petrobras 2026-07).
  // Only normalize the two identifier scalars; do NOT fill the root aggregate
  // count, whose explicit presence is required before claiming CNPJ absence.
  for (const est of company.estabelecimentos ?? []) {
    if (!Object.hasOwn(est, 'cnpj_ordem')) est.cnpj_ordem = 0;
    if (!Object.hasOwn(est, 'cnpj_dv')) est.cnpj_dv = 0;
  }
  return company;
}
