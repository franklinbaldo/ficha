/** Single browser entrypoint for the checked-in canonical generated Protobuf.
 * Bundled by esbuild at publication time; never serve node_modules to visitors.
 */
import { ficha } from '../../web/src/generated/company.pb.js';

export function decodeCompany(bytes) {
  const decoded = ficha.v1.Company.decode(bytes);
  // Retain snake_case field names and omit unset fields. Numeric 64-bit values
  // are safe for the fields consumed by the numeric v1 reader.
  return ficha.v1.Company.toObject(decoded, {longs:Number, enums:Number, defaults:false});
}
