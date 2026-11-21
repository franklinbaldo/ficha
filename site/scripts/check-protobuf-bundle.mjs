/** Offline contract: published bundle decodes the same bytes as the canonical
 * generated protobuf module. Does not contact Internet Archive.
 */
import assert from 'node:assert/strict';
import { ficha } from '../../web/src/generated/company.pb.js';
import { decodeCompany } from '../dist/ficha/js/company-decoder.mjs';

const original={
  cnpj_base:33000167,snapshot_yyyymm:202607,qtd_estabelecimentos:1,
  razao_social:'TESTE',estabelecimentos:[{cnpj_ordem:1,cnpj_dv:1,
    uf:'RJ',municipio_codigo:7107,cnae_principal_codigo:6201501}],
};
const bytes=ficha.v1.Company.encode(original).finish();
const decoded=decodeCompany(bytes);
assert.equal(decoded.cnpj_base,33000167);
assert.equal(decoded.snapshot_yyyymm,202607);
assert.equal(decoded.qtd_estabelecimentos,1);
assert.equal(decoded.estabelecimentos.length,1);
assert.equal(decoded.estabelecimentos[0].cnpj_ordem,1);
assert.equal(decoded.estabelecimentos[0].cnpj_dv,1);
assert.equal(decoded.estabelecimentos[0].municipio_codigo,7107);
// Wire Protobuf3 does not carry absent scalar uint32 with implicit zero.
// This is a common case for CNPJ check digits ending in 00/0.
const omittedDv=decodeCompany(ficha.v1.Company.encode({
  ...original,estabelecimentos:[{cnpj_ordem:2,uf:'RJ'}],
}).finish());
assert.equal(omittedDv.estabelecimentos[0].cnpj_dv,0,
  'DV ausente no wire proto3 é numericamente zero');
const omittedOrder=decodeCompany(ficha.v1.Company.encode({
  ...original,estabelecimentos:[{cnpj_dv:1,uf:'RJ'}],
}).finish());
assert.equal(omittedOrder.estabelecimentos[0].cnpj_ordem,0);
const missingCount={...original};
delete missingCount.qtd_estabelecimentos;
const uncounted=decodeCompany(ficha.v1.Company.encode(missingCount).finish());
assert.equal(Object.hasOwn(uncounted,'qtd_estabelecimentos'),false,
  'não fabricar completude do agregado ausente');
console.log('Bundle canônico PB: roundtrip sintético PASS');
