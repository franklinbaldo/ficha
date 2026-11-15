import { describe, it, expect } from 'vitest';
import { ficha } from '../generated/company.pb.js';
import { companyRoot, estabelecimentoCnpj, socioCnpj, companyArchiveUrl } from './companies';

describe('schema 1.1 textual identity with v1 numeric fallback', () => {
  it('decodes the Python packer wire for the first real RFB alphanumeric CNPJ', () => {
    // row_to_company(root=00000000, ordem=E08G, dv=12).SerializeToString()
    const bytes = Uint8Array.from('a20108100c2a0445303847'.match(/../g)!.map(b => parseInt(b, 16)));
    const c = ficha.v1.Company.decode(bytes);
    expect(estabelecimentoCnpj(c, c.estabelecimentos[0]!)).toBe('00000000E08G12');
  });
  it('continues reading numeric snapshot v1 bytes', () => {
    const bytes = Uint8Array.from('a201040801105b'.match(/../g)!.map(b => parseInt(b, 16)));
    const c = ficha.v1.Company.decode(bytes);
    expect(estabelecimentoCnpj(c, c.estabelecimentos[0]!)).toBe('00000000000191');
  });
  it('prefers text over legacy numeric mirrors', () => {
    expect(companyRoot({ cnpj_base_text: 'ABCD1234', cnpj_base: 0 })).toBe('ABCD1234');
    expect(socioCnpj({ cnpj_socio_text: '12ABC34501DE35', cnpj_socio: 0 })).toBe('12ABC34501DE35');
  });
  it('resolves alpha roots with the declared new 100-shard mapping', () => {
    const files = { companies: {
      shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
      schema_version: '1.1.0' as const,
      shards: [{ shard: '71', url: 'https://example.org/companies-71.zip', sha1: 'a'.repeat(40), size: 1 }],
    } };
    expect(companyArchiveUrl('ABCD1234', { identifier: 'test', files })).toBe('https://example.org/companies-71.zip');
  });
});
