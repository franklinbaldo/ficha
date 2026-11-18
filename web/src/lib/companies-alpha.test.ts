import { describe, it, expect } from 'vitest';
import { ficha } from '../generated/company.pb.js';
import { CompaniesShardedSchema } from '../schemas/v1/manifest';
import { companyRoot, estabelecimentoCnpj, socioCnpj, companyArchiveUrl, fetchCompany } from './companies';

describe('P0-A: manifest version negotiation at the updated reader boundary', () => {
  const shards = Array.from({ length: 100 }, (_, number) => ({
    shard: String(number).padStart(2, '0'),
    url: `https://example.org/companies-${String(number).padStart(2, '0')}.zip`,
    sha1: 'a'.repeat(40),
    size: 100,
  }));

  it('preserves legacy v1 numeric manifests without a per-layer version', () => {
    expect(CompaniesShardedSchema.safeParse({
      shard_by: 'cnpj_base_prefix_2',
      shards,
    }).success).toBe(true);
  });

  it('refuses to validate v1.1 alphanumeric mapping without its wire version', () => {
    for (const schema_version of [undefined, '1.0.0'] as const) {
      const result = CompaniesShardedSchema.safeParse({
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
        schema_version,
        shards,
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.issues.some(issue => issue.path.join('.') === 'schema_version')).toBe(true);
      }
    }
  });

  it('rejects 1.1 wire mislabeled with the legacy numeric bucket contract', () => {
    const result = CompaniesShardedSchema.safeParse({
      shard_by: 'cnpj_base_prefix_2',
      schema_version: '1.1.0',
      shards,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues.some(issue => issue.path.join('.') === 'schema_version')).toBe(true);
    }
    // Explicit v1.0 remains supported alongside historical missing versions.
    expect(CompaniesShardedSchema.safeParse({
      shard_by: 'cnpj_base_prefix_2',
      schema_version: '1.0.0',
      shards,
    }).success).toBe(true);
  });

  it('refuses a v1.1 index that aliases two shard numbers to one ZIP URL', () => {
    const aliasedShards = shards.map(shard => ({ ...shard }));
    aliasedShards[1]!.url = aliasedShards[0]!.url;
    const result = CompaniesShardedSchema.safeParse({
      shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
      schema_version: '1.1.0',
      shards: aliasedShards,
    });
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues.some(issue => issue.path.join('.') === 'shards'
        && issue.message.includes('duplicate company shard URL'))).toBe(true);
    }
  });

  it('accepts a complete explicitly versioned v1.1 manifest layer', () => {
    expect(CompaniesShardedSchema.safeParse({
      shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
      schema_version: '1.1.0',
      shards,
    }).success).toBe(true);
  });
});

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
  it('never routes an alphanumeric root through a legacy numeric ZIP, even with numeric prefix', () => {
    const files = { companies: {
      shard_by: 'cnpj_base_prefix_2' as const,
      shards: [{ shard: '00', url: 'https://example.org/companies-00.zip', sha1: 'a'.repeat(40), size: 1 }],
    } };
    expect(companyArchiveUrl('00123456', { identifier: 'legacy', files }))
      .toBe('https://example.org/companies-00.zip');
    expect(() => companyArchiveUrl('00AB1234', { identifier: 'legacy', files }))
      .toThrow(/alphanumeric root requires v1.1 shard mapping/);
    expect(companyArchiveUrl('00AB1234', { identifier: 'new', files: {
      companies: { ...files.companies,
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
        schema_version: '1.1.0' as const,
      },
    } }))
      .toBe('https://example.org/companies-00.zip');
  });

  it('fails closed for a v1.1 wire label under the old numeric mapping', () => {
    const files = { companies: {
      shard_by: 'cnpj_base_prefix_2' as const,
      schema_version: '1.1.0' as const,
      shards: [{ shard: '00', url: 'https://example.org/companies-00.zip', sha1: 'a'.repeat(40), size: 1 }],
    } };
    expect(() => companyArchiveUrl('00000000', { identifier: 'test', files }))
      .toThrow(/schema 1.1.0 cannot use legacy numeric shard mapping/);
  });

  it('fails closed when new bucket semantics omit the 1.1 wire contract', () => {
    const files = { companies: {
      shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1' as const,
      shards: [{ shard: '71', url: 'https://example.org/companies-71.zip', sha1: 'a'.repeat(40), size: 1 }],
    } };
    expect(() => companyArchiveUrl('ABCD1234', { identifier: 'test', files }))
      .toThrow(/schema 1\.1\.0 required/);
  });

  it('refuses alpha establishment wire mislabeled as v1, even with a matching numeric root', async () => {
    // Authentic 1.1 packer fixture: numeric root 00000000, alpha order E08G, DV 12.
    const bytes = Uint8Array.from('a20108100c2a0445303847'.match(/../g)!.map(b => parseInt(b, 16)));
    const fetchImpl = (async () => ({
      ok: true, status: 200, arrayBuffer: async () => bytes.buffer,
    })) as unknown as typeof fetch;
    const shard = { shard: '00', url: 'https://example.org/companies-00.zip', sha1: 'a'.repeat(40), size: 1 };
    await expect(fetchCompany('00000000', {
      identifier: 'legacy', fetchImpl,
      files: { companies: { shard_by: 'cnpj_base_prefix_2', shards: [shard] } },
    })).rejects.toThrow(/v1 numeric shard contains v1.1 textual CNPJ fields/);
    const valid = await fetchCompany('00000000', {
      identifier: 'new', fetchImpl,
      files: { companies: {
        shard_by: 'cnpj_base_prefix_or_base36_mod_100_v1',
        schema_version: '1.1.0', shards: [shard],
      } },
    });
    expect(valid).not.toBeNull();
    expect(estabelecimentoCnpj(valid!, valid!.estabelecimentos![0]!)).toBe('00000000E08G12');
  });

  it('rejects a mismatched legacy wire instead of silently returning root 00000000', async () => {
    const legacyBytes = ficha.v1.Company.encode({ cnpj_base: 0 }).finish();
    const fetchImpl = (async () => ({
      ok: true, status: 200, arrayBuffer: async () => Uint8Array.from(legacyBytes).buffer,
    })) as unknown as typeof fetch;
    await expect(fetchCompany('ABCD1234', { identifier: 'test', fetchImpl }))
      .rejects.toThrow(/root mismatch/);
  });

  it('accepts matching v1 numeric wire and v1.1 textual wire in the guarded reader', async () => {
    for (const [root, company] of [
      ['12345678', { cnpj_base: 12345678 }],
      ['ABCD1234', { cnpj_base_text: 'ABCD1234' }],
    ] as const) {
      const bytes = ficha.v1.Company.encode(company).finish();
      const fetchImpl = (async () => ({
        ok: true, status: 200, arrayBuffer: async () => Uint8Array.from(bytes).buffer,
      })) as unknown as typeof fetch;
      const decoded = await fetchCompany(root, { identifier: 'test', fetchImpl });
      expect(decoded && companyRoot(decoded)).toBe(root);
    }
  });
});
