import { describe, expect, it } from 'vitest';
import { CompaniesShardedSchema } from './manifest';

const numeric = 'cnpj_base_prefix_2';
const alphanumeric = 'cnpj_base_prefix_or_base36_mod_100_v1';
function layer(shard_by = numeric) {
  return {
    shard_by,
    shards: Array.from({length:100}, (_,i) => ({
      shard:String(i).padStart(2,'0'),
      url:'https://archive.org/download/ficha-2026-07/companies-'+String(i).padStart(2,'0')+'.zip',
      sha1:'a'.repeat(40), size:1,
    })),
  };
}

describe('manifest wire authority (#383)', () => {
  it('accepts numeric legacy omission/null and explicit v1', () => {
    expect(CompaniesShardedSchema.safeParse(layer()).success).toBe(true);
    for (const schema_version of [null,'1.0.0']) {
      const result=CompaniesShardedSchema.parse({...layer(),schema_version});
      expect(result.schema_version).toBe(schema_version);
    }
  });
  it('rejects incompatible numeric layer but preserves opt-in 1.1 mapping', () => {
    for (const schema_version of ['1.1.0','9.0.0','',1]) {
      expect(CompaniesShardedSchema.safeParse({...layer(),schema_version}).success).toBe(false);
    }
    expect(CompaniesShardedSchema.safeParse({...layer(alphanumeric),schema_version:'1.1.0'}).success).toBe(true);
    expect(CompaniesShardedSchema.safeParse({...layer(alphanumeric),schema_version:null}).success).toBe(false);
  });
  it('rejects any own shard declaration rather than stripping it', () => {
    for (const mapping of [numeric,alphanumeric]) {
      for (const i of [0,99]) {
        for (const value of [undefined,null,'1.0.0','1.1.0','9.0.0']) {
          const candidate={...layer(mapping),schema_version:mapping===numeric?'1.0.0':'1.1.0'};
          Object.assign(candidate.shards[i],{schema_version:value});
          const result=CompaniesShardedSchema.safeParse(candidate);
          expect(result.success).toBe(false);
          if(!result.success) expect(result.error.issues.some(issue =>
            issue.path.join('.')==='shards.'+i+'.schema_version')).toBe(true);
        }
      }
    }
  });
});
