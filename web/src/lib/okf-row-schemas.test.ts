import { describe, expect, it } from 'vitest';
import type { z } from 'zod';
import { EstabelecimentoSchema, RaizSchema, SocioSchema } from '../schemas/v1';
import { getOkfParquetRowSchema } from './okf-row-schemas';

// This is a compatibility gate for published v1 rows, not a claim that
// generated OKF/JSON Schema, physical Parquet or past snapshots are equivalent.
const contracts = [
  {
    type: 'Cnpj',
    publicSchema: EstabelecimentoSchema,
    enumFields: ['identificador_matriz_filial', 'porte_empresa', 'situacao_cadastral'],
    nullableExamples: ['nome_fantasia', 'complemento', 'opcao_simples'],
  },
  {
    type: 'Raiz',
    publicSchema: RaizSchema,
    enumFields: ['porte_empresa'],
    nullableExamples: ['ente_federativo_responsavel'],
  },
  {
    type: 'Socio',
    publicSchema: SocioSchema,
    enumFields: ['tipo', 'tipo_descricao', 'faixa_etaria'],
    nullableExamples: ['cnpj_socio', 'pais_codigo', 'faixa_etaria'],
  },
] as const;

const candidates: unknown[] = [
  null, '', '0', '1', '2', '3', '00', '01', '02', '03', '04', '05',
  '08', 'PJ', 'PF', 'estrangeiro', 0, false, [],
];

function validSample(schema: z.AnyZodObject): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(schema.shape).map(([name, field]) => {
      const validator = field as z.ZodTypeAny;
      const value = candidates.find((candidate) => validator.safeParse(candidate).success);
      if (value === undefined) throw new Error(`No valid fixture for v1 field ${name}`);
      return [name, value];
    })
  );
}

describe('published v1 OKF view row validation', () => {
  for (const { type, publicSchema, enumFields, nullableExamples } of contracts) {
    describe(type, () => {
      const runtimeSchema = getOkfParquetRowSchema(type);
      const baseRow = validSample(publicSchema);

      it('accepts the full v1 row shape without loosening extra-column checks', () => {
        expect(Object.keys(runtimeSchema.shape).sort()).toEqual(
          Object.keys(publicSchema.shape).sort()
        );
        expect(publicSchema.safeParse(baseRow).success).toBe(true);
        expect(runtimeSchema.safeParse(baseRow).success).toBe(true);
        expect(runtimeSchema.safeParse({ ...baseRow, unexpected_column: 1 }).success).toBe(false);
      });

      it('agrees with v1 nullability for every published field', () => {
        for (const name of Object.keys(publicSchema.shape)) {
          const row = { ...baseRow, [name]: null };
          expect(runtimeSchema.safeParse(row).success, `${type}.${name}`).toBe(
            publicSchema.safeParse(row).success
          );
        }
        for (const name of nullableExamples) {
          expect(runtimeSchema.safeParse({ ...baseRow, [name]: null }).success).toBe(true);
        }
      });

      it('enforces all public v1 enum domains', () => {
        for (const name of enumFields) {
          expect(runtimeSchema.safeParse({ ...baseRow, [name]: '__INVALID__' }).success).toBe(
            false
          );
          const publicField = (publicSchema.shape as Record<string, z.ZodTypeAny>)[name];
          for (const value of candidates.filter((candidate) => publicField.safeParse(candidate).success)) {
            expect(runtimeSchema.safeParse({ ...baseRow, [name]: value }).success).toBe(true);
          }
        }
      });
    });
  }

  it('rejects unknown relation names', () => {
    expect(() => getOkfParquetRowSchema('Unknown')).toThrow(
      /Unknown OKF parquet row type/
    );
  });
});
