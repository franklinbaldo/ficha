import { CnpjSchema, RaizSchema, SocioSchema } from '../generated/ficha-okf.zod';

export const okfParquetRowSchemas = {
  Cnpj: CnpjSchema.omit({ type: true, title: true, description: true }).strict(),
  Raiz: RaizSchema.omit({ type: true, title: true, description: true }).strict(),
  Socio: SocioSchema.omit({ type: true, title: true, description: true }).strict(),
} as const;

export type OkfParquetRowType = keyof typeof okfParquetRowSchemas;

export function getOkfParquetRowSchema(type: string) {
  const schema = okfParquetRowSchemas[type as OkfParquetRowType];
  if (!schema) throw new Error(`Unknown OKF parquet row type: ${type}`);
  return schema;
}
