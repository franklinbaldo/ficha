import { EstabelecimentoSchema, RaizSchema, SocioSchema } from '../schemas/v1';

// Until a versioned OKF contract has proven semantic parity, validation of
// published Parquet rows must follow the already published v1 contract.
// The generated OKF schemas currently reject 28 permitted nullable fields
// and widen seven enum domains; importing them here changes public semantics.
export const okfParquetRowSchemas = {
  Cnpj: EstabelecimentoSchema.strict(),
  Raiz: RaizSchema.strict(),
  Socio: SocioSchema.strict(),
} as const;

export type OkfParquetRowType = keyof typeof okfParquetRowSchemas;

export function getOkfParquetRowSchema(type: string) {
  const schema = okfParquetRowSchemas[type as OkfParquetRowType];
  if (!schema) throw new Error(`Unknown OKF parquet row type: ${type}`);
  return schema;
}
