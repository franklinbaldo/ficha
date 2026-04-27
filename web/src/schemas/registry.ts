/**
 * Registry de schemas por versão.
 * O frontend lê `ficha.schema_version` do footer do Parquet (ou do manifest)
 * e seleciona o schema correto aqui.
 */
import * as v1 from './v1';

export const registry = {
  '1.0.0': v1,
} as const;

export type SchemaVersion = keyof typeof registry;

export function getSchema(version: string) {
  if (!(version in registry)) {
    throw new Error(`Schema version não suportada: ${version}`);
  }
  return registry[version as SchemaVersion];
}
