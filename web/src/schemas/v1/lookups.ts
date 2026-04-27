import { z } from 'zod';

/**
 * Estrutura do `lookups.json` — tabelas de referência pequenas o bastante
 * pra serem carregadas integralmente no boot do site.
 *
 * Cada tabela é um mapa código → descrição.
 *
 * Ver ADR 0008.
 */
export const LookupsSchema = z.object({
  schema_version: z.string(),
  snapshot_date: z.string(),

  cnaes: z.record(z.string(), z.string()),
  motivos_situacao_cadastral: z.record(z.string(), z.string()),
  municipios: z.record(z.string(), z.string()),
  naturezas_juridicas: z.record(z.string(), z.string()),
  paises: z.record(z.string(), z.string()),
  qualificacoes_socio: z.record(z.string(), z.string()),
});

export type Lookups = z.infer<typeof LookupsSchema>;
