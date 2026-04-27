import { z } from 'zod';

/**
 * `web/public/manifest.json` — single source of truth dos snapshots disponíveis.
 *
 * Ver ADR 0003 (versionamento) e ADR 0008 (estrutura de arquivos por snapshot).
 */
const FileEntrySchema = z.object({
  url: z.string().url(),
  sha256: z.string(),
  size: z.number().int().nonnegative(),
});

export const SnapshotEntrySchema = z.object({
  date: z.string().regex(/^\d{4}-\d{2}$/),
  schema_version: z.string(),
  rfb_layout_date: z.string().nullable(),
  generated_at: z.string(),
  generator: z.string(),
  row_counts: z.object({
    cnpjs: z.number().int().nonnegative(),
    raizes: z.number().int().nonnegative(),
    socios: z.number().int().nonnegative(),
  }),
  files: z.object({
    cnpjs: FileEntrySchema,
    raizes: FileEntrySchema,
    socios: FileEntrySchema,
    lookups: FileEntrySchema,
  }),
});

export const ManifestSchema = z.object({
  current: z.string().regex(/^\d{4}-\d{2}$/),
  snapshots: z.array(SnapshotEntrySchema),
});

export type Snapshot = z.infer<typeof SnapshotEntrySchema>;
export type Manifest = z.infer<typeof ManifestSchema>;
