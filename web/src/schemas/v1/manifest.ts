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
    cnpj_contatos: z.number().int().nonnegative().optional(),
    cnpj_cnaes: z.number().int().nonnegative().optional(),
    raizes: z.number().int().nonnegative(),
    socios: z.number().int().nonnegative(),
    enderecos: z.number().int().nonnegative().optional(),
    pessoas: z.number().int().nonnegative().optional(),
  }),
  files: z.object({
    cnpjs: FileEntrySchema,
    // Opcional: um upload individual pro Internet Archive pode falhar
    // depois do commit do manifest (ver 2026-04 — cnpj_contatos/cnpj_cnaes
    // ficaram 404). O frontend já degrada a seção correspondente quando
    // ausente; o manifest não deve afirmar que um arquivo existe se a URL
    // não responde.
    cnpj_contatos: FileEntrySchema.optional(),
    cnpj_cnaes: FileEntrySchema.extend({ sort: z.array(z.string()) }).optional(),
    raizes: FileEntrySchema,
    socios: FileEntrySchema,
    enderecos: FileEntrySchema.extend({ sort: z.array(z.string()) }).optional(),
    pessoas: FileEntrySchema.extend({ sort: z.array(z.string()) }).optional(),
    lookups: FileEntrySchema,
    // Camada atômica (companies.zip, um protobuf por raiz). Opcional no schema
    // para compatibilidade com snapshots antigos (2026-04 não a declara), mas
    // exigida pelo ETL em snapshots novos (build_snapshot_entry). Quando
    // presente, o frontend pode rotear lookup exato de CNPJ por ela.
    companies_zip: FileEntrySchema.optional(),
  }),
  lookups: z.record(z.string(), z.object({ url: z.string().url() })).optional(),
});

export const ManifestSchema = z.object({
  current: z.string().regex(/^\d{4}-\d{2}$/),
  snapshots: z.array(SnapshotEntrySchema),
});

export type Snapshot = z.infer<typeof SnapshotEntrySchema>;
export type Manifest = z.infer<typeof ManifestSchema>;
