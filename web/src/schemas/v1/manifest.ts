import { z } from 'zod';

/**
 * `web/public/manifest.json` — single source of truth dos snapshots disponíveis.
 *
 * Ver ADR 0003 (versionamento) e ADR 0008 (estrutura de arquivos por snapshot).
 */
/**
 * Checksum de um arquivo declarado no manifesto.
 *
 * O contrato padronizou em **SHA-1**, o mesmo que o Internet Archive calcula e
 * expõe para cada objeto — o que torna a verificação homogênea com os shards de
 * `companies` e dispensa o ETL de calcular uma segunda família de hash.
 *
 * `sha256` permanece aceito e **opcional** porque snapshots já publicados
 * (2026-04) só têm ele: tornar `sha1` obrigatório de imediato invalidaria o
 * manifesto em produção. A migração é declarada, não silenciosa — a remoção de
 * `sha256` só pode acontecer depois que todo snapshot publicado carregar
 * `sha1`, e exige bump de `schema_version`.
 *
 * Pelo menos um dos dois precisa existir: um arquivo sem checksum nenhum não é
 * verificável, e o manifesto não deve declará-lo como se fosse.
 */
const FileEntryShape = z.object({
  url: z.string().url(),
  sha1: z
    .string()
    .regex(/^[0-9a-f]{40}$/)
    .optional(),
  sha256: z.string().optional(),
  size: z.number().int().nonnegative(),
});

/**
 * Exige pelo menos um checksum, preservando `.extend()`.
 *
 * O refinamento fica num helper porque aplicá-lo direto no schema base o
 * transformaria num `ZodEffects`, que não expõe `.extend()` — e três entradas
 * (`cnpj_cnaes`, `enderecos`, `pessoas`) estendem o FileEntry com `sort`.
 */
const comChecksum = <T extends typeof FileEntryShape>(shape: T) =>
  shape.refine((entry) => Boolean(entry.sha1 || entry.sha256), {
    message: 'file entry precisa de sha1 ou sha256',
    path: ['sha1'],
  });

const FileEntrySchema = comChecksum(FileEntryShape);

const CompanyShardSchema = z.object({
  shard: z.string().regex(/^\d{2}$/),
  url: z.string().url(),
  // Checksum operacional do IA; autenticidade semântica é validada no ETL.
  sha1: z.string().regex(/^[0-9a-f]{40}$/),
  size: z.number().int().positive(),
});

const CompaniesShardedSchema = z
  .object({
    shard_by: z.literal('cnpj_base_prefix_2'),
    shards: z.array(CompanyShardSchema).length(100),
  })
  .superRefine(({ shards }, ctx) => {
    const seen = new Set<string>();
    for (const shard of shards) {
      if (seen.has(shard.shard)) {
        ctx.addIssue({
          code: 'custom',
          path: ['shards'],
          message: `duplicate company shard: ${shard.shard}`,
        });
      }
      seen.add(shard.shard);
    }
    for (let value = 0; value < 100; value += 1) {
      const expected = String(value).padStart(2, '0');
      if (!seen.has(expected)) {
        ctx.addIssue({
          code: 'custom',
          path: ['shards'],
          message: `missing company shard: ${expected}`,
        });
      }
    }
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
    cnpj_cnaes: comChecksum(FileEntryShape.extend({ sort: z.array(z.string()) })).optional(),
    raizes: FileEntrySchema,
    socios: FileEntrySchema,
    enderecos: comChecksum(FileEntryShape.extend({ sort: z.array(z.string()) })).optional(),
    pessoas: comChecksum(FileEntryShape.extend({ sort: z.array(z.string()) })).optional(),
    lookups: FileEntrySchema,
    // Camada atômica histórica: um ZIP monolítico. Continua opcional para
    // compatibilidade com snapshots que já existem (incluindo 2026-04).
    companies_zip: FileEntrySchema.optional(),
    // Camada atômica retomável: conjunto completo 00..99. Cada entrada usa o
    // SHA-1 que o próprio Internet Archive calcula e expõe para o objeto; a
    // identidade semântica da materialização é validada separadamente no ETL.
    companies: CompaniesShardedSchema.optional(),
  }),
  lookups: z.record(z.string(), z.object({ url: z.string().url() })).optional(),
});

export const ManifestSchema = z.object({
  current: z.string().regex(/^\d{4}-\d{2}$/),
  snapshots: z.array(SnapshotEntrySchema),
});

export type Snapshot = z.infer<typeof SnapshotEntrySchema>;
export type Manifest = z.infer<typeof ManifestSchema>;
