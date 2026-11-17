/**
 * Separate opt-in Protobuf 1.1 discovery; never used by fetchManifest() or
 * by the public UI. No 1.1 index or ZIPs are published by this module.
 */
import { ManifestSchema, type Manifest } from '../schemas/v1/manifest';
import { withBase } from './base';
import { fetchCompany, type Company } from './companies';

const INDEX_PATH = 'manifest-atomic-v1.1.json';
const WIRE_VERSION = '1.1.0';
const SHARD_BY = 'cnpj_base_prefix_or_base36_mod_100_v1';

/** Validate every snapshot before allowing any versioned wire request. */
export function parseAtomicV11Manifest(raw: unknown): Manifest {
  const parsed = ManifestSchema.safeParse(raw);
  if (!parsed.success) {
    throw new Error('invalid opt-in Protobuf 1.1 index: ' + parsed.error.message);
  }

  const manifest = parsed.data;
  const dates = new Set<string>();
  if (manifest.snapshots.length === 0) {
    throw new Error('opt-in Protobuf 1.1 index must contain a snapshot');
  }
  for (const snapshot of manifest.snapshots) {
    if (dates.has(snapshot.date)) {
      throw new Error('duplicate snapshot in opt-in Protobuf 1.1 index: ' + snapshot.date);
    }
    dates.add(snapshot.date);
    const companies = snapshot.files.companies;
    if (snapshot.files.companies_zip || !companies ||
        companies.schema_version !== WIRE_VERSION || companies.shard_by !== SHARD_BY) {
      throw new Error('snapshot ' + snapshot.date + ' is not a standalone Protobuf 1.1 layer');
    }
    // The Zod schema already checks 100 unique bucket IDs and byte identities.
    // Exact URLs prevent routing new wire data into v1 or another IA item.
    for (const shard of companies.shards) {
      const expected = 'https://archive.org/download/ficha-' + snapshot.date +
        '/wire-1.1/companies-' + shard.shard + '.zip';
      if (shard.url !== expected) {
        throw new Error('snapshot ' + snapshot.date + ', shard ' + shard.shard +
          ': unversioned or noncanonical URL');
      }
    }
  }
  if (!dates.has(manifest.current)) {
    throw new Error('opt-in Protobuf 1.1 index current snapshot is missing');
  }
  return manifest;
}

/** HTTP 404 means no opt-in index: never fall back to v1 discovery. */
export async function fetchAtomicV11Manifest(
  { fetchImpl = fetch }: { fetchImpl?: typeof fetch } = {}
): Promise<Manifest | null> {
  const url = withBase(INDEX_PATH);
  const response = await fetchImpl(url, { cache: 'no-store' });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(url + ': HTTP ' + response.status);
  return parseAtomicV11Manifest(await response.json());
}

/** Request a company only after selecting a verified v1.1 index explicitly. */
export async function fetchAtomicV11Company(
  root: number | string,
  manifest: Manifest,
  { month = manifest.current, fetchImpl }: { month?: string; fetchImpl?: typeof fetch } = {}
): Promise<Company | null> {
  const checked = parseAtomicV11Manifest(manifest);
  const snapshot = checked.snapshots.find(entry => entry.date === month);
  if (!snapshot) throw new Error('opt-in Protobuf 1.1 snapshot ' + month + ' not found');
  return fetchCompany(root, {
    identifier: 'ficha-' + month,
    files: { companies: snapshot.files.companies },
    fetchImpl,
  });
}
