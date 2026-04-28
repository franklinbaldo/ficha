import { ManifestSchema, type Manifest, type Snapshot } from '../schemas/v1/manifest';

/**
 * Fetches `/manifest.json` from the same origin as the site (works on
 * GitHub Pages, dev server, custom domain — todos servem `public/`).
 *
 * Retorna `null` quando o manifest ainda não foi publicado (404). Outros
 * erros (network, parse, validação Zod) viram exceção.
 */
export async function fetchManifest(): Promise<Manifest | null> {
  const url = '/manifest.json';
  let response: Response;
  try {
    response = await fetch(url, { cache: 'no-store' });
  } catch (err) {
    throw new Error(`network error fetching ${url}: ${(err as Error).message}`);
  }
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`${url} → HTTP ${response.status}`);
  }
  const raw = await response.json();
  const parsed = ManifestSchema.safeParse(raw);
  if (!parsed.success) {
    throw new Error(`manifest schema validation failed: ${parsed.error.message}`);
  }
  return parsed.data;
}

/** Snapshot atual indicado pelo `current` field, ou null se ausente/inválido. */
export function currentSnapshot(manifest: Manifest): Snapshot | null {
  return manifest.snapshots.find((s) => s.date === manifest.current) ?? null;
}
