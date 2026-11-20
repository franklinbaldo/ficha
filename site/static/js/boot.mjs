import { mountSearch } from './search-app.mjs';

// This entrypoint belongs only to the SSG homepage. It is not deployed yet.
// Numeric 14-digit CNPJs use the existing Protobuf-first company page; other
// searches preserve the experimental analytical/Parquet path.
mountSearch({
  documentRef: document,
  openNumericFicha: url => globalThis.location.assign(url),
});
