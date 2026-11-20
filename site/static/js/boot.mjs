import { mountSearch } from './search-app.mjs';

// Home entrypoint for the published SSG site.
// Numeric 14-digit CNPJs use the existing Protobuf-first company page; other
// searches preserve the experimental analytical/Parquet path.
mountSearch({
  documentRef: document,
  openNumericFicha: url => globalThis.location.assign(url),
});
