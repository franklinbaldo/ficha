import { mountSearch } from './search-app.mjs';
import { mountExamplePills } from './examples.mjs';

// Home entrypoint for the published SSG site.
// Show three of four known public CNPJs in a different order on each visit.
mountExamplePills(document);
// Numeric 14-digit CNPJs use the existing Protobuf-first company page; other
// searches preserve the experimental analytical/Parquet path.
mountSearch({
  documentRef: document,
  openNumericFicha: url => globalThis.location.assign(url),
});
