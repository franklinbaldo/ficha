import { mountSearch } from './search-app.mjs';

// This entrypoint belongs only to the SSG homepage. It is not deployed yet.
mountSearch({ documentRef: document });
