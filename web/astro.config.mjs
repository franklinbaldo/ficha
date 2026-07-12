import { defineConfig } from 'astro/config';
import svelte from '@astrojs/svelte';

// SITE/BASE_PATH vêm do actions/configure-pages no deploy (ex.: project pages
// servem sob /ficha/). Localmente ambos ficam vazios → raiz, como antes.
const site = process.env.SITE || 'https://franklinbaldo.github.io';
const base = process.env.BASE_PATH || '/';

export default defineConfig({
  site,
  base,
  integrations: [svelte()],
  output: 'static',
  vite: {
    optimizeDeps: {
      exclude: ['@duckdb/duckdb-wasm'],
    },
  },
});
