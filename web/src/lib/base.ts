/**
 * Prefixa um caminho com o `base` configurado no Astro (`import.meta.env.BASE_URL`).
 *
 * No GitHub Pages de projeto o site é servido sob `/ficha/`, então links e
 * fetches absolutos (`/sobre`, `/manifest.json`) quebrariam. `BASE_URL` pode
 * vir com ou sem barra final — normalizamos aqui.
 */
export function withBase(path: string): string {
  const base = import.meta.env.BASE_URL ?? '/';
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}
