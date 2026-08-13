import { describe, expect, it } from 'vitest';
import { getOkfConvenienceView, okfConvenienceViews } from './okf-views';

describe('OKF convenience views', () => {
  it('exposes generated views by name', () => {
    expect(okfConvenienceViews.map((view) => view.name)).toEqual([
      'empresas_ativas',
      'socios_de_raizes_ativas',
    ]);
    expect(getOkfConvenienceView('empresas_ativas').inputs).toEqual(['Cnpj']);
  });

  it('keeps the relational view explicit about both input types', () => {
    const view = getOkfConvenienceView('socios_de_raizes_ativas');
    expect(view.inputs).toEqual(['Socio', 'Raiz']);
    expect(view.output).toBe('Socio');
    expect(view.sql).toContain('JOIN raizes');
  });

  it('fails closed for unknown view names', () => {
    expect(() => getOkfConvenienceView('nao_existe')).toThrow(/Unknown OKF convenience view/);
  });
});
