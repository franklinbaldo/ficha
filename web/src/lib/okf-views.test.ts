import { describe, expect, it } from 'vitest';
import { getOkfConvenienceView, okfConvenienceViews } from './okf-views';

describe('OKF convenience views', () => {
  it('exposes generated views by name', () => {
    expect(okfConvenienceViews.map((view) => view.name)).toContain('empresas_ativas');
    expect(getOkfConvenienceView('empresas_ativas').inputs).toEqual(['Cnpj']);
  });

  it('fails closed for unknown view names', () => {
    expect(() => getOkfConvenienceView('nao_existe')).toThrow(/Unknown OKF convenience view/);
  });
});
