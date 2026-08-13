import { describe, expect, it } from 'vitest';
import {
  getOkfConvenienceView,
  okfConvenienceViews,
  validateOkfConvenienceViewRows,
} from './okf-views';

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

  it('validates physical rows with the generated output schema', () => {
    const row = {
      cnpj_base: '11111111',
      tipo: '1',
      tipo_descricao: 'PJ',
      nome_socio_razao_social: 'OUTRA EMPRESA SA',
      cpf_mascarado: null,
      cnpj_socio: '44444444000100',
      qualificacao_codigo: '49',
      qualificacao_descricao: 'Sócio',
      data_entrada_sociedade: '2020-01-01',
      pais_codigo: '105',
      pais_nome: 'Brasil',
      representante_legal_cpf: null,
      representante_legal_nome: null,
      representante_legal_qualificacao_codigo: null,
      representante_legal_qualificacao_descricao: '',
      faixa_etaria: '0',
    };
    expect(validateOkfConvenienceViewRows('socios_de_raizes_ativas', [row])).toEqual([row]);
  });

  it('fails closed when a view returns a row outside its declared output shape', () => {
    expect(() => validateOkfConvenienceViewRows('socios_de_raizes_ativas', [{}])).toThrow(
      /returned invalid Socio row/
    );
  });

  it('fails closed for unknown view names', () => {
    expect(() => getOkfConvenienceView('nao_existe')).toThrow(/Unknown OKF convenience view/);
  });
});
