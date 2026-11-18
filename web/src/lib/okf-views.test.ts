import { describe, expect, it } from 'vitest';
import { SocioSchema as PublicV1SocioSchema } from '../schemas/v1/socio';
import {
  getOkfConvenienceView,
  okfConvenienceViews,
  validateOkfConvenienceViewRows,
} from './okf-views';

const socioRow = {
  cnpj_base: '11111111',
  tipo: '1',
  tipo_descricao: 'PJ',
  nome_socio_razao_social: 'OUTRA EMPRESA SA',
  cpf_mascarado: null,
  cnpj_socio: '44444444000100',
  qualificacao_codigo: '49',
  qualificacao_descricao: 'Socio',
  data_entrada_sociedade: '2020-01-01',
  pais_codigo: '105',
  pais_nome: 'Brasil',
  representante_legal_cpf: null,
  representante_legal_nome: null,
  representante_legal_qualificacao_codigo: null,
  representante_legal_qualificacao_descricao: '',
  faixa_etaria: '0',
};

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
    expect(validateOkfConvenienceViewRows('socios_de_raizes_ativas', [socioRow])).toEqual([
      socioRow,
    ]);
  });

  it('documents the existing public-v1 nullable-row mismatch at the OKF view boundary', () => {
    // A PF row may legitimately have no CNPJ for the partner.
    // The generated OKF schema is deliberately not equivalent to public v1.
    const publicV1Row = {
      ...socioRow,
      tipo: '2',
      tipo_descricao: 'PF',
      cpf_mascarado: '***.123.***-**',
      cnpj_socio: null,
    };

    expect(PublicV1SocioSchema.safeParse(publicV1Row).success).toBe(true);
    expect(() =>
      validateOkfConvenienceViewRows('socios_de_raizes_ativas', [publicV1Row])
    ).toThrow(/cnpj_socio/);
  });

  it('fails closed when a view returns a row outside its declared output shape', () => {
    expect(() => validateOkfConvenienceViewRows('socios_de_raizes_ativas', [{}])).toThrow(
      /returned invalid Socio row/
    );
  });

  it('fails closed when a view adds an undeclared column', () => {
    expect(() =>
      validateOkfConvenienceViewRows('socios_de_raizes_ativas', [
        { ...socioRow, extra_column: 'unexpected' },
      ])
    ).toThrow(/returned invalid Socio row/);
  });

  it('fails closed for unknown view names', () => {
    expect(() => getOkfConvenienceView('nao_existe')).toThrow(/Unknown OKF convenience view/);
  });
});
