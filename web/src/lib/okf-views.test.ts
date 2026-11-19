import { describe, expect, it } from 'vitest';
import { EstabelecimentoSchema as PublicV1CnpjSchema } from '../schemas/v1/estabelecimento';
import { RaizSchema as PublicV1RaizSchema } from '../schemas/v1/raiz';
import { okfParquetRowSchemas } from './okf-row-schemas';
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

  it('documents generated OKF enum widening for three Socio public-v1 domains', () => {
    // The generated shape uses unrestricted z.string() for these public enums.
    // Keep this as evidence of incompatibility, not as an accepted wire contract.
    const invalidPublicCodes = [
      { tipo: '9' },
      { tipo_descricao: 'desconhecido' },
      { faixa_etaria: '10' },
    ];
    for (const changed of invalidPublicCodes) {
      const row = { ...socioRow, ...changed };
      expect(PublicV1SocioSchema.safeParse(row).success).toBe(false);
      expect(validateOkfConvenienceViewRows('socios_de_raizes_ativas', [row])).toEqual([
        row,
      ]);
    }
  });

  it('documents Cnpj and Raiz public-v1 nullable fields rejected by generated OKF rows', () => {
    // Check field validators, not complete Parquet rows or live snapshots.
    expect(PublicV1CnpjSchema.shape.nome_fantasia.safeParse(null).success).toBe(true);
    expect(okfParquetRowSchemas.Cnpj.shape.nome_fantasia.safeParse(null).success).toBe(false);
    expect(PublicV1RaizSchema.shape.ente_federativo_responsavel.safeParse(null).success).toBe(
      true
    );
    expect(
      okfParquetRowSchemas.Raiz.shape.ente_federativo_responsavel.safeParse(null).success
    ).toBe(false);
  });

  it('documents generated OKF enum widening in Cnpj and Raiz field validators', () => {
    // Together with the Socio row test above, these cover all seven known enums.
    const invalidValues = [
      [
        PublicV1CnpjSchema.shape.identificador_matriz_filial,
        okfParquetRowSchemas.Cnpj.shape.identificador_matriz_filial,
        '9',
      ],
      [
        PublicV1CnpjSchema.shape.porte_empresa,
        okfParquetRowSchemas.Cnpj.shape.porte_empresa,
        '99',
      ],
      [
        PublicV1CnpjSchema.shape.situacao_cadastral,
        okfParquetRowSchemas.Cnpj.shape.situacao_cadastral,
        '99',
      ],
      [
        PublicV1RaizSchema.shape.porte_empresa,
        okfParquetRowSchemas.Raiz.shape.porte_empresa,
        '99',
      ],
    ] as const;
    for (const [publicField, generatedField, value] of invalidValues) {
      expect(publicField.safeParse(value).success).toBe(false);
      expect(generatedField.safeParse(value).success).toBe(true);
    }
  });

  it('distinguishes public-v1 unknown-field stripping from strict OKF view rows', () => {
    const rowWithExtra = { ...socioRow, future_column: 'new' };
    const parsed = PublicV1SocioSchema.safeParse(rowWithExtra);
    expect(parsed.success).toBe(true);
    if (parsed.success) expect(parsed.data).not.toHaveProperty('future_column');
    expect(() =>
      validateOkfConvenienceViewRows('socios_de_raizes_ativas', [rowWithExtra])
    ).toThrow(/returned invalid Socio row/);
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
