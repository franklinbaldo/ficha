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


/**
 * Two complete, synthetic public-v1 rows. Keep the column sets explicit rather
 * than deriving them from the generated OKF shape under test.
 */
const cnpjWholeRow = {
  cnpj: '11111111000100',
  cnpj_base: '11111111',
  cnpj_ordem: '0001',
  cnpj_dv: '00',
  identificador_matriz_filial: '1',
  razao_social: 'EMPRESA EXEMPLO',
  razao_social_normalizada: 'EMPRESA EXEMPLO',
  natureza_juridica_codigo: '2062',
  natureza_juridica_descricao: 'Sociedade empresaria',
  qualificacao_responsavel_codigo: '49',
  qualificacao_responsavel_descricao: 'Socio',
  capital_social: 1000,
  porte_empresa: '01',
  ente_federativo_responsavel: '',
  nome_fantasia: 'EXEMPLO',
  situacao_cadastral: '02',
  situacao_cadastral_descricao: 'Ativa',
  data_situacao_cadastral: '2020-01-01',
  motivo_situacao_cadastral_codigo: '00',
  motivo_situacao_cadastral_descricao: '',
  data_inicio_atividade: '2020-01-01',
  cnae_principal_codigo: '6201501',
  cnae_principal_descricao: 'Exemplo',
  cnae_secundario_codigos: [],
  cnae_secundario_descricoes: [],
  tipo_logradouro: 'RUA',
  logradouro: 'EXEMPLO',
  numero: '1',
  complemento: '',
  bairro: 'CENTRO',
  cep: '00000000',
  uf: 'RO',
  municipio_codigo: '0000',
  municipio_nome: 'EXEMPLO',
  pais_codigo: '105',
  pais_nome: 'Brasil',
  nome_cidade_exterior: '',
  ddd_1: '',
  telefone_1: '',
  ddd_2: '',
  telefone_2: '',
  ddd_fax: '',
  fax: '',
  correio_eletronico: '',
  situacao_especial: '',
  data_situacao_especial: '',
  opcao_simples: false,
  data_opcao_simples: '',
  data_exclusao_simples: '',
  opcao_mei: false,
  data_opcao_mei: '',
  data_exclusao_mei: '',
};

const raizWholeRow = {
  cnpj_base: '11111111',
  razao_social: 'EMPRESA EXEMPLO',
  razao_social_normalizada: 'EMPRESA EXEMPLO',
  natureza_juridica_codigo: '2062',
  natureza_juridica_descricao: 'Sociedade empresaria',
  capital_social: 1000,
  porte_empresa: '01',
  ente_federativo_responsavel: '',
  qtd_estabelecimentos: 1,
  qtd_estabelecimentos_ativos: 1,
  ufs_atuacao: ['RO'],
  cnaes_principais_distintos: ['6201501'],
  data_inicio_atividade_matriz: '2020-01-01',
  uf_matriz: 'RO',
  municipio_matriz_codigo: '0000',
  municipio_matriz_nome: 'EXEMPLO',
  cnae_principal_matriz_codigo: '6201501',
  cnae_principal_matriz_descricao: 'Exemplo',
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


  it('uses independently enumerated complete synthetic Cnpj/Raiz rows as controls', () => {
    expect(PublicV1CnpjSchema.safeParse(cnpjWholeRow).success).toBe(true);
    expect(okfParquetRowSchemas.Cnpj.safeParse(cnpjWholeRow).success).toBe(true);
    expect(PublicV1RaizSchema.safeParse(raizWholeRow).success).toBe(true);
    expect(okfParquetRowSchemas.Raiz.safeParse(raizWholeRow).success).toBe(true);
  });

  it('records Cnpj view consumer acceptance divergence, not a published-row verdict', () => {
    const nullName = { ...cnpjWholeRow, nome_fantasia: null };
    expect(PublicV1CnpjSchema.safeParse(nullName).success).toBe(true);
    expect(() => validateOkfConvenienceViewRows('empresas_ativas', [nullName])).toThrow(
      /nome_fantasia/
    );

    const invalidPorte = { ...cnpjWholeRow, porte_empresa: '99' };
    expect(PublicV1CnpjSchema.safeParse(invalidPorte).success).toBe(false);
    expect(validateOkfConvenienceViewRows('empresas_ativas', [invalidPorte])).toEqual([
      invalidPorte,
    ]);
  });

  it('demonstrates the Cnpj/Raiz null mismatch on complete rows', () => {
    const cnpjWithNull = { ...cnpjWholeRow, nome_fantasia: null };
    expect(PublicV1CnpjSchema.safeParse(cnpjWithNull).success).toBe(true);
    expect(okfParquetRowSchemas.Cnpj.safeParse(cnpjWithNull).success).toBe(false);

    const raizWithNull = { ...raizWholeRow, ente_federativo_responsavel: null };
    expect(PublicV1RaizSchema.safeParse(raizWithNull).success).toBe(true);
    expect(okfParquetRowSchemas.Raiz.safeParse(raizWithNull).success).toBe(false);
  });

  it('witnesses all 28 public-v1 nullable gaps on complete synthetic rows', () => {
    // Intentionally independent, literal witnesses: do not derive this list
    // from either Zod shape or the generator that this test checks.
    const cnpjGaps = [
      'ente_federativo_responsavel', 'nome_fantasia', 'tipo_logradouro',
      'logradouro', 'numero', 'complemento', 'bairro', 'cep',
      'pais_codigo', 'pais_nome', 'nome_cidade_exterior', 'ddd_1',
      'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax',
      'correio_eletronico', 'situacao_especial', 'opcao_simples',
      'data_opcao_simples', 'opcao_mei',
    ] as const;
    const raizGaps = ['ente_federativo_responsavel'] as const;
    const socioGaps = [
      'cnpj_socio', 'pais_codigo', 'pais_nome',
      'representante_legal_qualificacao_descricao', 'faixa_etaria',
    ] as const;

    expect([cnpjGaps.length, raizGaps.length, socioGaps.length]).toEqual([22, 1, 5]);
    for (const field of cnpjGaps) {
      const row = { ...cnpjWholeRow, [field]: null };
      expect(PublicV1CnpjSchema.safeParse(row).success, `public Cnpj.${field}`).toBe(true);
      expect(okfParquetRowSchemas.Cnpj.safeParse(row).success, `OKF Cnpj.${field}`).toBe(false);
    }
    for (const field of raizGaps) {
      const row = { ...raizWholeRow, [field]: null };
      expect(PublicV1RaizSchema.safeParse(row).success, `public Raiz.${field}`).toBe(true);
      expect(okfParquetRowSchemas.Raiz.safeParse(row).success, `OKF Raiz.${field}`).toBe(false);
    }
    for (const field of socioGaps) {
      const row = { ...socioRow, [field]: null };
      expect(PublicV1SocioSchema.safeParse(row).success, `public Socio.${field}`).toBe(true);
      expect(okfParquetRowSchemas.Socio.safeParse(row).success, `OKF Socio.${field}`).toBe(false);
    }
  });

  it('demonstrates four widened Cnpj/Raiz enum domains on complete rows', () => {
    const cnpjInvalidEnums = [
      { identificador_matriz_filial: '9' },
      { porte_empresa: '99' },
      { situacao_cadastral: '99' },
    ];
    for (const changed of cnpjInvalidEnums) {
      const row = { ...cnpjWholeRow, ...changed };
      expect(PublicV1CnpjSchema.safeParse(row).success).toBe(false);
      expect(okfParquetRowSchemas.Cnpj.safeParse(row).success).toBe(true);
    }

    const raizWithInvalidPorte = { ...raizWholeRow, porte_empresa: '99' };
    expect(PublicV1RaizSchema.safeParse(raizWithInvalidPorte).success).toBe(false);
    expect(okfParquetRowSchemas.Raiz.safeParse(raizWithInvalidPorte).success).toBe(true);
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

  it('separates absent required columns from unknown future columns for every v1 row type', () => {
    // A nullable field is still required in the wire shape: omitted != null.
    // Conversely, public v1 strips unknown keys, while generated OKF rejects them.
    const cases = [
      { name: 'Cnpj', row: cnpjWholeRow, required: 'nome_fantasia',
        publicSchema: PublicV1CnpjSchema, generatedSchema: okfParquetRowSchemas.Cnpj },
      { name: 'Raiz', row: raizWholeRow, required: 'ente_federativo_responsavel',
        publicSchema: PublicV1RaizSchema, generatedSchema: okfParquetRowSchemas.Raiz },
      { name: 'Socio', row: socioRow, required: 'cnpj_socio',
        publicSchema: PublicV1SocioSchema, generatedSchema: okfParquetRowSchemas.Socio },
    ] as const;

    for (const { name, row, required, publicSchema, generatedSchema } of cases) {
      expect(publicSchema.safeParse(row).success, `v1 ${name} baseline`).toBe(true);
      expect(generatedSchema.safeParse(row).success, `OKF ${name} baseline`).toBe(true);

      const omitted = { ...row } as Record<string, unknown>;
      delete omitted[required];
      expect(publicSchema.safeParse(omitted).success, `v1 ${name} missing ${required}`).toBe(false);
      expect(generatedSchema.safeParse(omitted).success, `OKF ${name} missing ${required}`).toBe(false);

      const future = { ...row, future_column: 'synthetic' };
      const parsed = publicSchema.safeParse(future);
      expect(parsed.success, `v1 ${name} extra column`).toBe(true);
      if (parsed.success) expect(parsed.data).not.toHaveProperty('future_column');
      expect(generatedSchema.safeParse(future).success, `OKF ${name} extra column`).toBe(false);
    }
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
