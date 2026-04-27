import { z } from 'zod';

/**
 * Linha do `cnpjs.parquet`: um estabelecimento completo, com dados da Empresa,
 * Simples e descrições de lookups inline (CNAE, Município, etc.).
 *
 * Ver ADR 0008 e 0009.
 */
export const EstabelecimentoSchema = z.object({
  // Identidade
  cnpj: z.string(),
  cnpj_base: z.string(),
  cnpj_ordem: z.string(),
  cnpj_dv: z.string(),
  identificador_matriz_filial: z.enum(['1', '2']),

  // Empresa (denormalizado da raiz)
  razao_social: z.string(),
  razao_social_normalizada: z.string(),
  natureza_juridica_codigo: z.string(),
  natureza_juridica_descricao: z.string(),
  qualificacao_responsavel_codigo: z.string(),
  qualificacao_responsavel_descricao: z.string(),
  capital_social: z.number(),
  porte_empresa: z.enum(['00', '01', '03', '05']),
  ente_federativo_responsavel: z.string().nullable(),

  // Estabelecimento
  nome_fantasia: z.string().nullable(),
  situacao_cadastral: z.enum(['01', '02', '03', '04', '08']),
  situacao_cadastral_descricao: z.string(),
  data_situacao_cadastral: z.string(),
  motivo_situacao_cadastral_codigo: z.string(),
  motivo_situacao_cadastral_descricao: z.string(),
  data_inicio_atividade: z.string(),

  // CNAE
  cnae_principal_codigo: z.string(),
  cnae_principal_descricao: z.string(),
  cnae_secundario_codigos: z.array(z.string()),
  cnae_secundario_descricoes: z.array(z.string()),

  // Endereço
  tipo_logradouro: z.string().nullable(),
  logradouro: z.string().nullable(),
  numero: z.string().nullable(),
  complemento: z.string().nullable(),
  bairro: z.string().nullable(),
  cep: z.string().nullable(),
  uf: z.string(),
  municipio_codigo: z.string(),
  municipio_nome: z.string(),
  pais_codigo: z.string().nullable(),
  pais_nome: z.string().nullable(),
  nome_cidade_exterior: z.string().nullable(),

  // Contato
  ddd_1: z.string().nullable(),
  telefone_1: z.string().nullable(),
  ddd_2: z.string().nullable(),
  telefone_2: z.string().nullable(),
  ddd_fax: z.string().nullable(),
  fax: z.string().nullable(),
  correio_eletronico: z.string().nullable(),

  // Situação especial
  situacao_especial: z.string().nullable(),
  data_situacao_especial: z.string().nullable(),

  // Simples / MEI (inline)
  opcao_simples: z.boolean().nullable(),
  data_opcao_simples: z.string().nullable(),
  data_exclusao_simples: z.string().nullable(),
  opcao_mei: z.boolean().nullable(),
  data_opcao_mei: z.string().nullable(),
  data_exclusao_mei: z.string().nullable(),
});

export type Estabelecimento = z.infer<typeof EstabelecimentoSchema>;
