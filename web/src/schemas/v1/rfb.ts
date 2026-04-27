import { z } from 'zod';

export const EmpresaSchema = z.object({
  cnpj_base: z.string(),
  razao_social: z.string(),
  natureza_juridica: z.string().optional(),
  qualificacao_responsavel: z.string().optional(),
  capital_social: z.number().optional(),
  porte_empresa: z.string().optional(),
  ente_federativo_responsavel: z.string().optional(),
});

export const EstabelecimentoSchema = z.object({
  cnpj_base: z.string(),
  cnpj_ordem: z.string(),
  cnpj_dv: z.string(),
  cnpj: z.string().optional(), // Frequentemente concatenado
  identificador_matriz_filial: z.string().optional(),
  nome_fantasia: z.string().optional(),
  situacao_cadastral: z.string().optional(),
  data_situacao_cadastral: z.string().optional(),
  motivo_situacao_cadastral: z.string().optional(),
  nome_cidade_exterior: z.string().optional(),
  pais: z.string().optional(),
  data_inicio_atividade: z.string().optional(),
  cnae_fiscal_principal: z.string().optional(),
  cnae_fiscal_secundaria: z.string().optional(),
  tipo_logradouro: z.string().optional(),
  logradouro: z.string().optional(),
  numero: z.string().optional(),
  complemento: z.string().optional(),
  bairro: z.string().optional(),
  cep: z.string().optional(),
  uf: z.string().optional(),
  municipio: z.string().optional(),
  ddd_1: z.string().optional(),
  telefone_1: z.string().optional(),
  ddd_2: z.string().optional(),
  telefone_2: z.string().optional(),
  ddd_fax: z.string().optional(),
  fax: z.string().optional(),
  correio_eletronico: z.string().optional(),
  situacao_especial: z.string().optional(),
  data_situacao_especial: z.string().optional(),
});

export const SocioSchema = z.object({
  cnpj_base: z.string(),
  identificador_socio: z.string().optional(),
  nome_socio_razao_social: z.string().optional(),
  cpf_cnpj_socio: z.string().optional(),
  qualificacao_socio: z.string().optional(),
  data_entrada_sociedade: z.string().optional(),
  pais: z.string().optional(),
  representante_legal: z.string().optional(),
  qualificacao_representante_legal: z.string().optional(),
  faixa_etaria: z.string().optional(),
});

export type Empresa = z.infer<typeof EmpresaSchema>;
export type Estabelecimento = z.infer<typeof EstabelecimentoSchema>;
export type Socio = z.infer<typeof SocioSchema>;
