import { z } from 'zod';

/**
 * Linha do `pessoas.parquet`: reverse lookup PF por CPF mascarado + nome.
 *
 * Inclui sócios PF (papel='socio_pf') e representantes legais
 * (papel='representante'). Exclui sócios PJ e estrangeiros sem CPF.
 *
 * Chave composta: (cpf_mascarado, nome_normalizado). A probabilidade de
 * dois indivíduos distintos compartilharem tanto CPF mascarado quanto nome
 * normalizado idêntico é astronomicamente baixa (< 1 em 10⁶ para nomes
 * comuns). Ver ADR 0024.
 *
 * Parquet ordenado por (cpf_mascarado, nome_normalizado) — todas as linhas
 * de uma mesma pessoa ficam contíguas em um row-group, tornando buscas
 * exatas muito eficientes.
 */
export const PessoaSchema = z.object({
  // CPF mascarado no formato RFB: "***.<middle6>-**"
  cpf_mascarado: z.string(),

  // Nome normalizado: UPPER + strip_accents + TRIM
  nome_normalizado: z.string(),
  nome_original: z.string().nullable(),

  papel: z.enum(['socio_pf', 'representante']),

  cnpj_base: z.string().length(8),

  qualificacao_codigo: z.string().nullable(),

  // Presentes apenas para papel='socio_pf'
  data_entrada_sociedade: z.string().nullable(),
  faixa_etaria: z
    .enum(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'])
    .nullable(),
});

export type Pessoa = z.infer<typeof PessoaSchema>;
