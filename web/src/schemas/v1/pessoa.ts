import { z } from 'zod';

/**
 * Linha do `pessoas.parquet`: reverse lookup PF por CPF mascarado + nome.
 *
 * Inclui sócios PF (papel='socio_pf') e representantes legais
 * (papel='representante'). Exclui sócios PJ e estrangeiros sem CPF.
 *
 * Grão: (cpf_mascarado, nome_normalizado, cnpj_base, papel) — uma linha
 * por pessoa × empresa × papel. `data_entrada_sociedade` e `faixa_etaria`
 * foram removidos porque são propriedades do vínculo (sócio×empresa),
 * não da pessoa, e são sempre NULL para representantes. Ver ADR 0024.
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
});

export type Pessoa = z.infer<typeof PessoaSchema>;
