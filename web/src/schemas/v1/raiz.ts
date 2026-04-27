import { z } from 'zod';

/**
 * Linha do `raizes.parquet`: uma raiz CNPJ (8 primeiros dígitos) com agregados
 * sobre seus estabelecimentos. Otimizado para autocomplete por nome.
 *
 * Ver ADR 0008.
 */
export const RaizSchema = z.object({
  cnpj_base: z.string(),
  razao_social: z.string(),
  razao_social_normalizada: z.string(),
  natureza_juridica_codigo: z.string(),
  natureza_juridica_descricao: z.string(),
  capital_social: z.number(),
  porte_empresa: z.enum(['00', '01', '03', '05']),
  ente_federativo_responsavel: z.string().nullable(),

  // Agregados sobre os estabelecimentos
  qtd_estabelecimentos: z.number().int(),
  qtd_estabelecimentos_ativos: z.number().int(),
  ufs_atuacao: z.array(z.string()),
  cnaes_principais_distintos: z.array(z.string()),

  // Snapshot dos campos da matriz (cnpj_ordem = 0001)
  data_inicio_atividade_matriz: z.string(),
  uf_matriz: z.string(),
  municipio_matriz_codigo: z.string(),
  municipio_matriz_nome: z.string(),
  cnae_principal_matriz_codigo: z.string(),
  cnae_principal_matriz_descricao: z.string(),
});

export type Raiz = z.infer<typeof RaizSchema>;
