import { z } from 'zod';

/**
 * Linha do `socios.parquet`: um vínculo de sociedade. Sócios PF, PJ e
 * estrangeiros convivem no mesmo Parquet, distinguidos pelo campo `tipo`.
 *
 * Layout "fat row" — campos só aplicáveis a um tipo são nullable nos demais.
 * Esse formato casa melhor com Parquet (colunar) do que uma união discriminada.
 *
 * Política LGPD: dados publicados conforme RFB original (ver ADR 0004 e
 * decisão registrada em discussão prévia).
 */
export const SocioSchema = z.object({
  cnpj_base: z.string(),

  // Discriminador: '1' = PJ, '2' = PF, '3' = estrangeiro (códigos da RFB)
  tipo: z.enum(['1', '2', '3']),
  tipo_descricao: z.enum(['PJ', 'PF', 'estrangeiro']),

  nome_socio_razao_social: z.string(),

  // CPF mascarado quando tipo=PF; vazio para estrangeiro; ausente para PJ
  cpf_mascarado: z.string().nullable(),
  // CNPJ do sócio quando tipo=PJ
  cnpj_socio: z.string().nullable(),

  qualificacao_codigo: z.string(),
  qualificacao_descricao: z.string(),
  data_entrada_sociedade: z.string(),

  pais_codigo: z.string().nullable(),
  pais_nome: z.string().nullable(),

  // Representante legal (presente para sócios menores de idade ou alguns casos PJ)
  representante_legal_cpf: z.string().nullable(),
  representante_legal_nome: z.string().nullable(),
  representante_legal_qualificacao_codigo: z.string().nullable(),
  representante_legal_qualificacao_descricao: z.string().nullable(),

  // Faixa etária (sócios PF) — códigos 1-9 e 0 (não se aplica)
  faixa_etaria: z.enum(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']).nullable(),
});

export type Socio = z.infer<typeof SocioSchema>;
