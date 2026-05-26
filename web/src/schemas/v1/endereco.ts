import { z } from 'zod';

/**
 * Linha do `enderecos.parquet`: reverse lookup de CNPJ por endereço e município.
 *
 * Parquet ordenado por (uf, municipio_codigo, logradouro_normalizado, numero)
 * para que buscas prefix e range usem min/max row-group pruning sem scan total.
 *
 * Ver ADR 0023.
 */
export const EnderecoSchema = z.object({
  uf: z.string().length(2),
  municipio_codigo: z.string(),

  // Logradouro normalizado: UPPER + strip_accents + abreviações expandidas
  // (R → RUA, AV → AVENIDA, etc.). Usado para buscas consistentes.
  logradouro_normalizado: z.string(),

  numero: z.string().nullable(),
  cep: z.string().nullable(),
  bairro: z.string().nullable(),

  // CNPJ completo de 14 dígitos (sem formatação)
  cnpj: z.string().length(14),
});

export type Endereco = z.infer<typeof EnderecoSchema>;
