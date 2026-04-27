/**
 * Validação e formatação de CNPJ.
 *
 * O CNPJ tem 14 dígitos: 12 dígitos base (8 raiz + 4 ordem) + 2 dígitos
 * verificadores calculados pelo módulo 11.
 */

export type CNPJ = string & { readonly __brand: 'CNPJ' };

const STRIP_RE = /\D/g;
const CNPJ_RE = /^\d{14}$/;

/** Remove qualquer caractere não-numérico. */
export function strip(input: string): string {
  return input.replace(STRIP_RE, '');
}

/** Calcula um dígito verificador conforme regra módulo 11 da Receita Federal. */
function checkDigit(base: string, weights: number[]): number {
  const sum = base
    .split('')
    .reduce((acc, ch, i) => acc + Number(ch) * weights[i]!, 0);
  const remainder = sum % 11;
  return remainder < 2 ? 0 : 11 - remainder;
}

const W1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
const W2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];

/**
 * Valida um CNPJ checando dígitos verificadores.
 * Aceita string formatada ou crua.
 */
export function isValid(input: string): boolean {
  const digits = strip(input);
  if (!CNPJ_RE.test(digits)) return false;
  if (/^(\d)\1+$/.test(digits)) return false; // todos dígitos iguais

  const base12 = digits.slice(0, 12);
  const dv1 = checkDigit(base12, W1);
  if (dv1 !== Number(digits[12])) return false;
  const dv2 = checkDigit(base12 + dv1, W2);
  return dv2 === Number(digits[13]);
}

/**
 * Type guard que confirma o input como CNPJ válido (14 dígitos puros).
 * Use depois de `isValid` para narrowing.
 */
export function asCNPJ(input: string): CNPJ | null {
  return isValid(input) ? (strip(input) as CNPJ) : null;
}

/** Formata 14 dígitos para `XX.XXX.XXX/XXXX-XX`. */
export function format(cnpj: string): string {
  const d = strip(cnpj);
  if (d.length !== 14) return cnpj;
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12, 14)}`;
}
