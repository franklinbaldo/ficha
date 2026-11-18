import { describe, it, expect } from 'vitest';
import { isValid, asCNPJ, format, strip } from './cnpj';

// Real-ish CNPJs used as fixtures (validated externally).
const VALID = '11.222.333/0001-81';
const VALID_RAW = '11222333000181';

describe('cnpj.strip', () => {
  it('removes formatting', () => {
    expect(strip(VALID)).toBe(VALID_RAW);
  });
  it('handles already-clean input', () => {
    expect(strip(VALID_RAW)).toBe(VALID_RAW);
  });
});

describe('cnpj.isValid', () => {
  it('accepts valid formatted CNPJ', () => {
    expect(isValid(VALID)).toBe(true);
  });

  it('accepts valid raw CNPJ', () => {
    expect(isValid(VALID_RAW)).toBe(true);
  });

  it('rejects wrong-length input', () => {
    expect(isValid('123')).toBe(false);
    expect(isValid('1'.repeat(15))).toBe(false);
  });

  it('rejects all-equal-digit pseudo-CNPJs', () => {
    expect(isValid('00000000000000')).toBe(false);
    expect(isValid('11111111111111')).toBe(false);
  });

  it('rejects bad check digits', () => {
    expect(isValid('11222333000180')).toBe(false); // last digit wrong
    expect(isValid('11222333000191')).toBe(false); // both digits wrong
  });

  it('rejects non-numeric input', () => {
    expect(isValid('abc')).toBe(false);
    expect(isValid('')).toBe(false);
  });
});

describe('cnpj.asCNPJ', () => {
  it('returns stripped value for valid CNPJ', () => {
    expect(asCNPJ(VALID)).toBe(VALID_RAW);
  });
  it('returns null for invalid', () => {
    expect(asCNPJ('00000000000000')).toBeNull();
  });
});

describe('cnpj.format', () => {
  it('formats raw 14 digits', () => {
    expect(format(VALID_RAW)).toBe(VALID);
  });
  it('passes through unrecognized input unchanged', () => {
    expect(format('abc')).toBe('abc');
  });
});

// RFB's first issued alphanumeric CNPJ, 31 July 2026 (Banco do Brasil filial).
describe('CNPJ alfanumérico RFB', () => {
  it('preserves letters and validates the official ASCII-minus-48 check digits', () => {
    expect(strip('00.000.000/e08g-12')).toBe('00000000E08G12');
    expect(isValid('00000000E08G12')).toBe(true);
    expect(isValid('12.ABC.345/01DE-35')).toBe(true); // RFB worked example
    expect(asCNPJ('00.000.000/E08G-12')).toBe('00000000E08G12');
    expect(format('00000000E08G12')).toBe('00.000.000/E08G-12');
  });
  it('rejects wrong DV, letters in DV and symbols rather than silently deleting them', () => {
    for (const bad of ['00000000E08G13', '00000000E08G1A', '00000000E08@12', '$11222333000181']) {
      expect(isValid(bad)).toBe(false);
    }
  });
});
