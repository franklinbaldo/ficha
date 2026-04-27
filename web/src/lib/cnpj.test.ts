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
