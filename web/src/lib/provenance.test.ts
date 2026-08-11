import { describe, expect, it } from 'vitest';
import { allFilesHashed, archiveItemFrom, formatDay } from './provenance';

/**
 * A faixa de proveniência faz afirmações públicas sobre onde o dado está
 * preservado e se é verificável. Estes testes cobrem exatamente a fronteira
 * "a UI pode afirmar isso?" — não o CSS.
 */

const hashed = (sha256: string) => ({ url: 'https://x/y', sha256, size: 1 });

describe('archiveItemFrom', () => {
	it('aceita archive.org e extrai o identificador', () => {
		expect(archiveItemFrom('https://archive.org/download/ficha-2026-04/cnpjs.parquet')).toBe(
			'ficha-2026-04'
		);
	});

	it('aceita /details/ além de /download/', () => {
		expect(archiveItemFrom('https://archive.org/details/ficha-2026-04')).toBe('ficha-2026-04');
	});

	it('aceita subdomínio legítimo do Internet Archive', () => {
		expect(archiveItemFrom('https://ia801504.us.archive.org/download/ficha-2026-04/x')).toBe(
			'ficha-2026-04'
		);
	});

	// O ponto do guard: `endsWith('archive.org')` sozinho aceitaria estes.
	it.each([
		'https://notarchive.org/download/ficha-2026-04/cnpjs.parquet',
		'https://evilarchive.org/download/ficha-2026-04/cnpjs.parquet',
		'https://archive.org.example.com/download/ficha-2026-04/cnpjs.parquet',
	])('rejeita host que apenas termina em archive.org: %s', (url) => {
		expect(archiveItemFrom(url)).toBeNull();
	});

	it('rejeita host legítimo sem caminho de item', () => {
		expect(archiveItemFrom('https://archive.org/')).toBeNull();
	});

	it('rejeita URL inválida', () => {
		expect(archiveItemFrom('não é uma url')).toBeNull();
		expect(archiveItemFrom('')).toBeNull();
	});
});

describe('allFilesHashed', () => {
	it('é verdadeiro quando todos os arquivos têm sha256', () => {
		expect(
			allFilesHashed({ cnpjs: hashed('a'.repeat(64)), raizes: hashed('b'.repeat(64)) } as never)
		).toBe(true);
	});

	it('é falso quando um único arquivo não tem sha256', () => {
		expect(
			allFilesHashed({
				cnpjs: hashed('a'.repeat(64)),
				raizes: { url: 'https://x/y', size: 1 },
			} as never)
		).toBe(false);
	});

	it('é falso quando um sha256 é string vazia', () => {
		expect(allFilesHashed({ cnpjs: hashed('') } as never)).toBe(false);
	});

	it('é falso quando não há arquivos — nada a afirmar', () => {
		expect(allFilesHashed({} as never)).toBe(false);
	});
});

describe('formatDay', () => {
	it('formata timestamp ISO em pt-BR', () => {
		expect(formatDay('2026-05-15T01:02:37Z')).toBe('15/05/2026');
	});

	it('devolve null para data inválida', () => {
		expect(formatDay('nunca')).toBeNull();
	});
});
