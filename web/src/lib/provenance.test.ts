import { describe, expect, it } from 'vitest';
import { archiveItemFrom, formatDay, verificationClaim } from './provenance';

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

const shards = (n = 100, sha1 = 'a'.repeat(40)) =>
	Array.from({ length: n }, (_, i) => ({
		shard: String(i).padStart(2, '0'),
		url: `https://archive.org/download/ficha-2026-05/companies-${String(i).padStart(2, '0')}.zip`,
		sha1,
		size: 1,
	}));

const analiticos = {
	cnpjs: hashed('a'.repeat(64)),
	raizes: hashed('b'.repeat(64)),
	socios: hashed('c'.repeat(64)),
	lookups: hashed('d'.repeat(64)),
};

describe('verificationClaim — contrato monolítico (legado, 2026-04)', () => {
	it('afirma SHA-256 quando todo arquivo traz sha256', () => {
		expect(verificationClaim(analiticos as never)).toBe('sha256');
	});

	it('inclui companies_zip sem virar contrato misto', () => {
		expect(
			verificationClaim({ ...analiticos, companies_zip: hashed('e'.repeat(64)) } as never)
		).toBe('sha256');
	});

	it('não afirma nada quando um único arquivo não tem sha256', () => {
		expect(
			verificationClaim({ ...analiticos, raizes: { url: 'https://x/y', size: 1 } } as never)
		).toBeNull();
	});

	it('não afirma nada quando um sha256 é string vazia', () => {
		expect(verificationClaim({ ...analiticos, socios: hashed('') } as never)).toBeNull();
	});

	it('não afirma nada quando não há arquivos', () => {
		expect(verificationClaim({} as never)).toBeNull();
	});
});

describe('verificationClaim — contrato shardado (2026-05)', () => {
	it('afirma o contrato misto quando os 100 shards têm SHA-1 válido', () => {
		expect(
			verificationClaim({
				...analiticos,
				companies: { shard_by: 'cnpj_base_prefix_2', shards: shards() },
			} as never)
		).toBe('sha256+sha1-shards');
	});

	// O ponto da correção: antes, `companies` entrava em Object.values() como se
	// fosse um FileEntry, não tinha sha256, e a faixa sumia inteira num snapshot
	// que na verdade é verificável de duas formas.
	it('não regride para "sem afirmação" só por existir a camada shardada', () => {
		expect(
			verificationClaim({
				...analiticos,
				companies: { shard_by: 'cnpj_base_prefix_2', shards: shards() },
			} as never)
		).not.toBeNull();
	});

	it('não afirma nada com conjunto incompleto de shards', () => {
		expect(
			verificationClaim({
				...analiticos,
				companies: { shard_by: 'cnpj_base_prefix_2', shards: shards(99) },
			} as never)
		).toBeNull();
	});

	it.each([
		['sha256 no lugar de sha1', 'a'.repeat(64)],
		['hex curto', 'a'.repeat(39)],
		['maiúsculas', 'A'.repeat(40)],
		['não-hex', 'z'.repeat(40)],
		['vazio', ''],
	])('não afirma nada quando o SHA-1 é inválido — %s', (_rotulo, sha1) => {
		expect(
			verificationClaim({
				...analiticos,
				companies: { shard_by: 'cnpj_base_prefix_2', shards: shards(100, sha1) },
			} as never)
		).toBeNull();
	});

	it('não afirma nada quando os analíticos falham, mesmo com shards válidos', () => {
		expect(
			verificationClaim({
				...analiticos,
				socios: { url: 'https://x/y', size: 1 },
				companies: { shard_by: 'cnpj_base_prefix_2', shards: shards() },
			} as never)
		).toBeNull();
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
