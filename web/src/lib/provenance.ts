import type { Snapshot } from '../schemas/v1/manifest';

/**
 * Helpers factuais da faixa de proveniência (#141 G2).
 *
 * A faixa afirma coisas sobre origem, preservação e verificação. Cada afirmação
 * só pode aparecer quando o snapshot carregado a sustenta — por isso a fronteira
 * "a UI pode afirmar isso?" mora aqui, isolada e testável, e não dentro do
 * componente.
 */

/**
 * Identificador do item no Internet Archive, extraído da URL real do snapshot.
 *
 * Devolve `null` para qualquer outro host: sem isso a faixa poderia declarar
 * "Preservação · Internet Archive" para uma URL que apenas *termina* em
 * `archive.org` (`notarchive.org`, `evilarchive.org`), o que é uma afirmação
 * factual falsa sobre onde o dado está preservado.
 */
export function archiveItemFrom(url: string): string | null {
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		return null;
	}
	const host = parsed.hostname;
	const isArchiveOrg = host === 'archive.org' || host.endsWith('.archive.org');
	if (!isArchiveOrg) return null;

	const match = parsed.pathname.match(/^\/(?:download|details)\/([^/]+)/);
	return match?.[1] ?? null;
}

/**
 * `true` somente quando TODO arquivo declarado no snapshot traz `sha256`
 * não-vazio. A faixa afirma "SHA-256 por arquivo" — se um único arquivo não
 * tiver hash, a afirmação é falsa.
 */
export function allFilesHashed(files: Snapshot['files']): boolean {
	const entries = Object.values(files ?? {});
	if (entries.length === 0) return false;
	return entries.every(
		(file) => typeof file?.sha256 === 'string' && file.sha256.length > 0
	);
}

/**
 * `"2026-05-15T01:02:37Z"` → `"15/05/2026"`, ou `null` se não parseável.
 *
 * Atenção ao significado: no contrato, `generated_at` é preenchido por
 * `build_snapshot_entry()` com o instante em que o **Ficha gerou a entrada do
 * snapshot** — não é uma data de fechamento da competência pela Receita. A UI
 * precisa rotular isso como "snapshot gerado em"; não existe campo de
 * fechamento de competência no manifesto.
 */
export function formatDay(iso: string): string | null {
	const date = new Date(iso);
	return Number.isNaN(date.getTime())
		? null
		: new Intl.DateTimeFormat('pt-BR', {
				day: '2-digit',
				month: '2-digit',
				year: 'numeric',
			}).format(date);
}
