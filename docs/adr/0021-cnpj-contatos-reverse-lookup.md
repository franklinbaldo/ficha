# ADR 0021 — `cnpj_contatos.parquet`: reverse lookup de contatos

**Status:** Aceito
**Data:** 2026-07-14
**Contexto:** docs/perf-plan-2026-05.md §12

---

## Contexto

`cnpjs.parquet` carrega 7 colunas de contato por estabelecimento (`ddd_1`,
`telefone_1`, `ddd_2`, `telefone_2`, `ddd_fax`, `fax`, `correio_eletronico`).
São, na prática, uma lista multi-valorada colapsada em colunas largas — não dá
para responder "quem tem este telefone?" ou "todos os CNPJs com e-mail
`@prefeitura.sp.gov.br`" sem full scan.

## Decisão

Produzir `cnpj_contatos.parquet` (`write_cnpj_contatos_parquet`,
`transform.py:995`), uma linha por contato:

| Coluna | Tipo | Notas |
|--------|------|-------|
| `cnpj` | VARCHAR(14) | CNPJ completo |
| `cnpj_base` | VARCHAR(8) | raiz do CNPJ |
| `tipo` | VARCHAR | `'telefone'`, `'fax'` ou `'email'` |
| `valor` | VARCHAR | `ddd || telefone` para telefone/fax; endereço para email |
| `posicao` | INTEGER | telefone: `1` ou `2`; fax e email: `0` |

`UNION ALL` de 4 branches sobre `estabelecimento` (telefone_1, telefone_2, fax,
email), cada um filtrando linhas vazias antes de concatenar.

**Sort:** `(tipo, valor, cnpj)`.

**Postura de privacidade:** telefones e e-mails são PII, mas a RFB já os
publica publicamente nos dumps originais. Este parquet é um re-shape puro dos
mesmos dados públicos — sem enriquecimento, sem nova exposição. As colunas
largas em `cnpjs.parquet` permanecem (alimentam a lâmina); este arquivo é só o
índice inverso.

## Consequências

- ✅ "Quem tem este telefone/e-mail?" e "detectar contato compartilhado entre
  CNPJs" (`GROUP BY valor HAVING count(*) > 1`) viram queries baratas por
  sort prefix em vez de full scan de `cnpjs.parquet`.
- ✅ Construído só a partir de `estabelecimento` — sem joins.
- Frontend usa `attachCnpjContatos(db, url)`
  (`web/src/lib/analytical.ts:118`), consumido pelo botão "Contatos" em
  `EmpresaFicha.svelte`.
- ⚠️ Depende de upload bem-sucedido no Internet Archive — o snapshot 2026-04
  publicado ficou sem este arquivo por falha de upload (ver
  `docs/vision-blockers-2026-07.md`), então o botão "Contatos" falha
  silenciosamente quando o parquet está ausente do manifest.
