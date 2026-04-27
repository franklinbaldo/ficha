# ADR 0013 — Estratégia de discovery do token Nextcloud da RFB

**Status:** Accepted
**Data:** 2026-04-27
**Complementa:** [ADR 0012](0012-ia-mirror-as-source-of-truth.md)

## Contexto

Conforme [ADR 0012](0012-ia-mirror-as-source-of-truth.md), o ETL precisa baixar dumps mensais do Nextcloud da RFB **uma vez por release**, pra popular o item `ficha-YYYY-MM` no Internet Archive. Cada release tem um token único:

```
https://arquivos.receitafederal.gov.br/s/{TOKEN}/download?path=%2F&files=Empresas0.zip
```

Tokens são strings opacas tipo `gn672Ad4CF8N6TK`, rotacionadas pela RFB sem aviso ou padrão público.

## Decisão

Discovery em **três camadas com fallback explícito**:

### 1. Variável de ambiente `CNPJ_SHARE_TOKEN`
Override manual. Se definida, usar essa token sem mais perguntas. Caso de uso: operador soube do token novo via canal não-automatizado e quer rodar manualmente.

```bash
CNPJ_SHARE_TOKEN=ABC123def ficha-etl run --month 2026-03
```

### 2. Lista de tokens conhecidos (hardcoded)
Tokens que já funcionaram em algum momento. Tentamos cada um até achar um que responda. Atualiza-se via PR quando descobrirmos um novo. Inicial:

```python
KNOWN_TOKENS = [
    "gn672Ad4CF8N6TK",  # observado em br-acc/etl
    "YggdBLfdninEJX9",  # observado em br-acc/etl
]
```

Pragmático mas não dá pra confiar a longo prazo — RFB pode invalidar todos sem aviso.

### 3. Scrape da página oficial
Last resort. Faz GET na página oficial de dados abertos do CNPJ e procura o link de download mais recente:

```
https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj
```

Extrai padrão `arquivos.receitafederal.gov.br/s/{TOKEN}` do HTML. Funciona enquanto a estrutura do portal for estável.

### Ordem de tentativa

```
discover_token():
  1. env CNPJ_SHARE_TOKEN set?       → return it (no validation)
  2. for token in KNOWN_TOKENS:
       does it respond 200 to a HEAD on the share root?  → return it
  3. scrape gov.br page for s/{TOKEN}  → return first match (validated via HEAD)
  4. raise NoTokenFoundError
```

## Por que essa ordem

- **Env primeiro** — operador override sempre vence
- **Conhecidos antes do scrape** — HTTP barato (HEAD), HTML scrape é mais frágil
- **Scrape por último** — caro, mas é o único que se adapta a tokens completamente novos

## Consequências

- ✅ Resiliente a 80% dos cenários sem intervenção humana
- ✅ Operator pode forçar override quando souber via canal externo
- ✅ Quando KNOWN_TOKENS expira E scrape quebra, falha com mensagem clara → cria issue → atualiza repo
- ⚠️ Scrape depende de HTML do portal gov.br — frágil
- ⚠️ Tokens conhecidos viram dívida técnica — precisa periodicamente limpar os mortos

## Mitigação operacional

- Workflow `etl-smoke.yml` (já existente) roda semanalmente e valida token discovery
- Quando todos os 3 níveis falharem, smoke alerta via comment no PR / actions failure email
- Issue template "RFB token rotation" pra operador atualizar `KNOWN_TOKENS` rapidamente

## Sinal pra reconsiderar

Se discovery scrape quebrar mais de 2 vezes em 12 meses, vale investigar:

- Existe RSS feed oficial de releases?
- Algum API governamental documentado?
- Acordo direto com SERPRO/RFB?

Por enquanto, scrape é suficiente.
