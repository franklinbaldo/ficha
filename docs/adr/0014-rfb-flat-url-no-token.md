# ADR 0014 — RFB usa URL flat sem token; histórico fica no mirror IA

**Status:** Accepted
**Data:** 2026-04-27
**Supersedes:** [ADR 0013](0013-rfb-token-discovery.md) — token discovery não é necessário; complementa [ADR 0010](0010-rfb-source-url.md) com URL atual

## Contexto

A primeira tentativa de discovery do canal RFB em 2026 (ADR 0013) baseou-se em código de outros repos brasileiros (br-acc) que apontavam pra **Nextcloud com tokens rotativos**. CI smoke da PR #8 mostrou que essas tokens já não funcionavam, e que mesmo o scraping da página oficial caía numa parede de login do Plone.

Investigação via Playwright (ignore-cert, browser real, locale pt-BR, timezone São Paulo) revelou:

- A URL `gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj` redireciona genericamente pro endpoint `acl_users/credentials_cookie_auth/require_login` — comportamento default do Plone para URLs movidas/inexistentes.
- A URL canônica nova é `gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/cnpj` (sem `/consultas/dados-publicos-cnpj`).
- O **portal autoritativo** é `dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`.
- Os ZIPs estão num **diretório flat** sem mês no path:
  ```
  https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip
  https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos0.zip
  ...
  ```
- Apenas o **snapshot atual** é servido. Conteúdo é sobrescrito a cada release. **Não há histórico** acessível via RFB.

A camada Nextcloud (br-acc) ou foi temporária (transição) ou nunca foi a fonte canônica oficial.

## Decisão

### URL canônica nova

```
https://dadosabertos.rfb.gov.br/CNPJ/{filename}
```

Sem subdiretório de mês. Sem token. Acesso direto via HTTPS público.

### Override

Mantém-se via env var `FICHA_RFB_BASE_URL` (ADR 0010 cláusula de resiliência).

### Histórico

Como a RFB **não preserva snapshots históricos**, [ADR 0012](0012-ia-mirror-as-source-of-truth.md) (IA como source-of-truth) deixa de ser opcional para **operacionalmente necessário**: se o ETL não rodou num mês, o snapshot daquele mês está perdido para sempre.

Cron mensal do ETL é, portanto, função crítica, não apenas conveniência.

## Consequências

- ✅ `upstream.py` simplifica drasticamente — sem token discovery, sem KNOWN_TOKENS, sem scraping.
- ✅ Smoke check vira trivial: HEAD em `dadosabertos.rfb.gov.br/CNPJ/`.
- ✅ Sem operator action recurring para descobrir tokens novos.
- ⚠️ **Falha do cron mensal = perda permanente daquele mês**. Vale alerta agressivo.
- ⚠️ Se a RFB mudar URL de novo, smoke vai falhar com warning claro; PR pequeno atualiza `DEFAULT_RFB_BASE_URL`.

## Supersedes ADR 0013

Token discovery em 3 camadas (env / known / scrape) foi rejeitada após confirmar que tokens nunca foram a fonte canônica oficial pública. ADR 0013 fica como histórico do raciocínio inicial, mas sua decisão **não está mais em vigor**.

## Mantido em vigor

- [ADR 0010](0010-rfb-source-url.md) — env var `FICHA_RFB_BASE_URL` para override
- [ADR 0012](0012-ia-mirror-as-source-of-truth.md) — IA como source-of-truth (reforçado)
