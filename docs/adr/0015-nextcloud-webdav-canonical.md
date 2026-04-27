# ADR 0015 — Nextcloud WebDAV é a fonte canônica (com histórico desde 2023-05)

**Status:** Accepted
**Data:** 2026-04-27
**Supersedes:** [ADR 0014](0014-rfb-flat-url-no-token.md) — a URL flat tem só snapshot atual; WebDAV tem 35 meses de histórico

## Contexto

[ADR 0014](0014-rfb-flat-url-no-token.md) concluiu que a URL flat
`dadosabertos.rfb.gov.br/CNPJ/{filename}` era a fonte canônica e que a RFB
não preservava histórico. Investigação adicional via probe diagnóstico (PR #10)
mostrou que **a conclusão estava parcialmente errada**:

- A URL flat existe e tem apenas o snapshot mais recente (correto).
- Mas existe um **share Nextcloud público** com histórico completo desde
  maio/2023, acessível via WebDAV padrão.

A confusão veio do fato de que o pattern do br-acc usava
`/s/{TOKEN}/download?path=...` (URL "bonita" do Nextcloud que requer
rewrite no servidor) em vez de `/public.php/webdav/` (WebDAV padrão).
O share existe e o token funciona — só estávamos batendo no endpoint errado.

## Decisão

Adotar **Nextcloud WebDAV como fonte canônica do FICHA**.

### URL

```
https://arquivos.receitafederal.gov.br/public.php/webdav/
```

Acesso via Basic auth com `username = TOKEN` e password vazio.

### Token

```
YggdBLfdninEJX9
```

Estável desde pelo menos maio/2023 (mtime das pastas mais antigas).
Mantido em `KNOWN_TOKENS` em `etl/src/ficha_etl/upstream.py`. Pode ser
sobrescrito via env var `CNPJ_SHARE_TOKEN`.

### Estrutura confirmada

```
/public.php/webdav/
├── 2023-05/   ~6.0 GB    ← mais antigo
├── 2023-06/   ~6.0 GB
├── ...
├── 2025-12/   ~7.3 GB
├── 2026-01/   ~7.3 GB
├── 2026-02/   ~7.4 GB
├── 2026-03/   ~7.4 GB
├── 2026-04/   ~7.5 GB    ← mais recente
└── cnpj.tar.gz  63.9 GB  ← legacy bundle, ignorado (last-modified jan/2026)
```

35 pastas mensais. Cada pasta tem 37 ZIPs (10 Empresas + 10 Estabelecimentos
+ 10 Socios + Simples + Cnaes + Motivos + Municipios + Naturezas + Paises
+ Qualificacoes), com tamanhos variando de 980 bytes (`Qualificacoes.zip`)
a 1.9 GB (`Estabelecimentos0.zip`).

### Operações WebDAV usadas

| Método | URL | Propósito |
|---|---|---|
| `PROPFIND` Depth: 1 | `/public.php/webdav/` | Listar snapshots disponíveis |
| `PROPFIND` Depth: 1 | `/public.php/webdav/{YYYY-MM}/` | Listar 37 ZIPs do snapshot |
| `GET` | `/public.php/webdav/{YYYY-MM}/{filename}` | Download (suporta Range para resume) |

Auth Basic em todas as requests, body XML padrão WebDAV pra PROPFIND.

## Consequências

- ✅ **35 meses de histórico imediatamente disponíveis** — backfill do mirror IA pode rodar de uma vez ou progressivo.
- ✅ Token estável, sem necessidade de discovery dinâmico (KISS).
- ✅ PROPFIND dá `name + size + mtime + etag` por arquivo — diff entre snapshots é trivial (mtime/etag mudou = re-download).
- ✅ Range requests funcionam → resume parcial ainda ajuda em snapshot grande de um GB.
- ⚠️ Se a RFB rotacionar o token (improvável, mas possível), `KNOWN_TOKENS` precisa de update via PR.
- ⚠️ Dependência do Nextcloud share continuar publicado pela RFB. Se sumir, o mirror IA é a única fonte (reforça [ADR 0012](0012-ia-mirror-as-source-of-truth.md)).

## Reforço com ADR 0012

ADR 0012 (IA como source-of-truth) **continua em vigor e fica reforçado**.
A diferença vs. ADR 0014:

- Antes (ADR 0014): IA precisava ser populado prospectivamente; sem
  histórico anterior à primeira rodada do FICHA.
- Agora (ADR 0015): IA pode ser populado retroativamente com 35 meses
  já existentes na RFB.

## Supersedes ADR 0014

A "URL flat sem histórico" descrita em ADR 0014 ainda é fato real (existe
em `dadosabertos.rfb.gov.br/CNPJ/`), mas já não é a fonte preferida.
Nextcloud WebDAV é estritamente superior:

| Aspecto | Flat URL (ADR 0014) | Nextcloud WebDAV (ADR 0015) |
|---|---|---|
| Histórico | Só atual | 35 meses |
| Listagem programática | Não | PROPFIND |
| Auth | Não | Basic (token público) |
| Range requests | Sim | Sim |
| Estabilidade do endpoint | Desconhecida | Confirmado funcional desde 2023-05 |

ADR 0014 fica como histórico do raciocínio inicial.
