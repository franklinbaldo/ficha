# ADR 0012 — Internet Archive como source-of-truth do FICHA

**Status:** Accepted
**Data:** 2026-04-27
**Supersedes (parcialmente):** [ADR 0010](0010-rfb-source-url.md) — RFB deixa de ser fonte direta do ETL

## Contexto

A primeira versão do ETL (PR #8) assumiu que dumps mensais da RFB ficavam em URLs estáveis e previsíveis (`{base}/{YYYY-MM}/Empresas0.zip`). Smoke check em GitHub Actions confirmou que essa premissa morreu:

| Domínio testado | Resultado | Status |
|---|---|---|
| `arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` | 404 universal | descontinuado |
| `dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/` | timeout (DNS/down) | descontinuado |

A RFB migrou pra **Nextcloud share com tokens rotativos** por release:

```
https://arquivos.receitafederal.gov.br/s/{TOKEN}/download?path=%2F&files=Empresas0.zip
```

Tokens expiram/rotam sem aviso, o que é incompatível com a vision de "URL previsível" do FICHA.

## Decisão

**O Internet Archive vira o source-of-truth do FICHA.** A RFB Nextcloud passa a ser tratada como **upstream descartável** que populamos no IA uma vez e descartamos.

### Arquitetura nova

```
┌──────────────────────┐
│  RFB Nextcloud       │  ← tokens rotativos, fonte instável, upstream descartável
│  (gov.br portal)     │
└──────────┬───────────┘
           │ download mensal (token discovery — ver ADR 0013)
           ▼
┌──────────────────────────────────────────┐
│  ficha-YYYY-MM @ Internet Archive        │  ← source-of-truth, URL estável
│  ├── raw/Empresas0.zip ... (mirror)      │
│  ├── cnpjs.parquet                       │
│  ├── raizes.parquet                      │
│  ├── socios.parquet                      │
│  └── lookups.json                        │
└──────────┬───────────────────────────────┘
           │ HTTP range requests
           ▼
┌──────────────────────┐
│  Frontend (Astro)    │  ← consome só do IA, nunca da RFB
└──────────────────────┘
```

### O que muda no item IA `ficha-YYYY-MM`

Adiciona **`raw/`** com os ZIPs originais da RFB do mês. Vantagens:

- ✅ Backup oficial — se Nextcloud da RFB sumir 100%, FICHA tem cópia íntegra
- ✅ Re-transformação sem re-download — basta puxar do IA, transform, re-upload
- ✅ Auditoria — qualquer um pode comparar nosso parquet contra o ZIP cru do mesmo item
- ✅ Vision-aligned — "Internet Archive como infraestrutura ativa de dados" deixa de ser slogan e vira literal

### Custo de espaço

Dump RFB mensal cru ~5-10GB. IA aceita pacote inteiro de até 100s GB sem reclamar. Dado que IA é gratuito pra dados públicos, custo zero.

## Consequências

- ✅ FICHA fica **resiliente** a mudanças unilaterais da RFB no canal de distribuição.
- ✅ Bootstrap problem fica isolado: token discovery só matters no momento de popular um snapshot novo.
- ✅ Smoke check do CI passa a ter dois alvos úteis: "consigo discoverar token RFB?" e "IA está acessível?".
- ⚠️ Workflow ETL precisa de etapa nova de "upload raw to IA" antes do transform.
- ⚠️ Cold start pra um mês novo: requires both token discovery + IA upload to succeed. Se RFB mudar tudo, single month perdido — não cascateia.

## Relação com outros ADRs

- **Supersedes parcialmente [ADR 0010](0010-rfb-source-url.md)** — URL canônica da RFB já não é fonte direta. ADR 0010 fica como histórico.
- **Complementa [ADR 0008](0008-three-parquet-architecture.md)** — adiciona `raw/` ao item mensal, sem mudar a estrutura dos Parquets.
- **Complementa [ADR 0004](0004-internet-archive-as-storage.md)** — IA agora é fonte primária *e* mirror, não só destino de publicação.
- **Complementado por [ADR 0013](0013-rfb-token-discovery.md)** — estratégia concreta de descobrir o token Nextcloud da RFB.
