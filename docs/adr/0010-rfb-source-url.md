# ADR 0010 — Fonte: dumps da RFB em arquivos.receitafederal.gov.br

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

A Receita Federal Brasileira publica os dados abertos do CNPJ em dumps mensais. A URL e formato mudaram em 2023 (do FTP antigo `ftp.receitafederal.gov.br` para o atual HTTPS).

## Decisão

### URL canônica

```
https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/YYYY-MM/
```

> **Histórico:** o domínio anterior `arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/` foi descontinuado em 2025/2026. Vários repos públicos referem-se a ele como "legacy". Mantido aqui só pra documentar a migração.

### Estrutura por snapshot

```
YYYY-MM/
  Empresas0.zip ... Empresas9.zip          # 10 ZIPs particionados
  Estabelecimentos0.zip ... Estabelecimentos9.zip
  Socios0.zip ... Socios9.zip
  Simples.zip
  Cnaes.zip
  Motivos.zip
  Municipios.zip
  Naturezas.zip
  Paises.zip
  Qualificacoes.zip
```

### Características dos arquivos

- **Encoding:** ISO-8859-1
- **Separador:** `;`
- **Sem header** (posições documentadas em PDF separado pela RFB)
- **Total descomprimido:** ~25-30GB
- **Ordem de publicação:** geralmente entre dias 1-5 do mês corrente, referente ao mês anterior

### Estratégia de override

URL é constante em `etl/src/ficha_etl/sources.py`. Pode ser sobrescrita via env var `FICHA_RFB_BASE_URL` para:

- Testar com mirror alternativo (ex.: backup no IA)
- CI rodando contra fixture local
- Quando RFB mudar URL de novo

```python
RFB_BASE_URL = os.environ.get(
    "FICHA_RFB_BASE_URL",
    "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj"
)
```

### Resiliência

- Se a URL canônica falhar 3 vezes seguidas em produção, ETL falha o workflow do mês — não tenta fontes alternativas silenciosamente.
- Mirror em IA (cópia do dump original do FICHA) é considerado como fallback **manual** — operador roda `ficha-etl run --month X --source-url <mirror>` se necessário.
- Para preservar o dump cru também (não apenas o transformado): `experiments/006-ia-mirror-raw-rfb/` — proposta separada, não bloqueia v0.1.

## Consequências

- ✅ Single source of truth bem documentada.
- ✅ Override por env var permite testes sem hardcode mock.
- ⚠️ Dependência de URL externa que pode mudar — mitigado por env var override + fallback manual.
- ⚠️ Layout RFB pode mudar (campo novo, renomeação) — mitigado pelo schema versioning ([ADR 0003](0003-schema-versioning.md)).

## Alternativas consideradas

- **Múltiplas URLs configuradas** com fallback automático — overengineering pro caso atual; manual fallback resolve.
- **Scrape da página de listagem RFB** — frágil, RFB pode mudar HTML.
- **API externa que serve o dump (BrasilAPI, etc.)** — adiciona terceiro como dependência.
