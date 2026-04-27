# ADR 0004 — Internet Archive como storage primário

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

FICHA precisa hospedar Parquets de ~3-5GB e ZIPs de fichas atômicas para o público acessar via HTTP, com:

- Custo zero ou simbólico
- Disponibilidade longa (anos, idealmente décadas)
- Suporte a `Range` requests (DuckDB-WASM precisa)
- Sem vendor lock-in operacional

## Decisão

Usar **Internet Archive (archive.org)** como storage primário, com **um item por snapshot mensal** (`ficha-YYYY-MM`).

## Por que IA

- ✅ **Custo zero** para dados públicos.
- ✅ **Persistência institucional** — o IA é uma das infraestruturas culturais mais estáveis da web.
- ✅ **`Range` requests funcionam** — DuckDB-WASM faz queries seletivas sem baixar o Parquet inteiro.
- ✅ **Funcionalidade nativa de "unzip transparente"** — permite acesso direto a `archive.org/.../ZIP/{cnpj}.json` sem descompactar tudo.
- ✅ **API S3-like** (`internetarchive` Python lib) facilita upload via GitHub Actions.
- ⚠️ **Latência variável** — acceptable, mitigado por cache TanStack Query no client.
- ⚠️ **CORS** — IA serve com headers permissivos; checado.

## Estrutura

- **1 item por snapshot mensal**: `ficha-2026-01`, `ficha-2026-02`, ...
- Cada item contém: `ficha-YYYY-MM.parquet`, `ficha-YYYY-MM.zip` (fichas JSON), `metadata.json`.
- **Manifest** vive no repo (`web/public/manifest.json`), não no IA — atomicidade de deploy. Ver [ADR 0003](0003-schema-versioning.md).

## Consequências

- ✅ Vision "dados como infraestrutura aberta" alinhada.
- ✅ Site Astro pode rodar em qualquer host (GH Pages); IA é só CDN de dados.
- ⚠️ Dependência de uma instituição externa (IA). Risco baixo mas existe.
- ⚠️ Throughput de upload limitado (~MB/s). Aceitável para snapshots mensais.

## Alternativas consideradas

- **R2/B2/S3**: custo previsível mas pago, e perdemos persistência institucional.
- **GitHub Releases**: limite 2GB por arquivo, inviável.
- **Hugging Face Datasets**: ótima alternativa, mas IA é mais estável a longo prazo.
- **IPFS**: pinning sustentável é caro; latência ruim.

## Reversibilidade

Se IA falhar, o ETL pode publicar para outro destino (R2, HF) trocando só `etl/src/ficha_etl/upload.py`. Frontend muda só URLs no `manifest.json`. Acoplamento baixo.
