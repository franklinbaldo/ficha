# ADR 0004 — Internet Archive como storage primário

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

FICHA precisa hospedar Parquets de vários gigabytes e ZIPs de fichas atômicas para o público acessar via HTTP, com:

- Custo zero ou simbólico
- Disponibilidade longa (anos, idealmente décadas)
- Suporte a `Range` requests (DuckDB-WASM precisa)
- Sem vendor lock-in operacional

## Decisão

Usar **Internet Archive (archive.org)** como storage primário, com **um item por snapshot mensal** (`ficha-YYYY-MM`).

## Por que IA

- ✅ **Custo zero** para dados públicos.
- ✅ **Persistência institucional** — o IA é uma das infraestruturas culturais mais estáveis da web.
- ⚠️ **`Range` HTTP funciona em teste pontual**, mas não comprova leitura por DuckDB-WASM: CORS precisa funcionar também. Ver adendo de 18/11/2026.
- ⚠️ **Acesso a membros de ZIP** — o IA oferece URLs de leitura de membros em determinadas condições, mas nomes e formatos (inclusive `.pb`) variam por snapshot; a disponibilidade HTTP deve ser testada, sem pressupor CORS.
- ✅ **API S3-like** (`internetarchive` Python lib) facilita upload via GitHub Actions.
- ⚠️ **Latência variável** — acceptable, mitigado por cache TanStack Query no client.
- ⚠️ **CORS não garantido** — a suposição inicial de headers permissivos foi contrariada por teste real em 18/11/2026. Ver adendo e [#208](https://github.com/franklinbaldo/ficha/issues/208).

## Estrutura

- **1 item por snapshot mensal**: `ficha-2026-01`, `ficha-2026-02`, ...
- Cada item pode conter os artefatos declarados no manifesto daquela competência: por exemplo, `cnpjs.parquet`, `raizes.parquet`, `pessoas.parquet` e shards `companies-NN.zip` com fichas Protobuf. Nomes, formatos e composição variam por snapshot; declaração não prova disponibilidade HTTP/CORS.
- **Manifest** vive no repo (`web/public/manifest.json`), não no IA — atomicidade de deploy. Ver [ADR 0003](0003-schema-versioning.md).

## Consequências

- ✅ Vision "dados como infraestrutura aberta" alinhada.
- ⚠️ Site Astro pode ser hospedado no GH Pages, mas a busca no navegador depende de uma origem de leitura com CORS e HTTP Range; o IA permanece como custódia dos dados.
- ⚠️ Dependência de uma instituição externa (IA). Risco baixo mas existe.
- ⚠️ Throughput de upload limitado (~MB/s). Aceitável para snapshots mensais.

## Alternativas consideradas

- **R2/B2/S3 como custódia primária**: custo de hospedagem pago e troca da preservação institucional escolhida nesta ADR. Uma eventual **origem separada de consulta** com CORS/Range (issue #208) não substitui essa custódia.
- **GitHub Releases**: limite 2GB por arquivo, inviável.
- **Hugging Face Datasets**: ótima alternativa, mas IA é mais estável a longo prazo.
- **IPFS**: pinning sustentável é caro; latência ruim.

## Reversibilidade

Se a origem de consulta falhar, é preciso selecionar uma alternativa com CORS e Range, sem substituir silenciosamente a URL arquivística do IA. O contrato de URL de consulta separado, integridade do espelho, custos e publicação dependem de decisão na [#208](https://github.com/franklinbaldo/ficha/issues/208); não há migração automática somente por troca de URL.

## Adendo de verificação — 18/11/2026 (sem revogar a decisão arquivística)

A decisão de **preservação no Internet Archive** continua válida. O pressuposto operacional de que o navegador pode consultar diretamente todos os Parquets via DuckDB-WASM **não** foi confirmado e não deve aparecer como garantia:

- Para `https://archive.org/download/ficha-2026-07/cnpjs.parquet`, GET somente do intervalo `bytes=0-0`, com `Origin: https://franklinbaldo.github.io`, resultou em `302 → 206` e `Content-Range: bytes 0-0/6076313170`, mas **sem `Access-Control-Allow-Origin`** na resposta final ([medição de 1 byte](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-6972963031)).
- Chromium real sobre o [Astro já publicado](https://franklinbaldo.github.io/ficha/), em 375×812 e 1440×900, bloqueou o acesso por CORS. A aplicação registrou `NetworkError`; a busca por CNPJ ficou desabilitada nessa execução ([evidência, capturas e limites](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-6974257617)). Isso não demonstra indisponibilidade permanente em todos os browsers.
- A futura **origem de consulta** precisa ser distinta da **URL arquivística canônica**, preservar identidade (tamanho e SHA-256 integral do mesmo artefato) e comprovar CORS, `206`, `Content-Range` exposto e leitura efetiva no navegador. Testar somente um byte é prova de transporte, **não de identidade**.
- Não há neste adendo autorização para criar proxy, espelho, recurso de nuvem, gasto, ETL, upload, manifesto novo ou alteração de Pages. A escolha do provedor, o contrato e o orçamento continuam na [issue #208](https://github.com/franklinbaldo/ficha/issues/208).

Este adendo atualiza a evidência de compatibilidade em novembro de 2026, sem reescrever a data da decisão original de abril de 2026.
