# ADR 0026 — Identidade do snapshot nasce na produção

Status: accepted

## Contexto

O backfill de `2026-05` expôs uma falha de ordem no pipeline: os bytes eram
produzidos e enviados ao Internet Archive, mas a identidade completa necessária
à promoção era reconstruída depois. Isso obrigava a baixar Parquets novamente
para recuperar SHA-256 e fazia o catálogo remoto participar da definição do que
deveria ter sido produzido.

Ao mesmo tempo, o Internet Archive já expõe SHA-1 de cada objeto no metadata.
Registrar SHA-1 localmente torna a verificação remota direta e barata. O SHA-256
continua útil como digest forte e contrato de consumidor.

## Decisão

Para snapshots novos, a publicação segue esta ordem:

1. produzir os outputs locais;
2. calcular e persistir `size + sha1 + sha256` dos Parquets e lookups;
3. persistir esse **production descriptor** como artifact antes do upload;
4. materializar cada `companies-NN.zip` e persistir seu receipt
   `size + sha1 + materialization_id` antes do PUT correspondente;
5. submeter exatamente os bytes descritos pelos receipts;
6. verificar no IA que `size + sha1` dos outputs-base e dos 100 shards são
   idênticos ao descriptor/receipts e que cada `_meta.json` declara o
   `MaterializationSpec` esperado;
7. promover exatamente essa identidade no `web/public/manifest.json`.

A promoção não calcula identidade e não aprende hashes esperados do remoto.
O Internet Archive é uma observação do objeto servido, não a autoridade sobre
qual objeto deveria existir.

## Organização dos artefatos

- ZIPs originais da RFB continuam em `raw/`.
- Parquets, `lookups.json` e `lookups/*.parquet` continuam na raiz/subdiretório
  já definido pelo contrato.
- Shards derivados `companies-00.zip` … `companies-99.zip` ficam **na raiz do
  item**, não em `raw/`.

## Hashes

- Parquets e lookups: `size + sha1 + sha256`.
- Shards `companies`: `size + sha1` para identidade de bytes e
  `materialization_id` para identidade semântica.
- SHA-1 é checksum operacional para comparação com o catálogo do IA, não uma
  assinatura de autenticidade.

## Retomada

O descriptor-base e cada receipt são artifacts independentes e duráveis. Uma
falha em upload, consistência eventual, verificação ou promoção não exige
reexecutar transform/pack apenas para redescobrir hashes já produzidos.

Reexecuções de submissão são fail-closed: se o nome remoto já existir, ele só
pode ser reutilizado quando a identidade semântica e `size + sha1` forem
exatamente iguais ao receipt local. Divergência nunca autoriza overwrite.

## Primeiro uso

`2026-06` é o primeiro snapshot que deve usar este fluxo integralmente. O
retrofit de `2026-05` não é requisito para validar a arquitetura nova.
