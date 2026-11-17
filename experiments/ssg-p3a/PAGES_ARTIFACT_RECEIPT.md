# P3-A: recibo de artefato Astro anterior (17/11/2026)

**Origem (somente leitura):** [Deploy site #44013291634](https://github.com/franklinbaldo/ficha/actions/runs/44013291634), commit de origem [c3e4a3acf8c8ab18a6a39bd42acee25f30c87917](https://github.com/franklinbaldo/ficha/commit/c3e4a3acf8c8ab18a6a39bd42acee25f30c87917), artefato **github-pages #14297876577** criado em 17/11 às 03:53 UTC.

Jobs **build** e **deploy** concluíram com sucesso; o workflow terminou **failure** por falha posterior em **verify**: Playwright 1.54.2 não instalou Chromium no runner Ubuntu 26.04. Isto não comprova o conteúdo que está **atualmente** servido ao público nem paridade de interfaces. ZIP expira em **18/11/2026 às 03:53 UTC**; digest não substitui os bytes.

## Recibos SHA-256 recomputados localmente do artefato original

| Objeto | SHA-256 |
| --- | --- |
| ZIP original | 36a4746612a63d41946375474e5b452cd546a7c2eabd4985e510171864e1d4ee |
| artifact.tar interno | 5a189d4023975ed13a4b64a0685b3d2d535f5a6f9e1ecb9e8f68ef3b0f097283 |
| Árvore inventory() para rollback | 32f5b138ea5b6659fb4f3578d3295100ca621e89a77f8978d8f0bd152c06fa0d |
| manifest.json RFB anterior | 8dcf725ca434a8fde3d4d9403cc8636e6feca1ea6ef53215f3e6130322d494d3 |
| index.html Astro anterior | 1f4c4094bba53995a0e609c2cee84e434dfca9daccc20ace3dc1a8c275e474cf |

14 arquivos, **471.641 bytes**, manifesto current=2026-07, quatro snapshots.

A árvore é SHA-256 de JSON.stringify([ [relative_path, SHA256(bytes)], ... ]),
ordenado por caminho, compatível com scripts/rollback-drill.mjs.
Os hashes foram calculados sobre o ZIP baixado do GitHub; **não são
atestação externa independente da versão pública atual do Pages**.

## Reprodução somente offline

Obtenha o ZIP do artefato autorizado, sem versioná-lo no repositório.
No diretório experiments/ssg-p3a:

    python3 scripts/import-pages-artifact.py /tmp/github-pages.zip /tmp/ficha-astro-previous \
      --expected-zip-sha256 36a4746612a63d41946375474e5b452cd546a7c2eabd4985e510171864e1d4ee \
      --expected-tree-sha256 32f5b138ea5b6659fb4f3578d3295100ca621e89a77f8978d8f0bd152c06fa0d

Depois de construir e verificar o SSG local (sem deploy):

    FICHA_PREVIOUS_TREE_SHA256=32f5b138ea5b6659fb4f3578d3295100ca621e89a77f8978d8f0bd152c06fa0d \
      node scripts/rollback-drill.mjs dist/ficha /tmp/ficha-astro-previous

O importador exige ZIP com somente artifact.tar, rejeita caminhos inseguros,
links, duplicatas, manifesto sem contrato RFB, hashes divergentes e
sobrescrita de destino. Limite total de 32 MiB, sem extractall.
O CI desta PR testa apenas fixtures sintéticas, sem download externo.

**Status:** importação offline do Astro original e digests ZIP/árvore/manifesto:
**PASS local**. Ensaio completo de rollback usando os dois artefatos reais:
**NOT_RUN** neste recibo. Rollback real do GitHub Pages, URL pública/cache,
paridade DuckDB/Protobuf, leitor de tela e decisão de migração:
**NOT_RUN / HOLD**.
