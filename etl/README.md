# ficha-etl

Pipeline mensal projetado para transformar os dumps de CNPJ da Receita Federal Brasileira em snapshots Parquet e publicá-los no Internet Archive. **A publicação remota permanece em HOLD #433**, sem prova independente de custódia S/R/F.

## Estado e caminhos de execução

Há **duas rotas distintas**: o CLI `ficha-etl run` transforma dados RFB em Parquets/lookups e um `companies.zip` **monolítico** (protobuf por raiz), mas sua publicação legada está recusada por guarda fail-closed. A [esteira mensal GitHub Actions](../.github/workflows/etl-monthly.yml) chama `python -m ficha_etl.monthly_sharded` e prevê camada atômica de **100 shards** `companies-XX.zip`, com validação e promoção do manifesto em etapas separadas; essas etapas **não estão liberadas para publicação** sem custódia S/R/F independente. Não usar evidência de uma rota para afirmar a execução da outra.

O [manifesto versionado](../web/public/manifest.json) declara snapshots e URLs. No corte `main@98329ff`, em **16/11/2026**, `current=2026-07`. Isso **não comprova** disponibilidade HTTP live, integridade dos arquivos, funcionalidade da interface ou frescor da RFB: cada propriedade exige teste próprio. O [diagnóstico de julho](../docs/vision-blockers-2026-07.md) e a [auditoria 18/07](../docs/audit-2026-07-18.md) são **históricos**, não atestados atuais. Coordenação operacional na [#247](https://github.com/franklinbaldo/ficha/issues/247).

## Setup local

```bash
cd etl
uv venv
uv pip install -e ".[dev]"
```

## Inspeção dos comandos (sem executar o pipeline)

```bash
# Ajuda local via ambiente .venv criado acima; sem upload/ETL
uv run --no-sync ficha-etl --help
uv run --no-sync ficha-etl run --help
```

### `run` e `pack`: publicação legada em HOLD #433

**Não execute `ficha-etl run --month YYYY-MM` como smoke test.** `skip_upload=False` continua sendo o padrão da interface, porém desde a [PR #456](https://github.com/franklinbaldo/ficha/pull/456) `_cmd_run` e `_cmd_pack` recusam esse caminho antes de iniciar trabalho, consultar credenciais ou fazer PUT. A recusa é **independente da presença de credenciais**: `IA_ACCESS_KEY` e `IA_SECRET_KEY` não a removem. `--skip-upload` preserva apenas o processamento local, sem autorizar publicação e sem atestar custódia remota.

Mesmo com `--skip-upload`, o `run` pode baixar dados completos, consumir CPU/disco, criar Parquets/ZIP local e **modificar o manifesto local**; `pack --skip-upload` também pode executar processamento volumoso. Não é dry-run barato nem somente leitura. A flag com nome similar na esteira mensal tem semântica própria, sem equivalência automática. O HOLD da [#433](https://github.com/franklinbaldo/ficha/issues/433) continua mesmo após a integração de #456.

Outros subcomandos: `list-snapshots`, `list-files`, `download`, `transform`, `pack`, `fetch`, `smoke`. Alguns acessam rede, executam trabalho ou escrevem arquivos. Qualquer ETL real, backfill, upload ou promoção depende de autorização operacional específica.

## Contratos com os consumidores

O Parquet **não é o único contrato** entre o ETL e o site público. As três fronteiras devem ser testadas separadamente:

- **Catálogo e proveniência:** [`web/public/manifest.json`](../web/public/manifest.json) declara competências, URLs, tamanhos e hashes dos artefatos. O manifesto é entrada para os leitores, não uma comprovação automática de que cada objeto remoto está acessível ou de que seus bytes foram autenticados. A [#281](https://github.com/franklinbaldo/ficha/issues/281) registra a investigação anterior sobre a completude das dez partes de estabelecimentos; a custódia física dos **37 ZIPs raw** é uma frente distinta, ainda aberta em [#433](https://github.com/franklinbaldo/ficha/issues/433).
- **Ficha atômica:** a esteira `monthly_sharded` exige o conjunto completo de 100 `companies-XX.zip` por competência, com membros Protobuf por raiz. O site SSG em [`site/`](../site/README.md) usa essa camada para **CNPJ numérico completo** e para História em competências compatíveis; ao encontrar um registro, também consulta `lookups.json`. O leitor [`atomic-record.mjs`](../site/static/js/atomic-record.mjs) aceita hoje a ausência de `files.companies.schema_version` no manifesto e usa a hipótese de wire `1.0.0`; isso **não constitui atestado da versão dos bytes publicados**. A correção de proveniência está em [#383](https://github.com/franklinbaldo/ficha/issues/383).
- **Consulta analítica:** os Parquets seguem os contratos de schema em [`web/src/schemas/v1/`](../web/src/schemas/v1/) e a versão no footer `ficha.schema_version`, conforme a [ADR 0003](../docs/adr/0003-schema-versioning.md). No site SSG, essa é a rota **experimental** de busca por nome ou CNPJ alfanumérico via DuckDB-WASM, sujeita a CORS/Range; sua falha não prova ausência cadastral ([#208](https://github.com/franklinbaldo/ficha/issues/208)). Não há fallback automático de História numérica para Parquet.

**Compatibilidade semântica não se deduz apenas do schema:** em [#359](https://github.com/franklinbaldo/ficha/issues/359#issuecomment-7016083338), uma comparação **somente leitura de dois derivados publicados** encontrou, para a mesma raiz `00000006` em `2026-07`, `porte_empresa='01'` no `raizes.parquet` (microempresa segundo o layout RFB) e `porte_empresa=1` no membro Protobuf v1.0.0 (enum `NAO_INFORMADO`). O teste offline integrado pela [#363](https://github.com/franklinbaldo/ficha/pull/363) preserva essa contraprova; os três XFAIL normativos do produtor (`00`, `01`, `02`) **continuam pendentes**. A comparação não autentica os ZIPs originais da RFB nem identifica o empacotador histórico e **não autoriza reinterpretar todo `wire=1` como microempresa**. A UI mantém o porte como **“Não determinado”**; qualquer correção exige leitura versionada e proveniência antes de reclassificar snapshots antigos.

Artifacts temporários do GitHub Actions, inclusive nos diretórios ocultos `.cache/` e `.publication/`, **não são custódia dos ZIPs no Internet Archive**. A [#420](https://github.com/franklinbaldo/ficha/pull/420), já integrada, tornou explícito o upload de artifacts ocultos, mas o CI comum não executou a transferência real do mensal. A [#427](https://github.com/franklinbaldo/ficha/pull/427), também integrada, agora separa os artifacts por `github.run_attempt` e recusa tentativas posteriores à primeira até que exista recuperação comprovada; **não implementa recovery**. Essas guardas não autenticam os bytes remotos nem demonstram readback do Internet Archive. Para os **37 ZIPs raw** de julho/2026, **7.643.363.104 bytes (~7,12 GiB)** são tamanhos **declarados no catálogo**, não medidos no destino. A [#433](https://github.com/franklinbaldo/ficha/issues/433) exige preflight físico global, fixação dos bytes entre retries, `UNKNOWN` durável e readback com digest antes de atestar custódia; nada disso está demonstrado pelas guardas citadas.

A [#458](https://github.com/franklinbaldo/ficha/pull/458), integrada em 22/11/2026, mantém o **mesmo descritor aberto** entre a conferência SHA-256 e o PUT síncrono dos ZIPs raw. Ela evita reabrir um pathname que poderia apontar para outro inode depois da verificação, mas **não sela o conteúdo do inode**. A [contraprova offline HTTPX/h11 da Tartaruga](https://github.com/franklinbaldo/ficha/issues/433#issuecomment-7075442238) demonstrou alteração A→B por escritor pré-aberto no **mesmo inode e com o mesmo tamanho**, recebimento de B e resposta HTTP **201**; o teste foi feito em loopback sintético, **não** no Internet Archive. `Content-Length` e 2xx não constituem digest independente nem confirmação de versão remota. Uma exceção local após iniciar o stream tampouco prova que **zero bytes** foram transmitidos. Ainda faltam spool fisicamente estável, journal `UNKNOWN` durável, readback de SHA-256 pinado e fence de publicação; a [#433](https://github.com/franklinbaldo/ficha/issues/433) permanece sob HOLD.

A [#456](https://github.com/franklinbaldo/ficha/pull/456) introduziu recusas fail-closed nos entrypoints legados de publicação e nos gates mensais de saída/finalização, **não** um verificador remoto de custódia. Não constitui prova de bytes remotos nem garantia para outros scripts ou chamadas Python internas. Nenhuma consulta, leitura do manifesto ou teste documental daqui autoriza executar ETL produtivo, download massivo RFB, upload, promoção de manifesto ou alterar o cron.
