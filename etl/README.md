# ficha-etl

Pipeline mensal que transforma os dumps de CNPJ da Receita Federal Brasileira em snapshots Parquet e publica no Internet Archive.

## Estado e caminhos de execução

Há **duas rotas distintas**: o CLI `ficha-etl run` transforma dados RFB em Parquets/lookups e um `companies.zip` **monolítico** (protobuf por raiz), com possibilidade de upload ao Internet Archive. A [esteira mensal GitHub Actions](../.github/workflows/etl-monthly.yml) chama `python -m ficha_etl.monthly_sharded` e distribui a camada atômica em **100 shards** `companies-XX.zip`, com validação e promoção do manifesto em etapas separadas. Não usar evidência de uma rota para afirmar a execução da outra.

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

### `run` é uma operação potencialmente publicadora

**Não execute `ficha-etl run --month YYYY-MM` como smoke test.** `skip_upload=False` é o padrão. Com `IA_ACCESS_KEY` e `IA_SECRET_KEY` disponíveis, o fluxo tenta (1) enviar ZIPs RFB ao IA, (2) transformar em Parquets/lookups, (3) enviar saídas ao IA, (4) gerar e enviar `companies.zip` monolítico e (5) atualizar o `manifest.json` local. Sem as credenciais, recusa o caminho padrão; não passa automaticamente a leitura inocente.

Mesmo com `--skip-upload`, o `run` pode baixar dados completos, consumir CPU/disco, criar Parquets/ZIP local e **modificar o manifesto local**. Não é dry-run barato nem somente leitura. A flag com nome similar na esteira mensal tem semântica própria, sem equivalência automática.

Outros subcomandos: `list-snapshots`, `list-files`, `download`, `transform`, `pack`, `fetch`, `smoke`. Alguns acessam rede, executam trabalho ou escrevem arquivos. Qualquer ETL real, backfill, upload ou promoção depende de autorização operacional específica.

## Contratos com os consumidores

O Parquet **não é o único contrato** entre o ETL e o site público. As três fronteiras devem ser testadas separadamente:

- **Catálogo e proveniência:** [`web/public/manifest.json`](../web/public/manifest.json) declara competências, URLs, tamanhos e hashes dos artefatos. O manifesto é entrada para os leitores, não uma comprovação automática de que cada objeto remoto está acessível ou de que seus bytes foram autenticados. A [#281](https://github.com/franklinbaldo/ficha/issues/281) registra a investigação anterior sobre a completude das dez partes de estabelecimentos; a custódia física dos **37 ZIPs raw** é uma frente distinta, ainda aberta em [#433](https://github.com/franklinbaldo/ficha/issues/433).
- **Ficha atômica:** a esteira `monthly_sharded` exige o conjunto completo de 100 `companies-XX.zip` por competência, com membros Protobuf por raiz. O site SSG em [`site/`](../site/README.md) usa essa camada para **CNPJ numérico completo** e para História em competências compatíveis; ao encontrar um registro, também consulta `lookups.json`. O leitor [`atomic-record.mjs`](../site/static/js/atomic-record.mjs) aceita hoje a ausência de `files.companies.schema_version` no manifesto e usa a hipótese de wire `1.0.0`; isso **não constitui atestado da versão dos bytes publicados**. A correção de proveniência está em [#383](https://github.com/franklinbaldo/ficha/issues/383).
- **Consulta analítica:** os Parquets seguem os contratos de schema em [`web/src/schemas/v1/`](../web/src/schemas/v1/) e a versão no footer `ficha.schema_version`, conforme a [ADR 0003](../docs/adr/0003-schema-versioning.md). No site SSG, essa é a rota **experimental** de busca por nome ou CNPJ alfanumérico via DuckDB-WASM, sujeita a CORS/Range; sua falha não prova ausência cadastral ([#208](https://github.com/franklinbaldo/ficha/issues/208)). Não há fallback automático de História numérica para Parquet.

**Compatibilidade semântica não se deduz apenas do schema:** em [#359](https://github.com/franklinbaldo/ficha/issues/359#issuecomment-7016083338), uma comparação **somente leitura de dois derivados publicados** encontrou, para a mesma raiz `00000006` em `2026-07`, `porte_empresa='01'` no `raizes.parquet` (microempresa segundo o layout RFB) e `porte_empresa=1` no membro Protobuf v1.0.0 (enum `NAO_INFORMADO`). O teste offline integrado pela [#363](https://github.com/franklinbaldo/ficha/pull/363) preserva essa contraprova; os três XFAIL normativos do produtor (`00`, `01`, `02`) **continuam pendentes**. A comparação não autentica os ZIPs originais da RFB nem identifica o empacotador histórico e **não autoriza reinterpretar todo `wire=1` como microempresa**. A UI mantém o porte como **“Não determinado”**; qualquer correção exige leitura versionada e proveniência antes de reclassificar snapshots antigos.

Artifacts temporários do GitHub Actions, inclusive nos diretórios ocultos `.cache/` e `.publication/`, **não são custódia dos ZIPs no Internet Archive**. A [#420](https://github.com/franklinbaldo/ficha/pull/420), já integrada, tornou explícito o upload de artifacts ocultos, mas o CI comum não executou a transferência real do mensal. A [#427](https://github.com/franklinbaldo/ficha/pull/427), também integrada, agora separa os artifacts por `github.run_attempt` e recusa tentativas posteriores à primeira até que exista recuperação comprovada; **não implementa recovery**. Essas guardas não autenticam os bytes remotos nem demonstram readback do Internet Archive. Para os **37 ZIPs raw** de julho/2026, **7.643.363.104 bytes (~7,12 GiB)** são tamanhos **declarados no catálogo**, não medidos no destino. A [#433](https://github.com/franklinbaldo/ficha/issues/433) exige preflight físico global, fixação dos bytes entre retries, `UNKNOWN` durável e readback com digest antes de atestar custódia; nada disso está demonstrado pelas guardas citadas.

Nenhuma consulta, leitura do manifesto ou teste documental daqui autoriza executar ETL produtivo, download massivo RFB, upload, promoção de manifesto ou alterar o cron.
