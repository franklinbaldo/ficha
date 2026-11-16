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
# Ajuda local; não processa snapshots nem faz uploads
ficha-etl --help
ficha-etl run --help
```

### `run` é uma operação potencialmente publicadora

**Não execute `ficha-etl run --month YYYY-MM` como smoke test.** `skip_upload=False` é o padrão. Com `IA_ACCESS_KEY` e `IA_SECRET_KEY` disponíveis, o fluxo tenta (1) enviar ZIPs RFB ao IA, (2) transformar em Parquets/lookups, (3) enviar saídas ao IA, (4) gerar e enviar `companies.zip` monolítico e (5) atualizar o `manifest.json` local. Sem as credenciais, recusa o caminho padrão; não passa automaticamente a leitura inocente.

Mesmo com `--skip-upload`, o `run` pode baixar dados completos, consumir CPU/disco, criar Parquets/ZIP local e **modificar o manifesto local**. Não é dry-run barato nem somente leitura. A flag com nome similar na esteira mensal tem semântica própria, sem equivalência automática.

Outros subcomandos: `list-snapshots`, `list-files`, `download`, `transform`, `pack`, `fetch`, `smoke`. Alguns acessam rede, executam trabalho ou escrevem arquivos. Qualquer ETL real, backfill, upload ou promoção depende de autorização operacional específica.

## Contrato com o frontend

Único acoplamento: o Parquet gerado precisa bater com o schema Zod em `web/src/schemas/v{N}/` e a versão precisa estar declarada no footer do Parquet (`ficha.schema_version`). Veja `docs/adr/0003-schema-versioning.md`.
