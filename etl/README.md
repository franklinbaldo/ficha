# ficha-etl

Pipeline mensal que transforma os dumps de CNPJ da Receita Federal Brasileira em snapshots Parquet e publica no Internet Archive.

## Status

Pipeline implementado e testado em fixtures: produz 7 parquets analíticos
(`cnpjs`, `raizes`, `socios`, `enderecos`, `pessoas`, `cnpj_cnaes`,
`cnpj_contatos`), `lookups.json` + 6 lookup-parquets e o `companies.zip`
(camada atômica, protobuf por raiz). A execução automática mensal
(`etl-monthly.yml`) ainda não completou um ciclo de ponta a ponta em
produção — ver `docs/vision-blockers-2026-07.md`.

## Setup local

```bash
cd etl
uv venv
uv pip install -e ".[dev]"
```

## Uso

```bash
ficha-etl run --month 2026-01
```

Etapas:
1. `download` — baixa o dump da RFB (via mirror IA quando já espelhado)
2. `transform` — converte CSV → parquets com schema versionado
3. `upload` — publica no Internet Archive como item `ficha-YYYY-MM`
4. `pack` — gera e publica o `companies.zip` (fichas atômicas em protobuf)
5. `manifest` — atualiza `web/public/manifest.json` com o novo snapshot

Outros subcomandos: `list-snapshots`, `list-files`, `download`, `transform`,
`pack`, `fetch`, `smoke` (ver `ficha-etl --help`).

## Contrato com o frontend

Único acoplamento: o Parquet gerado precisa bater com o schema Zod em `web/src/schemas/v{N}/` e a versão precisa estar declarada no footer do Parquet (`ficha.schema_version`). Veja `docs/adr/0003-schema-versioning.md`.
