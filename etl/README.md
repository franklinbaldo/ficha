# ficha-etl

Pipeline mensal que transforma os dumps de CNPJ da Receita Federal Brasileira em snapshots Parquet e publica no Internet Archive.

## Status

Esqueleto inicial — implementação ainda não começou. Veja `experiments/` na raiz do repo para PoCs anteriores que servirão de base.

## Setup local

```bash
cd etl
uv venv
uv pip install -e ".[dev]"
```

## Uso (planejado)

```bash
ficha-etl run --month 2026-01
```

Etapas:
1. `download` — baixa o dump mais recente da RFB
2. `transform` — converte CSV → Parquet único com schema versionado
3. `upload` — publica no Internet Archive como item `ficha-YYYY-MM`
4. `manifest` — atualiza `web/public/manifest.json` com o novo snapshot

## Contrato com o frontend

Único acoplamento: o Parquet gerado precisa bater com o schema Zod em `web/src/schemas/v{N}/` e a versão precisa estar declarada no footer do Parquet (`ficha.schema_version`). Veja `docs/adr/0003-schema-versioning.md`.
