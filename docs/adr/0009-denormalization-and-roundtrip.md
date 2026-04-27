# ADR 0009 — Denormalização e roundtrip-equivalence como gate

**Status:** Accepted
**Data:** 2026-04-27

## Contexto

O dump da RFB tem layout normalizado (Empresas, Estabelecimentos, Simples, Cnaes, Municípios, etc. em arquivos separados) por razões históricas e de tamanho. O FICHA não precisa preservar essa estrutura — só precisa **retornar o mesmo conteúdo**.

## Decisão

Reestruturar livremente para os padrões de query do FICHA, com **roundtrip-equivalence** como gate de qualidade no ETL.

### Liberdades exercidas

1. **Denormalização inline** — `cnpjs.parquet` carrega Empresa + Estabelecimento + Simples + descrições resolvidas (CNAE, Município, Natureza, etc.) na mesma linha. ~5% mais espaço, zero joins no client.
2. **Agregados materializados** — `raizes.parquet` carrega `qtd_estabelecimentos`, `ufs_atuacao` (array), `cnaes_secundarios_distintos`, etc.
3. **Reorganização** — `socios.parquet` mistura sócios PF e PJ com flag `tipo`. RFB separa por bloco; FICHA junta porque a query do usuário não distingue.
4. **Drop de redundâncias** — campos que são derivações puras de outros (ex.: `cnpj` completo = `cnpj_base + cnpj_ordem + cnpj_dv`) podem ser pre-calculados.
5. **Adição de campos derivados** — `razao_social_normalizada` (uppercase, sem acento) pra busca; `idade_anos` (computed em runtime ou no ETL).

### Roundtrip-equivalence — gate de publicação

No `etl/src/ficha_etl/transform.py`, antes de fazer upload:

```python
def assert_roundtrip(parquet_dir: Path, raw_rfb_dir: Path, sample_size: int = 1000) -> None:
    """
    Sortea N CNPJs do dump original e verifica que cada campo retornável
    pelo Ficha bate com a extração crua do RFB. Falha o ETL se divergir.
    """
    sample = sample_cnpjs_from_rfb(raw_rfb_dir, n=sample_size)
    for cnpj in sample:
        ficha_data = query_ficha_parquets(parquet_dir, cnpj)
        rfb_data = extract_from_raw(raw_rfb_dir, cnpj)
        assert_equivalent(ficha_data, rfb_data, ignore=["computed_fields"])
```

Isso garante que mesmo com toda a reestruturação, **o usuário recebe os mesmos dados** que receberia consumindo o dump cru.

### Campos computados (excluídos do roundtrip)

Estes existem no Ficha mas **não** no dump original — claramente derivados:

- `razao_social_normalizada` — derivado de `razao_social`
- `idade_anos` — derivado de `data_inicio_atividade`
- `qtd_estabelecimentos` (em `raizes.parquet`) — derivado por agregação
- Descrições inline (cnae_descricao, municipio_nome, etc.) — derivadas por lookup

## SemVer do schema

- Adicionar campo computado novo: **patch** (1.0.0 → 1.0.1)
- Adicionar campo do RFB que não estava antes: **minor** (1.0.0 → 1.1.0)
- Remover campo do RFB que existia: **major** (quebra roundtrip → 1.x.x → 2.0.0)
- Renomear campo: **major**
- Mudar tipo de campo: **major**

Schemas Zod versionados em `web/src/schemas/vN/` continuam imutáveis após publicados (ver [ADR 0003](0003-schema-versioning.md)).

## Consequências

- ✅ Frontend simples — zero joins no client.
- ✅ Liberdade total pra otimizar layout pra performance.
- ✅ Confiança operacional — roundtrip falha CI antes de publicar lixo.
- ⚠️ Custo de implementar `assert_roundtrip` no ETL (~50 linhas Python).
- ⚠️ Sample de 1000 CNPJs roda em segundos; sample maior pra garantia mais alta é trivial.

## Alternativas

- **Normalização fiel ao RFB** — força joins no client, deteriora UX.
- **Validação só por contagem/sums** — não detecta corrupção semântica em campos individuais.
