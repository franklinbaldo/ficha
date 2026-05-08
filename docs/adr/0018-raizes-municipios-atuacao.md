# ADR 0018 — `raizes.parquet` v2: trocar `ufs_atuacao` por `municipios_atuacao`

**Status:** Proposed
**Data:** 2026-05-07

## Contexto

`raizes.parquet` v1 (ADR 0008, schema `web/src/schemas/v1/raiz.ts`) traz
para cada raiz (cnpj_basico) um array `ufs_atuacao` com as 1-27 UFs onde
a empresa tem estabelecimentos. A intenção era dar um sinal geográfico
de "onde a empresa atua".

Na prática, o sinal é muito fraco:

- Brasil tem 27 UFs. Empresas de porte nacional atendem várias delas;
  pra raizes grandes a lista vira `["AC","AL","AM","AP","BA",...]` com
  10-20+ entradas, o que diz pouco além de "empresa grande".
- Pra raizes pequenas a UF coincide com a UF da matriz, então o array é
  redundante com o campo `uf_matriz` que já existe na mesma linha.
- Casos interessantes ("empresa do Sul que tem filial em Manaus")
  ficam invisíveis dentro do array.

O sinal forte de footprint geográfico está no **município**:

- Brasil tem ~5570 municípios — muito mais granular.
- Empresas tipicamente atuam em poucos municípios (mediana esperada
  bem baixa), então o array é compacto e informativo por linha.
- Raizes com presença em 50+ municípios são genuinamente especiais e
  detectar isso vira uma query útil ("empresas com pegada nacional
  multimunicipal").

Este ADR propõe schema v2 do `raizes.parquet` substituindo o array
de UFs por array de municípios.

## Decisão

`raizes.parquet` v2 troca `ufs_atuacao` (`array<string>` de UFs) por:

| Campo                     | Tipo            | Descrição                                                                 |
|---------------------------|-----------------|---------------------------------------------------------------------------|
| `municipios_atuacao`      | `array<string>` | códigos IBGE distintos onde a raiz tem estabelecimento                    |
| `municipios_atuacao_count`| `integer`       | `len(municipios_atuacao)` denormalizado pra ordenação/filtro sem expandir |

`uf_matriz` (escalar, da matriz da raiz) continua existindo. A
informação "em quantas UFs a raiz atua" pode ser recomputada no
cliente derivando do `municipios_atuacao` cruzado com `lookups.json`
(que já mapeia município → UF) — não vale a pena materializar como
coluna v2.

### Versionamento

Per ADR 0003 (versionamento de schema em três camadas):

- `web/src/schemas/v2/raiz.ts` ← novo
- `web/src/schemas/v2/index.ts` ← reexports
- `web/src/schemas/v1/raiz.ts` mantido, marcado deprecated mas válido
  pra leitura de snapshots antigos
- `manifest.json` ganha por-snapshot `schema_version`: snapshots de
  fevereiro/2026 pra trás permanecem `1.0.0`, novos snapshots saem
  `2.0.0`. Frontend lê o version e seleciona schema dinamicamente.
- Footer do parquet (`ficha.schema_version`) bumpa pra `2.0.0`.

### Implementação no ETL

Em `etl/src/ficha_etl/transform.py:write_raizes_parquet`:

```sql
-- v1
LIST(DISTINCT est.uf) AS ufs_atuacao,
LIST(DISTINCT est.cnae_fiscal_principal) AS cnaes_principais_distintos
```

vira:

```sql
-- v2
LIST(DISTINCT est.municipio) AS municipios_atuacao,
COUNT(DISTINCT est.municipio)::INTEGER AS municipios_atuacao_count,
LIST(DISTINCT est.cnae_fiscal_principal) AS cnaes_principais_distintos
```

Custo de memória do `LIST(DISTINCT est.municipio)`: 5570 valores
distintos *no universo*; por raiz, na grande maioria dos casos, <10.
Mesmo perfil que UF (na prática até menor por raiz típica) — não
agrava o OOM que perseguimos em PR #24.

## Consequências

- ✅ **Sinal geográfico real**. Footprint multimunicipal vira queryable.
- ✅ **Cardinalidade trivial via `municipios_atuacao_count`**. Top-N de
  raizes por presença sem expandir o array.
- ✅ **`uf_matriz` ainda dá a UF principal sem precisar do array**.
  Para "empresas de SP", o filtro continua escalar.
- ✅ **Sem regressão de memória/disco**: cardinalidade por raiz é
  compatível com a do array de UFs na prática (~poucas entradas).
- ⚠️ **Schema break.** Consumers de v1 precisam migrar pra v2 ou ler
  manifest version e ramificar. ADR 0003 já prevê esse fluxo.
- ⚠️ **Backfill.** Snapshots v1 já publicados ficam v1; só novos saem
  v2. Coexistência permanente possível (consumidores escolhem por
  manifest version), mas backfill com schema novo é uma opção
  separada (custo: re-processar 35 meses).
- ❌ **`ufs_atuacao` sai.** Quem usa esse campo precisa migrar pro
  novo. Pode-se manter computando dele a partir de `municipios_atuacao`
  + `lookups.json` no cliente (1 linha de TS).

## Alternativas consideradas e rejeitadas

- **Aditivo: manter `ufs_atuacao` e adicionar `municipios_atuacao`.**
  Sem schema break. Mas perpetua o campo de baixo sinal e dobra o
  custo de armazenamento/query do parquet. ADR 0003 está aí
  exatamente pra fazer breaks limpos quando o ganho compensa.
- **Manter UF, adicionar só municípios principais (top-3 por
  estabelecimento count).** Reduz array pra 3 entradas, mas perde
  raizes com pegada distribuída (que é justamente o sinal interessante).
- **Não fazer nada (v1 forever).** Deixa um campo de baixo sinal
  publicado indefinidamente. Custo de migração só cresce.

## Próximos passos

1. Mergir bootstrap PR #24 (snapshot v1 publicado).
2. PR `feat(schema-v2): municipios_atuacao em raizes.parquet`. Inclui:
   - `web/src/schemas/v2/raiz.ts` + index.
   - `transform.py` ajustado.
   - `manifest.json` schema bumpa pra suportar `schema_version` per-snapshot.
   - Frontend lê manifest e seleciona schema; mostra `municipios_atuacao`
     na lâmina digital quando v2.
   - ADR 0018 promovido pra `Accepted`.
3. Operar em coexistência: snapshot 2026-04 fica v1 (publicado pelo
   bootstrap), 2026-05+ saem v2.
4. (Opcional) Decidir se o backfill (ADR 0016) republica os 35
   históricos em v2 ou mantém em v1. Provavelmente v1 — custo do
   reprocessamento não compensa pra meses arquivais.
