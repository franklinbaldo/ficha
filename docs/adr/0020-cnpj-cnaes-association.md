# ADR 0020 — `cnpj_cnaes.parquet` — position-aware CNAE association (W11)

**Status:** Accepted
**Data:** 2026-05-15

## Contexto

O plano de performance (§11 / W11) detalha o uso de CNAEs e suas associações inversas. Atualmente, os códigos e descrições dos CNAEs secundários ficam denormalizados no array em `cnpjs.parquet` (`cnae_secundario_codigos`, `cnae_secundario_descricoes`), que são essenciais para visualização direta da lâmina sem realizar joins.

Porém, para responder a perguntas analíticas como "quais os CNPJs do setor de restaurantes?" (busca reversa por CNAE), uma coluna baseada em arrays seria lenta no DuckDB-WASM sem uma estrutura de índice dedicada.

## Decisão

Adicionar o Parquet `cnpj_cnaes.parquet` como um índice inverso para consultas que relacionam um CNAE a um CNPJ, com suporte a posição na ordem de registro.

- **Schema:** `(cnpj, cnpj_base, cnae_codigo, posicao)`.
- **Lógica de Posição:** `posicao = 0` indica que é o CNAE primário. `posicao >= 1` indica que é secundário, na mesma ordem de registro na base da Receita Federal.
- **Sort:** Ordenado por `(cnae_codigo, posicao, cnpj_base)`.
- **Bloom filters:** Nas colunas `cnae_codigo` e `cnpj_base`.
- **Coexistência:** Os arrays denormalizados dentro de `cnpjs.parquet` permanecem.

## Por quê

- **Busca Reversa Rápida:** Ao possuir as colunas de ordenação corretas e bloom filter no código do CNAE, conseguimos resgatar empresas associadas a um CNAE apenas baixando fragmentos pequenos (row groups) que contém o ID desejado.
- **Pruning Inteligente por Posição:** Com a posição atrelada à ordem, as estatísticas de min/max dos row groups descartam automaticamente CNAEs secundários quando aplicamos `WHERE posicao = 0`, evitando precisar de uma coluna booleana explícita para o CNAE primário.
- **Analíticas Complexas:** O formato possibilita fazer análises sobre o registro como "Para CNAEs primários da área médica, qual o principal CNAE secundário?".

## Consequências

- ✅ Eficiência de leitura e range requests na busca reversa para DuckDB-WASM.
- ✅ Possibilita cruzamentos complexos e precisos baseados na ordem de listagem.
- ⚠️ Necessidade de compor o layout do Parquet com arrays de subscritos da base (aumento de lógica de SQL na construção do ETL).
- ⚠️ Geração do novo arquivo adiciona um índice analítico que coexiste com os campos denormalizados do array (`cnae_secundario_codigos`).

## Referências

- PR #36
- Plano de Performance `docs/perf-plan-2026-05.md` (§11 / W11)
