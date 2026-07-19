# Benchmark: fundir (não materializar) `_raizes_counts`/`_raizes_empresa`/`_raizes_matriz`? (2026-07-18)

**Contexto:** follow-up ao [benchmark do agregado `raizes`](ibis-raizes-benchmark-2026-07-18.md)
e à migração para Ibis de `write_raizes_parquet_from_cnpjs` (que preservou as
**seis** fronteiras de materialização do SQL original, na mesma ordem, pelo
princípio "a forma exata de execução segura o OOM histórico").

Uma auditoria dessa escolha (2026-07-18) separou as seis em dois grupos:

- **Quatro com mecanismo de OOM documentado** (`_raizes_ufs`/`_raizes_ufs_agg`/
  `_raizes_cnaes`/`_raizes_cnaes_agg`) — o pre-dedup de dois passos coberto pelo
  benchmark irmão. Materializar aqui é necessário, não uma escolha.
- **Três sem histórico de OOM documentado** (`_raizes_counts`: `COUNT`/`COUNT
  FILTER`; `_raizes_empresa`: `.arbitrary()` por grupo sobre 7 colunas;
  `_raizes_matriz`: filtro + janela `ROW_NUMBER`) — marcadas "vale testar
  fundido, NÃO 'seguro para adiar' sem evidência", porque o OOM real de
  produção (PR #24, run 25522678418) veio de **pipelinar múltiplos operadores
  pesados juntos**, não de nenhum desses três isoladamente.

Este benchmark testa essa hipótese diretamente: constrói a mesma saída de duas
formas —

- `eager` — materializa `_raizes_counts`/`_raizes_empresa`/`_raizes_matriz`
  como TEMP TABLEs próprias antes do join final (código atual do PR #59)
- `fused` — compila counts/empresa/matriz como sub-expressões Ibis inline,
  unidas na MESMA query final (ufs_agg/cnaes_agg continuam eager nos dois
  casos — essa fronteira está fora do escopo aqui)

**Harness:** [`etl/scripts/diagnostics/ibis_raizes_fusion_benchmark.py`](../etl/scripts/diagnostics/ibis_raizes_fusion_benchmark.py).
Verificação de equivalência em duas camadas: um checksum sobre **todas** as
colunas de saída (não um subconjunto), mais um diff simétrico linha-a-linha
(`EXCEPT` nos dois sentidos) — a checagem mais forte disponível, calculada no
mesmo processo/paths do benchmark (evita qualquer ambiguidade de path entre
shells).

## Resultados

DuckDB 1.5.2, Ibis 12.0.0, `threads=1`, `preserve_insertion_order=false`.

| Escala | `memory_limit` | `eager` | `fused` | Equivalência |
|---|---|---|---|---|
| 200K grupos | 2 GB | ok, 2.4s, spill=0 B | ok, 1.0s, spill=0 B | idêntico (0 diffs linha-a-linha) |
| 3M grupos | 1 GB | ok, 28.1s, spill=472.2 MB | ok, 34.7s, spill=326.7 MB (**0.69×**) | idêntico (0 diffs linha-a-linha) |
| 20M grupos | 2 GB | **OOM**, 32.4s, spill=2.5 GB | **OOM**, 120.1s, spill=6.6 GB (**2.64×**) | — (ambos falharam) |

### O achado central: o comportamento **não é monotônico**

A 3M grupos, `fused` teve *menos* spill que `eager` (0.69×) e uma saída
idêntica — nessa escala, fundir pareceria uma vitória. Mas a 20M grupos, sob o
mesmo teto de memória apertado, a relação **se inverte**: `fused` gasta 2.64×
mais spill que `eager` e demora 3.7× mais para finalmente estourar (120s vs
32s) antes de falhar do mesmo jeito. Isso é exatamente o mecanismo que a
auditoria alertou: o risco de fundir não aparece isoladamente em nenhum dos
três agregados — aparece quando o plano final tenta pipelinar os três
**junto com** o join contra ufs_agg/cnaes_agg sob pressão de memória, e essa
pressão só fica severa perto da escala de produção.

Ou seja: **não existe um único número que decida essa questão** — o mesmo
código pode parecer uma melhoria a 3M e ser claramente pior a 20M. Produção
roda a ~67M grupos, além do ponto onde `eager` já vence claramente aqui.

## Recomendação

- **Manter `eager`** (código atual do PR #59) para `_raizes_counts`,
  `_raizes_empresa`, `_raizes_matriz`. Não há evidência de que fundir seja
  seguro na escala de produção — pelo contrário, a evidência a 20M já aponta
  na direção oposta.
- **Não tratar o resultado a 3M como sinal de segurança.** É dependente de
  escala, exatamente como o `ibis_idiomatic` 0-spill do benchmark irmão foi
  dependente do plano — um número bom numa escala menor não garante nada na
  escala de produção.
- PR #59 e PR #61 (fix do OOM em `write_cnpjs_parquet_chunked`) seguem sem
  necessidade de mudança adicional por este achado.

## Reproduzir

```bash
cd etl
uv run python scripts/diagnostics/ibis_raizes_fusion_benchmark.py                                    # smoke local
FICHA_BENCH_EMPRESA_ROWS=3000000 FICHA_BENCH_DUP=1 FICHA_BENCH_MEMORY_GB=1 \                          # fused parece melhor aqui — não confiar
    uv run python scripts/diagnostics/ibis_raizes_fusion_benchmark.py
FICHA_BENCH_EMPRESA_ROWS=20000000 FICHA_BENCH_DUP=1 FICHA_BENCH_MEMORY_GB=2 \                         # inversão: fused nitidamente pior
    uv run python scripts/diagnostics/ibis_raizes_fusion_benchmark.py
```
