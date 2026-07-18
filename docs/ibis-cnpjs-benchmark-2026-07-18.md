# Benchmark: migração do join `cnpjs` para Ibis (2026-07-18)

**Contexto:** [ADR 0017](adr/0017-ibis-shared-analytical-layer.md) mantém os joins
pesados (`cnpjs`/`raizes`) em SQL bruto e exige que qualquer migração para Ibis
venha com **benchmark de memória** comparando o plano que o Ibis compila contra
o SQL manual — não uma troca cega. Este documento é esse benchmark, para o join
`cnpjs`.

**Harness:** [`etl/scripts/diagnostics/ibis_cnpjs_benchmark.py`](../etl/scripts/diagnostics/ibis_cnpjs_benchmark.py).
Gera dados sintéticos com o shape de produção (empresa/estabelecimento/simples +
lookups), constrói `cnpjs` das **duas formas sobre o mesmo conjunto de colunas**
(join de 3 tabelas grandes + LEFT JOINs de lookup + CASE/TRY_CAST/datas/
`strip_accents`), roda cada uma sob as configs de produção (`threads=1`,
`memory_limit` explícito, temp dir dedicado) e mede tempo de parede + **pico de
spill** (thread sampler no temp dir), depois compara os parquets por checksum
ordenado. As saídas são **bit-a-bit idênticas** em todas as execuções.

## Resultados

Ambiente: DuckDB 1.5.2, Ibis 12.0.0, 16 GB RAM, 4 cores (mesmo perfil do runner
GH Actions). `threads=1` em todos os casos.

| Escala | `memory_limit` | Caminho | Tempo | Pico de spill | Saída |
|---|---|---|---|---|---|
| 6M estab / 5M emp | 3 GB | SQL manual | 23.3s | 0 B | 25.5 MB |
| 6M estab / 5M emp | 3 GB | **Ibis** | 17.9s | 0 B | 25.5 MB |
| 6M estab / 5M emp | 1 GB (força spill) | SQL manual | 73.9s | 436 MB | 129 MB |
| 6M estab / 5M emp | 1 GB (força spill) | **Ibis** | 66.6s | 481 MB | 129 MB |

**Delta (sob pressão, 1 GB):** Ibis ≈ **0.90× do tempo** (mais rápido) e
**1.10× do spill** (~10% mais). Checksum idêntico.

## Leitura

1. **O join `cnpjs` não é patológico em memória sob `threads=1`.** Nem o SQL nem
   o Ibis fazem spill com 3 GB para 6M linhas; só sob um teto artificial de 1 GB
   é que o spill aparece. Isso é consistente com o diagnóstico do perf-plan: o
   OOM histórico de produção (pico de 70 GB) veio de `threads=4` + do
   `LIST(DISTINCT …)` do **raizes**, não do join do `cnpjs` em si — que é
   justamente o que o `chunk-per-ZIP` + `threads=1` já domam.

2. **O plano compilado pelo Ibis fica perto do SQL manual — sem penhasco.** Sob
   pressão, ~10% mais spill e tempo igual ou melhor, com saída idêntica. Não há
   sinal de que o compilador do Ibis gere um plano que estoure o orçamento onde
   o SQL não estoura. Pela evidência, **migrar o `cnpjs` para Ibis é baixo
   risco** — o oposto do medo hipotético que o ADR 0017 registrou.

3. **Ressalvas (por que isto não é um sinal-verde total):**
   - **~1/12 da escala de produção** (6M vs ~71M estab). O número definitivo é
     rodar o harness em CI com `FICHA_BENCH_ESTAB_ROWS=70000000`.
   - **Dados sintéticos comprimem mais** que texto RFB real; spills reais serão
     maiores em ambos os caminhos, mas a *razão* entre eles deve se manter.
   - **`raizes` não foi medido.** É a agregação `LIST(DISTINCT)` que causou o OOM
     real, e seu comportamento de memória sob Ibis é **desconhecido**. Migrar
     `raizes` continua sendo o alvo arriscado e precisa do **seu próprio
     benchmark** antes de qualquer troca.
   - O spill de ~10% é uma amostra única de uma métrica ruidosa (sampler a cada
     50 ms); tratar como indicativo, não como medida de precisão.

## Recomendação

- **`cnpjs`:** migração para Ibis é viável e de baixo risco pela evidência local;
  confirmar com uma execução do harness em CI na escala de produção antes de
  trocar o código de produção.
- **`raizes`:** benchmark próprio feito em
  [`ibis-raizes-benchmark-2026-07-18.md`](ibis-raizes-benchmark-2026-07-18.md) —
  viável desde que a lista-distinta use o pre-dedup de dois passos
  (`.distinct().collect()`), **nunca** o `collect(distinct=True)` idiomático.
- **`socios`:** já migrado para Ibis neste PR (sem histórico de OOM, join simples,
  equivalência bit-a-bit verificada em fixtures).

## Reproduzir

```bash
cd etl
# smoke local (~6M linhas, cabe em 16 GB):
uv run python scripts/diagnostics/ibis_cnpjs_benchmark.py
# forçar spill:
FICHA_BENCH_MEMORY_GB=1 uv run python scripts/diagnostics/ibis_cnpjs_benchmark.py
# escala de produção (CI):
FICHA_BENCH_ESTAB_ROWS=70000000 FICHA_BENCH_MEMORY_GB=9 \
    uv run python scripts/diagnostics/ibis_cnpjs_benchmark.py
```
