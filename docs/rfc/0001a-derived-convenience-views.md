# RFC 0001A — Views derivadas de conveniência sobre snapshots publicados

**Status:** Accepted amendment  
**Data:** 2026-08-13  
**Emenda:** RFC 0001

## Decisão

O FICHA não terá uma segunda camada de dados denominada **canônica**.

A RFC 0001 originalmente propôs materializar uma família adicional de Parquets internos por entidade (`empresa`, `estabelecimento`, `simples`, `socio` e lookups) e fazer os produtos dependerem dela. Essa obrigação fica substituída por uma regra mais simples:

> os Parquets publicados no snapshot são o substrato físico; interpretações reutilizáveis são views derivadas/de conveniência, preferencialmente DuckDB SQL; materialização é opcional e ocorre apenas quando um consumidor ou uma medição justificar.

Uma view não é uma fonte de verdade superior e não deve receber nomes que impliquem autoridade global.

```text
snapshot publicado
  ├── cnpjs.parquet
  ├── raizes.parquet
  ├── socios.parquet
  └── demais Parquets
          │
          ├── view DuckDB → consulta
          ├── view DuckDB → análise
          └── view DuckDB → COPY TO parquet (se necessário)
```

## Autoridade

1. Os ZIPs preservados da Receita são a fonte bruta histórica.
2. O `manifest.json` e os artefatos nele declarados formam o contrato público de cada snapshot do FICHA.
3. Views derivadas são interpretações úteis sobre esse contrato, específicas de uma tarefa ou domínio.

## Materialização

`COPY (...) TO 'x.parquet' (FORMAT PARQUET)` é uma escolha de execução, cache, distribuição ou interoperabilidade. Materializar uma view não a torna mais correta nem mais canônica.

## Contrato de uma view reutilizável

Uma view que saia de `experiments/` deve declarar: versão, inputs, colunas/tipos, propósito, invariantes, semântica de NULL/normalizações e limitações. Deve rodar sobre snapshot real e demonstrar utilidade concreta.

## Efeito sobre os experimentos anteriores

Os trabalhos de `estabelecimento`, `empresa`, `simples` e `socio` continuam úteis como evidência de schema, casts, chaves, cardinalidade e qualidade. Eles não obrigam a publicação ou retenção de `canonical/*.parquet`.

Em particular, a investigação de `socio` pode alimentar views e testes de invariantes sem precisar resultar numa tabela “socio canônica”.

## Próximo experimento

Criar duas ou três views DuckDB sobre os Parquets já publicados de `2026-05`, medir schema, EXPLAIN, cardinalidade e tempo, e materializar uma delas apenas para comparar custo/tamanho. Repetir sobre `2026-04` quando compatível.

## Consequência para a RFC 0001

Ficam supersedidas as partes da RFC 0001 que exigem:

- uma árvore obrigatória `canonical/{tabela}/part-*.parquet`;
- “Parquet canônico” como fronteira obrigatória de checkpoint;
- que todos os produtos consumam apenas esses Parquets;
- uma Fase 6 cuja condição seja virar todo o pipeline para essa camada.

Permanecem válidos os princípios independentes dessa materialização: preservação de raw, tipagem explícita, qualidade de dados, determinismo, observabilidade, medição sob computação restrita e experimentação antes de mudar produção.
