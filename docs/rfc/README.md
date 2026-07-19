# Requests for Comments

RFCs são propostas arquiteturais ainda abertas a discussão, experimentação e
revisão. Servem para mudanças grandes demais para caber num ADR curto ou que
ainda dependem de benchmark antes de virar decisão.

## Relação com ADRs

- **RFC**: explora o problema, princípios, arquitetura-alvo, alternativas,
  métricas e plano incremental. Pode mudar durante a revisão.
- **ADR**: registra uma decisão já tomada. Depois de `Accepted`, não é editado;
  mudanças posteriores usam outro ADR que o sucede.

Uma RFC aprovada normalmente gera um ou mais ADRs pequenos, cada um registrando
uma decisão concreta. A implementação pode começar em modo experimental antes
da aprovação, desde que não altere contratos públicos nem seja apresentada como
decisão definitiva.

## Status

| # | Título | Status |
|---|---|---|
| [0001](0001-etl-v2-canonical-pipeline.md) | ETL V2: pipeline canônico tipado sob computação restrita | Proposed |
