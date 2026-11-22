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
| [0001](0001-etl-v2-canonical-pipeline.md) | ETL V2: pipeline canônico tipado sob computação restrita | Accepted |
| [0001A](0001a-okf-relational-model.md) | Emenda: modelo relacional OKF sobre Parquets publicados | Draft; decisão física pendente |
| [Fence raw F, 2026-11](2026-11-raw-remote-fence-falsification.md) | Critérios adversariais para 37 ZIPs, sem autorização de publicação | Draft / HOLD |
| [Visão de produto, 2026-11](ficha-greenfield-visao-produto-2026-11.md) | FICHA: fichário público e acervo por competência | Direção aprovada; implementação incremental pendente |
