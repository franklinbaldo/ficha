---
type: View
name: socios_de_raizes_ativas
inputs:
  - Socio
  - Raiz
output: Socio
purpose: Filtrar vínculos societários de raízes com pelo menos um estabelecimento ativo.
sql: |
  SELECT s.*
  FROM socios AS s
  JOIN raizes AS r USING (cnpj_base)
  WHERE r.qtd_estabelecimentos_ativos > 0
---

# Sócios de raízes ativas

View de conveniência relacional entre [Socio](../types/socio.md) e [Raiz](../types/raiz.md). Preserva exatamente o shape de `Socio`: o join é usado apenas como filtro, portanto o output continua sendo o tipo declarado `Socio`.

Não é fonte de verdade nem materialização obrigatória. Pode ser executada diretamente em DuckDB/DuckDB-WASM ou materializada posteriormente se benchmark justificar.
