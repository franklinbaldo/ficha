# ADR 0021 — `cnpj_contatos.parquet` — reverse contact lookup (W12)

**Status:** Accepted
**Data:** 2026-05-15

## Contexto

Conforme discutido no plano de performance (§12 / W12), a entidade `cnpjs.parquet` armazena dados de contatos (e-mail, fax e telefones 1 e 2) através de colunas denormalizadas muito largas. Elas permanecem ideais para visualização unitária na lâmina da empresa.

Entretanto, consultas transversais como "quem é o dono deste telefone?" ou "quais os domínios de email de governos?" são pesadas demais. Isso exige um arquivo dedicado para inverter os eixos, focado em consultas analíticas de contatos.

## Decisão

Criar o Parquet `cnpj_contatos.parquet` como um índice inverso unificando múltiplos campos esparsos em uma modelagem com formato key/value explícita.

- **Schema:** `(cnpj, cnpj_base, tipo, valor, posicao)`.
- **Domínio:** O campo `tipo` assume os valores `{'telefone', 'fax', 'email'}`.
- **Sort:** Ordenado por `(tipo, valor, cnpj)`.
- **Bloom filters:** Aplicados nas colunas `valor` e em uma expressão extraída de `split_part(valor, '@', 2)` (representando o domínio de email).
- **Colunas largas:** Permanecem em `cnpjs.parquet` para fornecer renderizações mais rápidas no front end.

### Postura de Privacidade (PII)

Os e-mails e telefones atrelados às empresas são PII (Personally Identifiable Information). Porém, **a RFB publica estes dados em formato público**. Este Parquet é estritamente uma reordenação mecânica do mesmo dado e de maneira alguma realiza enriquecimento. A transparência desta postura está devidamente formalizada.

## Por quê

- **Consultas em Frações de Segundos:** Através do bloom e ordenação no Parquet, a busca reversa por um telefone isola imediatamente os pequenos blocos de row groups correspondentes ao invés de forçar o download completo de milhares de contatos.
- **Detecção e Analytics:** Analisar o domínio público extraído do e-mail é feito sem esforço de manipulação de strings na engine, e localizar "quais CNPJs compartilham este mesmo telefone" (uma prática de identificação de fraude) se torna prático.
- **Limitações do Arquivo Principal:** O `cnpjs.parquet` não perderá sua utilidade principal na camada de visualização por continuar abrigando colunas unificadas, servindo os propósitos essenciais da lâmina de dados em cache no cliente.

## Consequências

- ✅ Análises investigativas sobre contatos se tornam escaláveis via DuckDB-WASM, isolando dados relevantes em poucos kilobytes de rede.
- ✅ Possibilita filtros em tempo real usando porções calculadas de domínios sem penalidade computacional na UI.
- ⚠️ Trade-off no tamanho final da distribuição, visto que geramos arquivos invertidos extras para dados preexistentes no formato denormalizado das lâminas.

## Referências

- PR #37
- Plano de Performance `docs/perf-plan-2026-05.md` (§12 / W12)
