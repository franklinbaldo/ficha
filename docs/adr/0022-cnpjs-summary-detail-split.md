# ADR 0022 — `cnpjs_summary.parquet` for autocomplete / search (W4.2)

**Status:** Proposed
**Data:** 2026-05-15

## Contexto

Conforme analisado no plano de performance (§4.2 / W4.2), a busca principal por prefixos de Razão Social no backend atualmente atravessa a estrutura denormalizada pesada de `cnpjs.parquet`.

Isso provoca downloads e extrações desnecessárias que atingem o limite e atrasam o autocomplete devido ao volume que é trafegado pela rede do front-end. O split entre uma representação pequena para busca de listagem (search) e a representação maior para consulta final (detail) otimiza significativamente o tempo de uso primário no cliente (cold-cache).

## Decisão

Adicionar um novo Parquet, `cnpjs_summary.parquet`, contendo os atributos essenciais necessários para o listamento dos resultados via `SearchCNPJ.svelte`.

- **Schema:** Uma projeção de ~10 colunas extraídas do schema principal de `cnpjs.parquet`.
- **Sort:** Ordenado pela coluna `cnpj`.
- **Bloom filters:** Nas colunas `cnpj` e `razao_social_normalizada`.
- **Responsabilidades da UI:** O componente principal de visualização de detalhes (`EmpresaFicha.svelte`) continuará consumindo o modelo mais amplo `cnpjs.parquet`. O componente de listas de busca e autocomplete (`SearchCNPJ.svelte`) vai transicionar para usar exclusivamente `cnpjs_summary.parquet`.

*Nota de implementação: Como o PR de W4.2 ainda está sob revisão ou não fundido em branchs subjacentes, a implementação correspondente viverá em uma ramificação paralela/pendente associada.*

## Por quê

- **Redução de Payload Drástica:** Fazer o split de visualização economiza muito processamento. Ele corta os bytes para os downloads síncronos frios em ~5×. O tráfego analítico de listagens na web é assimétrico às visualizações individuais profundas.

## Consequências

- ✅ A performance das pesquisas frias (cold-cache searches) e da renderização em lista por Razão Social ganham expressivo impulso.
- ⚠️ O ETL introduz mais uma saída (arquivos gerados para os dumps mensais). Requer coordenação entre a disponibilização na UI contra o build deste snapshot.
- ⚠️ Os esquemas em diferentes UI-components da base de front-end deverão explicitamente entender que os metadados reduzidos de CNPJ e os metadados totais vivem em objetos Parquet diferentes.

## Referências

- PR W4.2 (Em revisão/espera)
- Plano de Performance `docs/perf-plan-2026-05.md` (§4.2 / W4.2)
