# ADR 0024 — `pessoas.parquet`: reverse lookup PF por CPF mascarado + nome

**Status:** Aceito  
**Data:** 2026-05-26  
**Contexto:** docs/perf-plan-2026-05.md §8

---

## Contexto

`socios.parquet` é sorted por `cnpj_base` — otimizado para "sócios da empresa X"
mas ineficiente para a direção inversa: "em quais empresas aparece a pessoa Y?".
Esse padrão de consulta é central para o caso de uso de transparência do FICHA.

## Decisão

Produzir `pessoas.parquet` como índice inverso de pessoas físicas:

| Coluna | Tipo | Notas |
|--------|------|-------|
| `cpf_mascarado` | VARCHAR | Formato RFB: CPF mascarado |
| `nome_normalizado` | VARCHAR | UPPER + strip_accents + TRIM |
| `nome_original` | VARCHAR | Nome como publicado pela RFB |
| `papel` | ENUM | `socio_pf` ou `representante` |
| `cnpj_base` | VARCHAR(8) | Raiz do CNPJ |
| `qualificacao_codigo` | VARCHAR | Qualificação RFB |
| `qualificacao_descricao` | VARCHAR | descrição denormalizada da qualificação |
| `faixa_etaria` | VARCHAR | Código 0-9; NULL para representantes |

**Grão:** `(cpf_mascarado, nome_normalizado, faixa_etaria, cnpj_base, papel)` — uma linha por pessoa × empresa × papel.  
**Sort:** `(cpf_mascarado, nome_normalizado)`.

A fonte continua sendo `socio`. `qualificacao_descricao` é acrescentada no ETL
por `LEFT JOIN` com o minúsculo `lookup_qualificacoes`, tanto para o papel de
sócio PF quanto para representante legal.

O lookup parquet de qualificações continua existindo como projeção auxiliar
barata (ADR 0019), mas não é necessário para interpretar/renderizar uma linha
de `pessoas.parquet`. Essa é a mesma regra já usada por `socios.parquet`.

`faixa_etaria` é atributo da pessoa e ajuda a desambiguar homônimos. É NULL para
representantes porque a RFB não publica esse campo em
`representante_legal_*`.

`data_entrada_sociedade` permanece em `socios.parquet`, pois é propriedade do
vínculo sócio×empresa.

## Inclusão e exclusão

| Tipo | Incluído | Motivo |
|------|----------|--------|
| Sócios PF (`identificador_socio='2'`) | ✅ | Identidade por CPF mascarado |
| Representantes legais (`representante_legal <> ''`) | ✅ | Presente em qualquer tipo de sócio |
| Sócios PJ (`'1'`) | ❌ | São empresas, não pessoas |
| Estrangeiros (`'3'`) sem CPF | ❌ | Sem campo de identificação uniforme |

## Chave composta e taxa de colisão

O par `(cpf_mascarado, nome_normalizado)` continua sendo a chave composta do
índice. `faixa_etaria` fornece sinal adicional de desambiguação quando
presente. Nenhum enriquecimento ou desmascaramento é realizado.

## Relação com `socios.parquet`

| Parquet | Pergunta servida |
|---------|-----------------|
| `socios.parquet` | "Quais são os sócios da empresa X?" (forward) |
| `pessoas.parquet` | "Em quais empresas aparece a pessoa Y?" (reverse) |

As duas projeções são deliberadamente redundantes para servir padrões de acesso
distintos; ambas carregam descrições necessárias à interpretação de suas
próprias linhas.

## Consequências

- +1 write no ETL para o índice inverso;
- tabela `socio` continua podendo ser liberada após `pessoas`;
- manifest mantém entrada `pessoas` com metadata de sort;
- schema Zod aceita ausência de `qualificacao_descricao` apenas para snapshots históricos;
- `lookups/qualificacoes.parquet` continua útil para consultas sobre o vocabulário.
