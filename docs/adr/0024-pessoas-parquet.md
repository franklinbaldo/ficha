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
| `cpf_mascarado` | VARCHAR | Formato RFB: `***.<middle6>-**` |
| `nome_normalizado` | VARCHAR | UPPER + strip_accents + TRIM |
| `nome_original` | VARCHAR | Nome como publicado pela RFB |
| `papel` | ENUM | `socio_pf` ou `representante` |
| `cnpj_base` | VARCHAR(8) | Raiz do CNPJ |
| `qualificacao_codigo` | VARCHAR | Qualificação RFB |
| `data_entrada_sociedade` | VARCHAR | Presente apenas para `socio_pf` |
| `faixa_etaria` | VARCHAR | Código 0-9, presente apenas para `socio_pf` |

**Sort:** `(cpf_mascarado, nome_normalizado)`  
**Fonte:** tabela `socio` via UNION ALL de dois subsets.

## Inclusão e exclusão

| Tipo | Incluído | Motivo |
|------|----------|--------|
| Sócios PF (`identificador_socio='2'`) | ✅ | Identidade por CPF mascarado |
| Representantes legais (`representante_legal <> ''`) | ✅ | Presente em qualquer tipo de sócio |
| Sócios PJ (`'1'`) | ❌ | São empresas, não pessoas |
| Estrangeiros (`'3'`) sem CPF | ❌ | Sem campo de identificação uniforme |

## Chave composta e taxa de colisão

A RFB expõe apenas os dígitos do meio do CPF (`***.123.456-**`), gerando
~1M valores distintos para ~200M CPFs brasileiros (~200× colisão por CPF só).
Nome só também colide massivamente ("JOSÉ DA SILVA").

O **par `(cpf_mascarado, nome_normalizado)` é a chave composta** do parquet:
a probabilidade de dois indivíduos distintos compartilharem ambos é < 1 em 10⁶
para nomes comuns. Residual de falsos positivos é aceitável e documentado
("aparece em 7 empresas — pode incluir homônimos com mesmo CPF mascarado").

## Relação com `socios.parquet`

Não depreca `socios.parquet`. As duas visões são complementares:

| Parquet | Pergunta servida |
|---------|-----------------|
| `socios.parquet` | "Quais são os sócios da empresa X?" (forward) |
| `pessoas.parquet` | "Em quais empresas aparece a pessoa Y?" (reverse) |

Analogia com `cnpjs.parquet` + `raizes.parquet` (ADR 0008): redundância
barata que serve padrões de acesso genuinamente distintos.

## Postura de privacidade

Dados publicados diretamente pela RFB no dump público de CNPJ — sem enriquecimento,
sem desmascaramento de CPF. Aplica-se ADR 0004 e ADR 0006.

## Consequências

- +1 write no phase 3 do ETL (~5-10 min, ~6 GB peak — entrada pequena)
- Tabela `socio` é liberada da memória após write de `pessoas`
- Manifest ganha entrada `pessoas` com metadata de sort
- Frontend usa `attachPessoas(db, url)` para registrar e criar VIEW
- Schema Zod em `web/src/schemas/v1/pessoa.ts`
