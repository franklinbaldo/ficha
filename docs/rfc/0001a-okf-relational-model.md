---
type: RFC
title: Emenda à RFC 0001 — modelo relacional OKF sobre Parquets publicados
status: draft
description: Substitui a meta de uma segunda camada de Parquets canônicos por contratos relacionais OKF e views de conveniência materializáveis sob demanda.
---

# RFC 0001A — modelo relacional OKF sobre Parquets publicados

## Decisão proposta

A RFC 0001 acertou ao exigir tipagem explícita, regras com dono, validação relacional e derivações reproduzíveis. Ela errou ao transformar essas propriedades numa obrigação física: `raw -> Parquets canônicos -> Parquets de produto`.

O Ficha já publica relações Parquet estáveis e verificáveis. Criar outra família obrigatória de Parquets apenas para lhes atribuir autoridade semântica duplica armazenamento, transporte e lifecycle sem produzir informação nova.

Esta emenda propõe outra fronteira:

```text
snapshot publicado (Parquets)
        +
modelo OKF dos shapes relacionais
        |
        +--> JSON Schema / Zod / tipos
        +--> contratos DuckDB
        +--> relações entre tabelas
        +--> views de conveniência
        +--> materializações opcionais
```

Nenhuma view recebe o rótulo de "canônica". Uma view é uma interpretação nomeada para uma tarefa. Materializá-la em Parquet não aumenta sua autoridade; apenas troca custo de computação por storage.

## Como o OKF entra

Um documento OKF continua sendo o que já é: uma linha de uma relação identificada por `type`. Para modelar o shape de uma relação publicada, mantemos linhas representativas dos tipos `Cnpj`, `Raiz`, `Socio` etc. O `okf-parser` compila o mesmo bundle para relações DuckDB e gera projeções de schema.

Os milhões de registros não são convertidos para Markdown. O Parquet continua sendo a materialização massiva das linhas daquele shape. O trabalho de integração é provar que o contrato inferido/declarado para o tipo e o schema observado do Parquet concordam.

## Relações e views

Os tipos podem ser conectados por suas chaves naturais, por exemplo:

```text
Cnpj.cnpj_base  N:1  Raiz.cnpj_base
Socio.cnpj_base N:1 Raiz.cnpj_base
```

Isso permite gerar e validar joins DuckDB sem transformar o resultado em nova fonte de verdade. Views como `empresas_ativas`, `quadro_societario` ou `empresas_por_uf` ficam versionadas como conveniência. Um consumidor pode executá-las diretamente em DuckDB/DuckDB-WASM ou materializá-las quando o perfil de uso justificar.

## Efeito sobre a RFC 0001 original

Ficam preservados como aprendizado e ferramentas: registry de leitura da RFB, investigação de tipos/casts, estudos de chaves e cardinalidade, métricas, gates de equivalência e evidência histórica dos shadow runs.

Deixam de ser objetivo arquitetural obrigatório: `canonical/{tabela}/part-*.parquet`, migração de todos os writers para uma segunda camada física, retenção permanente dessa camada e a noção de que apenas ela poderia alimentar produtos.

A RFC 0001 original permanece como registro da hipótese e dos experimentos que produziram esse aprendizado. Esta emenda substitui apenas sua conclusão sobre a necessidade de uma camada física intermediária obrigatória.

## Primeiro experimento

1. representar `Cnpj`, `Raiz` e `Socio` em OKF com linhas derivadas das fixtures já usadas pelo ETL;
2. gerar JSON Schema e Zod com `okf-parser`;
3. comparar o shape gerado com os três Parquets do snapshot corrente;
4. declarar `cnpj_base` como relação entre os tipos quando o suporte relacional do parser estiver disponível na versão consumida;
5. ligar uma view simples ao DuckDB-WASM do site sem materialização nova.

Só depois desse ciclo decidimos quais capacidades pertencem genericamente ao `okf-parser` e quais ficam no Ficha.
