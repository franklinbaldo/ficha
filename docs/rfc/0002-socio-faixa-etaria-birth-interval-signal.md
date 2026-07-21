# RFC 0002 — `faixa_etaria` de `socio` como sinal auxiliar de intervalo de nascimento

**Status:** Draft
**Data:** 2026-07-21
**Escopo:** derivação diagnóstica/auxiliar a partir de `socio.faixa_etaria`; não altera a chave de relacionamento recomendada nem declara `SOCIO_CANONICAL`

## 1. Resumo

[Issue #97](https://github.com/franklinbaldo/ficha/issues/97) slice 5 e
[docs/socio-key-investigation.md](../socio-key-investigation.md) (PR #108)
recomendam excluir `faixa_etaria` (faixa etária do sócio pessoa física) da
chave de relacionamento recomendada, com o argumento de que o código é
**temporalmente instável**: a faixa etária de uma pessoa real muda conforme
ela envelhece entre snapshots.

Essa RFC propõe **não descartar a informação**, e sim transformá-la num
**intervalo de nascimento estimado, relativo ao snapshot** — uma quantidade
que, ao contrário do código bruto, não muda de valor entre snapshots para a
mesma pessoa real (só pode ficar mais estreita conforme mais snapshots são
observados). Esse intervalo é proposto como **sinal auxiliar de
diagnóstico e reconciliação entre snapshots**, nunca como componente da
chave primária de relacionamento.

A chave de relacionamento de pessoa física recomendada por #97 slice 5
continua:

```text
cnpj_basico + cpf_mascarado + nome_normalizado + qualificacao_socio + data_entrada_sociedade
```

Esta RFC não muda essa recomendação. Ela propõe um campo derivado adicional,
só para fins de auditoria/diagnóstico, a ser medido pela mesma ferramenta de
investigação (`socio_key_audit.py`) antes de qualquer decisão de schema
canônico.

## 2. Contexto

`socio.faixa_etaria` é um código RFB (1 a 10) representando uma faixa etária
fechada (`até 12`, `13–20`, ..., `71–80`, `acima de 80`) ou "não informada".
A auditoria de #97 slice 5 mediu esse campo apenas como diagnóstico
identidade-nível (`same_masked_cpf_and_name_different_faixa_etaria_count`) e
concluiu que ele resolve pouco (77 casos em 26,8M linhas de pessoa física no
snapshot 2026-04) e que, no nível de relacionamento recomendado, incluí-lo
não muda nenhum número — a chave recomendada já chega a zero duplicatas sem
ele.

A revisão que motivou essa medição (comentário na PR #108) apontou uma
lacuna conceitual: o código bruto é instável **como valor categórico**, mas
carrega uma restrição temporal real e estável — um intervalo de anos de
nascimento plausíveis, dado o código e a data do snapshot. Descartar o
campo inteiramente joga fora essa restrição; codificá-lo cru na chave
mistura um atributo que muda ao longo do tempo com atributos que não
mudam (`qualificacao_socio`, `data_entrada_sociedade`). A alternativa
correta é derivar uma quantidade que seja, ela mesma, estável por
construção.

## 3. Decisões existentes preservadas

Esta RFC não revisita:

- a recomendação de chave de #97 slice 5 / PR #108 para nenhuma categoria
  de sócio (pessoa jurídica, física, estrangeiro);
- o modelo de candidatas identidade/relacionamento de `socio_key_audit.py`;
- a decisão de que `conflicting_key_count` só é computado (via comparação
  real, não hash) para candidatas de nível de relacionamento;
- a ausência de `SOCIO_CANONICAL` — esta RFC não propõe declarar um
  contrato canônico para `socio`, nem aqui nem como consequência direta.

## 4. Terminologia

- **Código de faixa etária**: o valor bruto de `socio.faixa_etaria` (1–10).
- **Data de referência (`R`)**: a data usada para converter o código numa
  janela de nascimento (ver seção 6).
- **Intervalo de nascimento estimado**: par `[birth_year_min, birth_year_max]`
  (ou par de datas exatas) derivado do código + `R`, representando o
  conjunto de anos de nascimento compatíveis com aquele código naquele
  snapshot.
- **Consolidação entre snapshots**: interseção dos intervalos de nascimento
  estimados da mesma identidade observada em múltiplos snapshots.

## 5. Objetivos

1. Preservar a informação temporal contida em `faixa_etaria` sem
   reintroduzir instabilidade na chave de relacionamento.
2. Definir uma derivação determinística, auditável e testável do código
   RFB para um intervalo de nascimento.
3. Definir como o intervalo derivado se consolida ao observar a mesma
   identidade em múltiplos snapshots, e o que uma interseção vazia
   significa.
4. Deixar claro que, **dentro de um único snapshot**, o intervalo derivado
   não carrega mais informação que o código bruto — o ganho real é
   **entre snapshots**.
5. Especificar um plano de testes e um plano de medição via
   `socio_key_audit.py`, sem implementar ainda.

## 6. Não objetivos

- Não declara `SOCIO_CANONICAL` nem qualquer contrato canônico para
  `socio`.
- Não altera a chave de relacionamento recomendada para nenhuma categoria
  de sócio.
- Não afirma conhecer a data de nascimento real de ninguém — o intervalo
  derivado é uma restrição conservadora, não uma estimativa pontual.
- Não implementa a derivação em `socio_key_audit.py` nem em nenhum
  writer — esta RFC precisa ser aceita antes de qualquer PR de
  implementação.
- Não resolve como/onde a consolidação entre snapshots seria persistida
  em produção (fora de escopo: isso depende de decisões de #97 slices 6/7
  ainda não tomadas).

## 7. Data de referência (`R`)

A conversão do código pra intervalo de nascimento depende de uma data de
referência explícita. Regras, em ordem de preferência:

1. usar uma data real de snapshot/as-of quando disponível na fonte (se a
   RFB algum dia publicar uma data de referência explícita por registro
   ou por arquivo);
2. na ausência disso — o caso de hoje — usar o **último dia do mês do
   snapshot** (`snapshot_yyyymm`), ex.: `2026-04-30` para o snapshot
   `2026-04`;
3. **nunca** usar a data de execução do workflow do GitHub Actions nem a
   data de criação do artefato — essas são datas de processamento, não
   datas do dado, e variam por motivos operacionais (reprocessamento,
   reanálise) sem relação com quando o snapshot foi realmente extraído
   pela RFB.

Qual data foi usada e qual suposição (regra 1, 2 ou 3) foi aplicada devem
ser persistidas junto com o intervalo derivado — nunca implícitas.

## 8. Mapeamento código → faixa etária

Baseado no layout de códigos da RFB para `faixa_etaria`:

| Código | Faixa etária | `idade_min` | `idade_max` |
|---|---|---:|---:|
| 1 | até 12 anos | 0 | 12 |
| 2 | 13–20 anos | 13 | 20 |
| 3 | 21–30 anos | 21 | 30 |
| 4 | 31–40 anos | 31 | 40 |
| 5 | 41–50 anos | 41 | 50 |
| 6 | 51–60 anos | 51 | 60 |
| 7 | 61–70 anos | 61 | 70 |
| 8 | 71–80 anos | 71 | 80 |
| 9 | acima de 80 anos | 81 | *(sem limite superior de idade)* |
| 10 ou ausente | não informada | *(sem intervalo inferido)* | *(sem intervalo inferido)* |

Código 9 é aberto por cima em idade (sem `idade_max`), o que se traduz em
**sem limite inferior de ano de nascimento** (`birth_year_min` indefinido) e
um limite superior de ano de nascimento derivado de `idade_min = 81`.
Código 10 (ou valor ausente/inválido) não produz nenhum intervalo — os
campos derivados ficam `NULL`, e isso é reportado como
"sem faixa informada", não como um intervalo vazio.

## 9. Fórmulas de derivação

Para um código com intervalo fechado `[idade_min, idade_max]` e data de
referência `R`:

```text
birth_date_lower_exclusive = R - (idade_max + 1) anos
birth_date_upper_inclusive = R - idade_min anos
```

(o limite inferior é exclusivo e o superior é inclusivo porque uma pessoa
com `idade_max` anos completos ainda não fez `idade_max + 1`; a data exata
de aniversário dentro do ano é desconhecida, então a sobreposição nas
bordas entre faixas adjacentes é esperada e correta — ver seção 10).

Para código 9 (aberto por cima): `birth_date_upper_inclusive = R - 81 anos`,
sem `birth_date_lower_exclusive`.

Versão conservadora em nível de ano (mais simples de auditar/ler,
derivada das datas exatas acima):

```text
birth_year_min = ano(R) - idade_max - 1
birth_year_max = ano(R) - idade_min
```

Exemplo ilustrativo com `R` em 2026 (granularidade de ano; a derivação real
usa a data completa de `R`, não só o ano):

| Faixa | Intervalo conservador de nascimento (ano) |
|---|---:|
| até 12 | 2013–2026 |
| 13–20 | 2005–2013 |
| 21–30 | 1995–2005 |
| 31–40 | 1985–1995 |
| 41–50 | 1975–1985 |
| 51–60 | 1965–1975 |
| 61–70 | 1955–1965 |
| 71–80 | 1945–1955 |
| acima de 80 | até 1945 |

## 10. Campos derivados

Todos os campos abaixo são **auxiliares/diagnósticos**, nunca parte de uma
chave persistente:

- `faixa_etaria_codigo`: valor bruto da RFB, preservado sem alteração.
- `faixa_etaria_snapshot`: o `snapshot_yyyymm` de origem do registro.
- `_birth_reference_date` (`R`): a data de referência efetivamente usada.
- `_birth_interval_source`: de onde veio `R` -- `"snapshot_asof"` (regra 1),
  `"snapshot_month_last_day"` (regra 2). Nunca `"workflow_run_date"` --
  essa origem não é uma opção válida (ver seção 7).
- `_birth_year_min_estimated` / `_birth_year_max_estimated`: os limites de
  ano conservadores (seção 9); `NULL` para código 10/ausente;
  `_birth_year_min_estimated` também `NULL` para código 9.
- limites de data exatos (`_birth_date_lower_exclusive_estimated` /
  `_birth_date_upper_inclusive_estimated`): mantidos internamente para
  quem precisar de precisão sub-ano; os campos de ano acima são a
  interface principal para diagnóstico.

Nenhum desses campos entra na chave de relacionamento recomendada de
`socio` pessoa física (seção 1) nem em qualquer candidata testada por
`socio_key_audit.py` como parte da IDENTIDADE recomendada -- eles são
medidos como diagnóstico adicional, do mesmo jeito que
`same_masked_cpf_and_name_different_faixa_etaria_count` já é hoje.

## 11. Uso como sinal auxiliar

### 11.1 Dentro de um único snapshot

Um único snapshot não ganha informação: converter o código `4` pra
`1985–1995` é só outra representação do mesmo dado, com o mesmo poder de
distinção que o código original. **Esta RFC não afirma que a versão
derivada melhora cardinalidade dentro de um snapshot isolado**, e o plano
de medição (seção 13) deve confirmar isso explicitamente, não assumir.

### 11.2 Entre snapshots -- o ganho real

Ao observar a mesma identidade candidata (ex.: mesmo `cnpj_basico` +
CPF mascarado + nome normalizado) em múltiplos snapshots, os intervalos de
nascimento estimados podem ser consolidados por interseção:

```text
ano_min_consolidado = máximo dos ano_min observados entre os snapshots
ano_max_consolidado = mínimo dos ano_max observados entre os snapshots
```

Duas propriedades importantes:

- a interseção **nunca cresce** conforme mais snapshots são observados --
  ela só permanece igual ou estreita;
- **uma interseção vazia** (`ano_min_consolidado > ano_max_consolidado`)
  é evidência de que os dois registros não podem ser a mesma pessoa (dado
  o mascaramento de CPF e a possibilidade de nomes normalizados
  coincidirem) -- ou de inconsistência na fonte. Isso vira um sinal de
  diagnóstico, não uma correção automática.

Esse mecanismo também explica corretamente uma pessoa real migrando de
`31–40` para `41–50` entre dois snapshots: os intervalos de nascimento das
duas faixas se sobrepõem parcialmente (ver a sobreposição de bordas na
tabela da seção 9), então a interseção continua não vazia -- a identidade
não muda "de repente" mesmo com o código de faixa etária mudando.

## 12. Plano de testes

Antes de qualquer implementação, a PR que fechar esta RFC deve incluir
testes cobrindo:

- cada mapeamento código → `[idade_min, idade_max]` da tabela da seção 8;
- os anos-limite exatos (`birth_year_min`/`birth_year_max`) para pelo
  menos um código de cada extremo (`1` e `8`);
- o intervalo aberto por cima do código `9` (`birth_year_min` ausente,
  `birth_year_max` derivado de idade 81);
- código `10` / valor ausente / valor inválido → nenhum intervalo inferido
  (campos `NULL`, não um intervalo vazio);
- o tratamento da data de referência: `R` = último dia de
  `snapshot_yyyymm` quando não há data as-of real; a origem persistida
  (`_birth_interval_source`) reflete corretamente qual regra foi usada;
- dois snapshots sucessivos onde a mesma pessoa muda de faixa etária mas
  os intervalos de nascimento inferidos continuam compatíveis
  (interseção não vazia);
- dois registros cujos intervalos têm interseção vazia (sinal de
  identidades incompatíveis ou inconsistência de fonte).

## 13. Plano de medição

Depois de aceita, o próximo passo (fora do escopo desta RFC, mas
registrado aqui como consequência esperada) é medir a versão normalizada
temporalmente na mesma ferramenta que já mede `faixa_etaria` cru
(`socio_key_audit.py`/`docs/socio-key-investigation.md`):

- confirmar, com dado real, que o intervalo derivado não muda cardinalidade
  dentro de um único snapshot (seção 11.1);
- medir, contra pelo menos dois snapshots históricos reais, quantos pares
  de identidade candidata (mesmo CPF mascarado + nome normalizado) têm
  interseção vazia entre snapshots -- isso quantifica o valor do sinal
  como reconciliador/desambiguador entre snapshots, que é o motivo real
  de preservar a informação em vez de descartá-la.

Este plano de medição não é implementado por esta RFC.

## 14. Alternativas consideradas

- **Descartar `faixa_etaria` inteiramente** (a posição da PR #108 antes
  desta revisão): simples, mas joga fora uma restrição temporal real que
  não tem outro substituto nos dados disponíveis.
- **Manter só o código bruto como diagnóstico, sem derivar intervalo**:
  não resolve o problema de comparar a mesma pessoa entre snapshots
  quando o código muda de valor.
- **Estimar um ano de nascimento pontual (ex.: ponto médio da faixa)** em
  vez de um intervalo: fabricaria precisão que o dado não sustenta --
  epistemicamente desonesto, rejeitado.
- **Incluir o intervalo (ou o código bruto) na chave de relacionamento
  persistente**: reintroduziria a instabilidade temporal que motivou esta
  RFC; rejeitado explicitamente pelos não objetivos (seção 6).

## 15. Consequências

- Nenhuma mudança de código, schema ou comportamento até esta RFC ser
  aceita.
- Se aceita, a implementação é uma extensão diagnóstica de
  `socio_key_audit.py` (novos campos derivados + medição), não uma
  mudança na chave recomendada nem em `SOCIO_CANONICAL` (que continua
  não implementado).
- `docs/socio-key-investigation.md` ganharia, na implementação, uma nota
  atualizada: `faixa_etaria` bruto é inadequado pra chave persistente por
  ser instável, mas sua informação não deve ser descartada -- deve virar
  um intervalo de nascimento relativo ao snapshot, retido como sinal
  auxiliar de reconciliação entre snapshots.

## 16. Questões abertas

- Onde e como a consolidação entre snapshots (seção 11.2) seria
  persistida em produção -- depende de decisões ainda não tomadas em
  #97 slices 6/7 sobre o formato de qualquer futura camada de
  reconciliação de identidade entre snapshots.
- Se/quando a RFB expuser uma data as-of real por registro (regra 1 da
  seção 7), como isso se reconcilia com registros históricos que só têm a
  regra 2 (fim do mês do snapshot) disponível.
- Se o ganho medido (seção 13) entre snapshots reais justifica o custo de
  manter os campos derivados em produção, ou se o valor é majoritariamente
  o efeito documental de deixar claro por que `faixa_etaria` foi excluído
  da chave.

## 17. Decisão solicitada

Aceitar esta RFC como a base para uma implementação futura (fora do
escopo de #97 slice 5 / PR #108) que:

1. adiciona a derivação de intervalo de nascimento como campos
   diagnósticos em `socio_key_audit.py`, seguindo as fórmulas da seção 9
   e a tabela de mapeamento da seção 8;
2. mede, contra pelo menos dois snapshots históricos reais, o valor do
   sinal como reconciliador entre snapshots (seção 13);
3. atualiza `docs/socio-key-investigation.md` para descrever
   `faixa_etaria` como transformável num sinal auxiliar em vez de
   simplesmente descartável;
4. não altera a chave de relacionamento recomendada de nenhuma categoria
   de sócio, nem declara `SOCIO_CANONICAL`.
