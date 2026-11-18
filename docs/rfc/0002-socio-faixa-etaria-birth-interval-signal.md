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
**intervalo de nascimento estimado, relativo a cada snapshot**. Cada
snapshot produz o seu próprio intervalo, e esse intervalo POR SNAPSHOT
**muda sim** de um snapshot pro outro -- porque a data de referência `R`
muda a cada snapshot e o código de faixa etária bruto também pode mudar. A
propriedade estável não é o intervalo individual de um snapshot: é a
**interseção acumulada entre os intervalos de snapshots sucessivos**. Essa
interseção consolidada nunca cresce -- ela pode permanecer igual, ficar
mais estreita, ou ficar vazia -- conforme mais snapshots são observados,
mas nunca fica mais larga. O intervalo por snapshot e sua interseção
consolidada são propostos como **sinal auxiliar de diagnóstico e
reconciliação entre snapshots**, nunca como componente da chave primária
de relacionamento.

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
  janela de nascimento (ver seção 7). Muda a cada snapshot.
- **Intervalo de nascimento por snapshot**: `(birth_date_lower_exclusive,
  birth_date_upper_inclusive]` -- datas exatas, derivadas do código +
  `R` daquele snapshot específico (seção 9). Este é o intervalo
  AUTORITATIVO para teste de compatibilidade (seção 11.2). Muda de
  snapshot pra snapshot para a mesma pessoa real.
- **Limites conservadores em ano** (`birth_year_min`/`birth_year_max`):
  projeção do intervalo exato acima pro componente de ano, só pra
  apresentação e diagnóstico grosseiro -- NÃO é a fonte de verdade pra
  teste de compatibilidade (ver seção 11.2 sobre por que a interseção em
  nível de ano pode divergir da interseção exata).
- **Candidata de nível de pessoa**: CPF mascarado + nome normalizado, sem
  escopo de empresa -- a mesma granularidade que as candidatas de
  identidade de `socio_key_audit.py` já medem (`pf:cpf_nome`), sem incluir
  `cnpj_basico`.
- **Candidata de nível de relacionamento**: `cnpj_basico` (empresa) +
  candidata de nível de pessoa + fatos de papel/entrada
  (`qualificacao_socio` + `data_entrada_sociedade`) -- a mesma
  granularidade que a chave de relacionamento recomendada por #97
  slice 5 (`pf:relationship`).
- **Interseção consolidada (entre snapshots)**: interseção dos intervalos
  de nascimento por snapshot (datas exatas, não anos) de UMA MESMA
  candidata -- de pessoa OU de relacionamento, dependendo do escopo sendo
  medido -- observada em múltiplos snapshots. Não é prova de que os
  registros são a mesma pessoa real; é só um sinal de
  compatibilidade/incompatibilidade (ver seção 11.3).

## 5. Objetivos

1. Preservar a informação temporal contida em `faixa_etaria` sem
   reintroduzir instabilidade na chave de relacionamento.
2. Definir uma derivação determinística, auditável e testável do código
   RFB para um intervalo de nascimento por snapshot (datas exatas).
3. Definir, usando datas exatas (não a projeção em ano), como o intervalo
   por snapshot consolida ao observar a mesma candidata em múltiplos
   snapshots, e o que uma interseção vazia significa.
4. Separar reconciliação em **nível de pessoa** (CPF mascarado + nome
   normalizado, sem empresa) de reconciliação em **nível de
   relacionamento** (empresa + pessoa + papel/entrada), e documentar que
   agrupamento em nível de pessoa é inerentemente probabilístico -- CPF é
   mascarado e nomes podem colidir, então uma interseção não vazia não é
   prova de que dois registros são a mesma pessoa real.
5. Deixar claro que, **dentro de um único snapshot**, o intervalo derivado
   não carrega mais informação que o código bruto — o ganho real é
   **entre snapshots**.
6. Especificar um plano de testes e um plano de medição via
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
bordas entre faixas adjacentes é esperada e correta — ver o exemplo de
transição de faixa etária na seção 11.2).

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

**As datas exatas (`birth_date_lower_exclusive`/`birth_date_upper_inclusive`)
são a fonte de verdade para teste de compatibilidade entre intervalos
(seção 11.2). Os limites em ano acima são só apresentação e diagnóstico
grosseiro** -- duas janelas exatas podem ser disjuntas mesmo quando suas
projeções em ano ainda compartilham um ano em comum, porque a projeção em
ano descarta a informação de mês/dia que a subtração exata preserva (ver
o exemplo na seção 11.2).

## 10. Campos derivados

Todos os campos abaixo são **auxiliares/diagnósticos**, nunca parte de uma
chave persistente:

- `faixa_etaria_codigo`: valor bruto da RFB, preservado sem alteração.
- `faixa_etaria_snapshot`: o `snapshot_yyyymm` de origem do registro.
- `_birth_reference_date` (`R`): a data de referência efetivamente usada.
- `_birth_interval_source`: de onde veio `R` -- `"snapshot_asof"` (regra 1),
  `"snapshot_month_last_day"` (regra 2). Nunca `"workflow_run_date"` --
  essa origem não é uma opção válida (ver seção 7).
- `_birth_date_lower_exclusive_estimated` / `_birth_date_upper_inclusive_estimated`:
  os limites de data EXATOS (seção 9) -- fonte de verdade pra qualquer
  teste de compatibilidade/interseção (seção 11.2); `NULL` para código
  10/ausente; `_birth_date_lower_exclusive_estimated` também `NULL` para
  código 9.
- `_birth_year_min_estimated` / `_birth_year_max_estimated`: projeção dos
  limites exatos acima pro componente de ano -- só apresentação e
  diagnóstico grosseiro, NÃO usados pra teste de compatibilidade; mesmas
  regras de `NULL` que os campos de data exata.

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

### 11.2 Interseção entre snapshots -- datas exatas são a fonte de verdade

Ao observar a mesma candidata (de pessoa ou de relacionamento -- seção
11.3) em múltiplos snapshots, cada snapshot contribui seu **próprio
intervalo de nascimento exato** (seção 9). A interseção consolidada usa
SEMPRE as datas exatas, nunca a projeção em ano:

```text
consolidado_lower_exclusive = máximo dos birth_date_lower_exclusive observados
consolidado_upper_inclusive = mínimo dos birth_date_upper_inclusive observados

vazio quando consolidado_lower_exclusive >= consolidado_upper_inclusive
```

A condição de vazio usa `>=`, não só `>`: como o limite inferior é
exclusivo e o superior é inclusivo, um consolidado onde os dois valores
são exatamente iguais também é vazio -- o único ponto candidato é excluído
pelo próprio limite inferior. Ver os testes de fronteira abaixo.

**Por que não usar os limites em ano para este teste**: duas janelas
exatas podem ser disjuntas mesmo quando suas projeções em ano ainda
compartilham um ano em comum, porque a projeção em ano descarta
informação de mês/dia. Exemplo concreto:

- Intervalo A (de algum código/`R` cuja derivação resulte nisso): exato
  `(1988-01-01, 1990-06-30]`; projeção em ano `[1988, 1990]`.
- Intervalo B: exato `(1990-07-01, 1992-01-01]`; projeção em ano
  `[1990, 1992]`.
- As projeções em ano **parecem** se sobrepor em `1990`. As datas exatas
  **não se sobrepõem**: `máximo(1988-01-01, 1990-07-01) = 1990-07-01` é
  posterior a `mínimo(1990-06-30, 1992-01-01) = 1990-06-30` -- interseção
  exata vazia.

Se o código de produção usasse só a projeção em ano como teste de
compatibilidade, esse par seria erroneamente classificado como
compatível. Por isso a interseção em ano é só apresentação/diagnóstico
grosseiro (seção 9), nunca a implementação do teste de compatibilidade.

Um segundo exemplo mostra que uma transição real de faixa etária entre
dois snapshots próximos permanece corretamente compatível quando testada
com datas exatas: `R1 = 2026-01-31` com código `4` (31–40) produz
`(1985-01-31, 1995-01-31]`; `R2 = 2026-02-28` (snapshot seguinte) com
código `5` (41–50, a pessoa "mudou de faixa") produz
`(1975-02-28, 1985-02-28]`. A interseção exata é
`(1985-01-31, 1985-02-28]` -- não vazia (28 dias), então a candidata
continua compatível mesmo com o código de faixa etária tendo mudado.

### 11.3 Escopo: candidata de pessoa vs. candidata de relacionamento

A interseção de intervalos de nascimento deve ser medida em dois escopos
separados, com interpretações diferentes:

- **Nível de pessoa** (CPF mascarado + nome normalizado, sem
  `cnpj_basico`): testa se pessoas aparentemente idênticas -- possivelmente
  em empresas diferentes, ou no mesmo snapshot, ou em snapshots diferentes
  -- continuam compatíveis em idade. Como o CPF é mascarado e nomes podem
  colidir, uma candidata de nível de pessoa **não é uma prova de
  identidade real** -- é apenas um agrupamento por coincidência de campos
  observáveis. O agrupamento em nível de pessoa é, portanto,
  **inerentemente probabilístico**.
- **Nível de relacionamento** (`cnpj_basico` + candidata de pessoa +
  `qualificacao_socio` + `data_entrada_sociedade`): testa a continuidade
  de UMA relação empresa-sócio específica ao longo de snapshots -- a
  mesma granularidade que a chave recomendada por #97 slice 5.

**O intervalo de nascimento é um sinal auxiliar de incompatibilidade, não
uma prova de que linhas compatíveis são a mesma pessoa.** Uma interseção
vazia é evidência forte de que dois registros NÃO podem ser a mesma
pessoa real (dado o intervalo). Uma interseção não vazia não prova o
contrário -- só significa que o sinal disponível não encontrou uma
incompatibilidade; a candidata pode ainda ser duas pessoas reais
diferentes que coincidem em CPF mascarado, nome normalizado, e faixa
etária compatível. Isso vale com mais força ainda em nível de pessoa
(sem o escopo de empresa) do que em nível de relacionamento.

## 12. Plano de testes

Antes de qualquer implementação, a PR que fechar esta RFC deve incluir
testes cobrindo:

**Derivação por snapshot:**

- cada mapeamento código → `[idade_min, idade_max]` da tabela da seção 8;
- as datas exatas (`birth_date_lower_exclusive`/`birth_date_upper_inclusive`)
  e os anos-limite conservadores (`birth_year_min`/`birth_year_max`) pra
  pelo menos um código de cada extremo (`1` e `8`);
- o intervalo aberto por cima do código `9` (limite inferior ausente,
  limite superior derivado de idade 81);
- código `10` / valor ausente / valor inválido → nenhum intervalo inferido
  (campos `NULL`, não um intervalo vazio);
- o tratamento da data de referência: `R` = último dia de
  `snapshot_yyyymm` quando não há data as-of real; a origem persistida
  (`_birth_interval_source`) reflete corretamente qual regra foi usada.

**Interseção entre snapshots (datas exatas, seção 11.2) -- testes de
fronteira, obrigatórios:**

- duas janelas cujas projeções em ano se sobrepõem mas cujas datas exatas
  NÃO se sobrepõem (o exemplo `(1988-01-01,1990-06-30]` vs.
  `(1990-07-01,1992-01-01]` da seção 11.2) -- deve resultar em interseção
  exata vazia mesmo que uma implementação ingênua baseada em ano a
  classificasse como compatível;
- duas janelas que se tocam exatamente no limite inferior excluído (ex.:
  `(1988-01-01,1990-06-30]` vs. `(1990-06-30,1992-01-01]`) -- deve
  resultar em vazio, não em "compatível por um instante", porque o limite
  inferior é exclusivo;
- uma transição real de faixa etária entre snapshots sucessivos que
  permanece compatível (o exemplo `R1=2026-01-31`/código `4` vs.
  `R2=2026-02-28`/código `5` da seção 11.2, interseção não vazia de 28
  dias) -- confirma que o mecanismo não quebra o caso comum de uma pessoa
  real envelhecendo entre snapshots.

**Escopo pessoa vs. relacionamento (seção 11.3):**

- a mesma candidata de nível de PESSOA (CPF mascarado + nome) medida em
  duas empresas diferentes, com intervalos de nascimento compatíveis --
  documentar explicitamente no teste que compatibilidade aqui NÃO prova
  que é a mesma pessoa real, só que o sinal disponível não encontrou
  incompatibilidade;
- a mesma candidata de nível de RELACIONAMENTO (empresa + pessoa +
  papel/entrada) medida em snapshots sucessivos, intervalos compatíveis;
- um par com interseção vazia em nível de pessoa (sinal de identidades
  incompatíveis ou inconsistência de fonte) propagando corretamente pra
  qualquer candidata de relacionamento que a contenha.

## 13. Plano de medição

Depois de aceita, o próximo passo (fora do escopo desta RFC, mas
registrado aqui como consequência esperada) é medir a versão normalizada
temporalmente na mesma ferramenta que já mede `faixa_etaria` cru
(`socio_key_audit.py`/`docs/socio-key-investigation.md`):

- confirmar, com dado real, que o intervalo derivado não muda cardinalidade
  dentro de um único snapshot (seção 11.1);
- medir, contra pelo menos dois snapshots históricos reais, quantos pares
  de candidata de nível de PESSOA (mesmo CPF mascarado + nome normalizado,
  sem `cnpj_basico`) têm interseção EXATA (não em ano) vazia entre
  snapshots -- isso quantifica o valor do sinal como
  reconciliador/desambiguador nesse escopo, mais amplo e mais
  probabilístico;
- medir separadamente o mesmo, restrito a candidatas de nível de
  RELACIONAMENTO (com `cnpj_basico`) -- esse escopo é o que efetivamente
  importa para a chave recomendada por #97 slice 5, e a expectativa é que
  a taxa de interseção vazia seja próxima de zero ali (já que a chave de
  relacionamento já é quase perfeitamente única sem esse sinal, ver
  `docs/socio-key-investigation.md`);
- reportar as duas taxas separadamente, nunca uma única taxa combinada --
  misturar os dois escopos esconderia justamente a diferença entre "sinal
  probabilístico de agrupamento de pessoa" e "verificação de continuidade
  de um relacionamento já identificado por outras colunas".

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
- Se e como uma interseção não vazia em nível de PESSOA deveria acumular
  mais confiança com o tempo (ex.: compatível em três snapshots seguidos
  é mais forte que compatível em dois), dado que compatibilidade nunca é
  prova -- ou se essa noção de "confiança acumulada" é desnecessária
  porque o escopo de relacionamento já resolve o problema prático que
  motivou esta RFC.

## 17. Decisão solicitada

Aceitar esta RFC como a base para uma implementação futura (fora do
escopo de #97 slice 5 / PR #108) que:

1. adiciona a derivação de intervalo de nascimento por snapshot (datas
   exatas + projeção em ano só para apresentação) como campos
   diagnósticos em `socio_key_audit.py`, seguindo as fórmulas da seção 9
   e a tabela de mapeamento da seção 8;
2. implementa a interseção consolidada usando SEMPRE as datas exatas
   (seção 11.2), com os testes de fronteira da seção 12;
3. mede, contra pelo menos dois snapshots históricos reais, o valor do
   sinal como reconciliador entre snapshots, separadamente em nível de
   pessoa e em nível de relacionamento (seção 13);
4. documenta explicitamente que candidatas de nível de pessoa são
   probabilísticas -- compatibilidade de intervalo não é prova de
   identidade (seção 11.3);
5. atualiza `docs/socio-key-investigation.md` para descrever
   `faixa_etaria` como transformável num sinal auxiliar em vez de
   simplesmente descartável;
6. não altera a chave de relacionamento recomendada de nenhuma categoria
   de sócio, nem declara `SOCIO_CANONICAL`.
