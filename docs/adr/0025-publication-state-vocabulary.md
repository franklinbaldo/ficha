# ADR 0025 — vocabulário de estados de publicação

**Status:** Aceito
**Data:** 2026-08-11
**Contexto:** #131, evidência do run `31450937194` (#110)

---

## Contexto

O run `31450937194` publicou um artifact chamado `final-manifest-2026-05`. O
conteúdo real eram 825 bytes: o `web/public/manifest.json` intocado, isto é, o
manifesto de **`2026-04`**. O pipeline havia sido cancelado durante o upload de
`companies.zip` e nunca chegou a `build_snapshot_entry()`.

Nenhum gate de integridade falhou. O step que de fato promoveria
(`Commit verified manifest`, condicionado ao sucesso do pipeline) foi
corretamente pulado. O defeito foi de **nomenclatura**: o artifact era gerado
sob `if: always()` e anunciava um estado que o run não alcançou.

O custo é real. Durante a investigação de #110, um artifact com esse nome é a
primeira coisa que alguém abre para saber se a competência foi publicada — e
ele responde a pergunta errada com aparência de autoridade.

A causa de fundo é que o pipeline distingue estados que os nomes não separam.

## Decisão

Adotar cinco estados nomeados, e exigir que qualquer evidência produzida por um
workflow declare a qual deles pertence:

| # | Estado | Significa | Evidência típica |
|---|--------|-----------|------------------|
| 1 | **tentativa** | um run existiu | logs, `transform-metrics-<mês>` |
| 2 | **materialização** | artefatos duráveis no IA | metadata de `ia:ficha-<mês>` |
| 3 | **candidato** | entry construída, ainda não verificada | saída de `build_snapshot_entry()` |
| 4 | **promovido** | passou `verify_snapshot_files()` e foi comitado | `manifest-promoted-<mês>` |
| 5 | **público** | o que `main` anuncia em `web/public/manifest.json` | manifesto servido |

Regras derivadas, verificadas estaticamente em
`etl/tests/test_workflow_publication_naming.py`. Ambas só se aplicam a
**artifacts de publicação** — os que apontam para `web/public/manifest.json` ou
cujo nome contém `manifest`/`publication`/`publish`:

1. Um artifact cujo nome anuncia publicação concretizada (`promoted`, `final`,
   `published`) **exige gate explícito**: um `if:` que referencie
   `steps.<id>.outcome`/`.conclusion` ou `success()`. Nem `always()` nem a
   ausência de `if:` satisfazem a regra.
2. Um artifact que publica `web/public/manifest.json` sem gate explícito
   **deve** declarar que não foi promovido, via prefixo `manifest-before-`,
   `manifest-candidate-` ou `manifest-attempt-`.

### O que o guard prova — e o que não prova

Ele prova que a evidência de promoção **não repousa em `success()` implícito**:
um step sem `if:` roda por herança dos anteriores, e ninguém declarou sob qual
resultado ele deveria existir. Foi essa a forma que a primeira versão do guard
deixava passar.

Ele **não** prova que o step referenciado no `if:` seja de fato
`build_snapshot_entry()` + `verify_snapshot_files()`. Verificar isso exigiria um
parser semântico de expressões do Actions e um modelo do que cada step faz —
desproporcional ao problema. A regra é deliberadamente conservadora: exige que
alguém tenha **declarado explicitamente** a condição, e deixa a correção dessa
condição para o review humano.

O escopo restrito ao domínio de publicação também é intencional. `final` e
`published` aparecem legitimamente em artifacts sem relação com o manifesto — um
guard que reprovasse `final-transform-metrics` estaria proibindo vocabulário que
nunca teve a ver com o bug.

Evidência de estado (1) sob `always()` continua sendo desejável — é justamente
quando o pipeline falha que `transform_metrics.json` mais importa. As regras não
a atingem.

## Consequências

- Nome de artifact/job deixa de ser aceitável como prova de publicação, e passa
  a ser verificável: ou o nome declara não-promoção, ou o gate garante a
  promoção.
- Workflows operacionais ad hoc (como os usados em backfills) passam pelo mesmo
  guard, que é onde o defeito original nasceu — o workflow permanente
  `etl-monthly.yml` nunca publicou artifact de manifesto.
- O guard é estático e barato; não valida o pipeline nem duplica lógica de
  negócio.

## Alternativa considerada e adiada

Emitir um `publication_state.json` por estágio concluído, tornando o estado
legível sem arqueologia de log. É a solução mais completa, mas exige que o
pipeline saiba reportar seu próprio progresso — nova infraestrutura, não
renomeação. Fica com #132, junto com checkpoint/resume, que precisa da mesma
noção de estágio concluído.

## Não altera

`build_snapshot_entry()`, `verify_snapshot_files()` e os gates de integridade
permanecem exatamente como estão. Esta ADR trata de como o resultado é
**nomeado**, não de como é **validado**.
