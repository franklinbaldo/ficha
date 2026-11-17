# Ficha P3-A — protótipo isolado SSG Rust

Isto **não é a busca real**. Demonstra shell HTML/CSS, base `/ficha/`
e um adaptador de consulta JS substituível. Nenhum dado é baixado.
O frontend de produção permanece em `web/`.

Fontes: `content/index.md`, `templates/{template,index,page,post}.html`
(StaticWeaver), `templates/tera/*.html` (MiniJinja),
`static/css/style.css`, `static/js/*.mjs`.

**Versão executada: `ssg 0.0.76`** no Ifro (Linux GNU x86_64); release SHA-256 `a0ed1f8cde9b8b00d93f9a4f31bd78a803cc43f4370872cd3909835e32ad4358` validado contra GitHub, sidecar e arquivo baixado.
A série 0.0.x pode alterar comportamento entre patch releases. O binário
precisa vir de release verificável (checksum/assinatura) ou `cargo install
ssg --version 0.0.76 --locked` em executor autorizado, não de instalador
`latest` nem de contêiner sem digest.

Em executor autorizado com SSG e Node instalados:

```sh
cd experiments/ssg-p3a
ssg --version  # deve ser 0.0.76 antes de prosseguir
ssg build -f config.toml --jobs 2 --max-memory 128
node scripts/finalize-output.mjs  # apenas PoC; isola widget e manifesto do SSG
node scripts/materialize-assets.mjs
node --test tests/*.test.mjs
node tests/smoke-built.mjs dist/ficha
```

Saída declarada: `dist/ficha/`. O preview deve servir `dist/` como raiz
e abrir `http://127.0.0.1:8000/ficha/`. O smoke pós-build **falha**
se o SSG não emitir `index.html`, CSS, os módulos JS, o formulário, a
live region e os caminhos `/ficha/`, ou se deixar tags de template cruas.
O validador não cria assets. `scripts/materialize-assets.mjs` só copia CSS e dois módulos de `static/` após o SSG produzir HTML; falha sem HTML, sem fonte ou com symlink e não sobrescreve o manifesto. Não é prova de WCAG nem da busca real.
Os testes `smoke-built.test.mjs` usam exclusivamente fixtures sintéticas;
nenhum PASS de fixture deve ser contado como build SSG aprovado.

**Evidência de build real (Ifro, `fdc80a732bd239a3797ab66a31b03a4dda57a190`):** o gerador
verificado emitiu `dist/ficha/index.html` e retornou código 0.
Contudo, **não copiou** `static/css` nem `static/js`: o smoke
falhou por CSS ausente. A cópia de apenas esses três assets na
demonstração isolada fez o smoke passar. O comando de cópia está agora
versionado com regressões positivas e negativas.

**Pendências reais do SSG:** apesar de exit 0, o log mostrou
`Error generating manifest: Invalid manifest name: Name cannot be empty`
e quality gate **8/10**. O gerador também injetou um widget genérico
de busca em inglês. O `manifest.json` da PoC é um artefato do SSG,
**não** o contrato de snapshots RFB. Esses pontos impedem tratar o
protótipo como apto a substituir o site publicado.

Em cada commit, repetir o build real, finalização estrita, cópia de assets,
smoke HTTP e testes e registrar SHA. Ainda faltam navegador/axe,
DuckDB/Protobuf, paridade e produção (NOT_RUN). Nenhum comando de deploy.

O adaptador padrão retorna *indisponível na demonstração*. Os testes
Node cobrem normalização alfanumérica, entrada vazia, distinção entre
ausência/erro e resposta obsoleta. Não comprovam navegador, rede,
DuckDB, Protobuf, parser do SSG nem WCAG. Esses gates são **NOT_RUN**
até prova em executor. Sem comandos de deploy.

## Checkpoint: isolamento de efeitos do SSG 0.0.76

A [pipeline upstream v0.0.76](https://github.com/sebastienrousseau/static-site-generator/blob/v0.0.76/src/core/pipeline.rs)
registra SearchPlugin incondicionalmente. Na saída observada, ele injeta uma
segunda busca genérica em inglês, incompatível com a semântica da busca CNPJ.
A versão também gera um manifest.json de aplicativo (name=Static Site,
start_url=/), diferente do contrato RFB (current + snapshots), e o
compilador registra Invalid manifest name: Name cannot be empty apesar de
retornar zero. O aviso do compilador persiste, não foi resolvido pela PoC.

O script scripts/finalize-output.mjs é uma contenção experimental e versionada:
confere a estrutura exata do widget e de seus CSS/JS, retira sua referência
do HTML e isola o manifesto sintético como ssg-generated-webmanifest.json.
Recusa nomes/estrutura desconhecidos e nunca toca em manifestos contendo
current ou snapshots. Não se deve reutilizar o script para produção sem
contrato formal com o gerador e revisão da estratégia do manifesto.
A página de teste usa uma URL absoluta para o contrato do site publicado
(https://franklinbaldo.github.io/ficha/manifest.json), não um manifesto
de dados local fictício. CSS/JS e navegação continuam sob /ficha/.

O template MiniJinja inclui CSP de demonstração connect-src 'none'.
Essa política impede consultas remotas de propósito, pois o adaptador é mock;
para produção haverá política revisada para IA/CORS/WASM, com verificação
de CSP/SRI e sem unsafe-inline. O quality gate nativo passou 9/10 em
build isolado do SSG 0.0.76; o único pilar reprovado exige texto promocional
Made with SSG no rodapé, que não deve ser inserido só para maquiar o gate.
O resultado 9/10 não significa que testes de navegador/WCAG passaram.

Build HTTP local e node --test são obrigatórios antes de corte.
Ainda faltam: testes reais de teclado, axe, leitor de tela, dados
DuckDB/Protobuf, paridade, proteção da publicação e deploy autorizado.

## Ensaio de navegador opcional (somente preview local)

Após gerar e finalizar `dist/ficha/`, testar a jornada mock sem dados reais
em Chromium com Playwright instalado em executor autorizado:

```sh
FICHA_PLAYWRIGHT_MODULE=/caminho/verificado/node_modules/playwright \
  node tests/browser-built.mjs dist
```

O script falha se não houver build ou Chromium; **não transforma
dependência ausente em PASS**. Valida o foco inicial (skip link), idioma,
live region, ausência de rolagem horizontal em 320/375/1440 px, CNPJ
alfanumérico passando pelo formulário, erros de entrada, reset na troca
de modalidade, ausência de consulta de rede e ausência de exceções JS.
Este ensaio **não** equivale a auditoria axe/WCAG, leitor de tela, dados
reais, desempenho WASM/Protobuf ou autorização de deploy.


## Checkpoint P3-A: auditoria axe real, somente no protótipo

O script `tests/axe-built.mjs` usa **Chromium + axe-core 4.10.3** em
preview **loopback** do resultado SSG já construído. Dependências ausentes
ou build ausente **falham**, não são convertidos em aprovação. A biblioteca
axe é servida somente em `/ficha/__audit__/axe.js` no servidor local do
ensaio, nunca incorporada a `dist/` nem ao GitHub Pages. O navegador não
pode acessar IA, RFB ou qualquer origem remota.

```sh
# Instalar/inspecionar as dependências em pasta temporária, fora do frontend:
FICHA_PLAYWRIGHT_MODULE=/caminho/versionado/node_modules/playwright \
FICHA_AXE_MODULE=/caminho/versionado/node_modules/axe-core \
node tests/axe-built.mjs dist
```

**Evidência Ifro do commit `d24883758fae595cf20fc7bc29ed95a3cb4ccbed`:**
`ssg 0.0.76` (tarball SHA-256 do README), build e finalização em
`dist/ficha/`, materialização de 3 assets, `smoke-built` PASS,
`node --test` **27/27 PASS**, navegador legado **3/3 viewports PASS**.
A nova auditoria com **axe-core 4.10.3 + Chromium** cobriu
**320/375/1440 px × 4 estados** (inicial, inválido, indisponível,
troca de modalidade): **12/12 estados sem violações** das regras
automatizadas WCAG 2.0/2.1 A/AA escolhidas, `incomplete=[]` neste
ensaio. Controle negativo reproduzível em cópia isolada:
trocar `for="search-term"` por `for="missing-term"` fez o teste
**falhar com exit 1**, regra axe `label` de impacto crítico, no
primeiro estado/viewport. Essa contraprova não modifica arquivos do repo.

**Limite estrito:** axe não substitui leitor de tela, julgamento humano,
testes de dados verdadeiros ou matriz completa WCAG. Todos esses gates
continuam `NOT_RUN`. O SSG ainda registra erro de manifest em log
e quality gate 9/10 apesar de gerar HTML; portanto **PR DRAFT/HOLD**,
sem merge/deploy. Não existe execução do axe em Actions nessa PoC:
somente ensaio Ifro sobre HEAD conferido.
