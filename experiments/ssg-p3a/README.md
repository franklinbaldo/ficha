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
ssg build -f config.toml
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

No HEAD que acrescenta o script de cópia, o build completo precisa ser
reexecutado e ligado a SHA, logs, assets e testes. Navegador/axe,
DuckDB/Protobuf, paridade e produção ainda `NOT_RUN`.
Nenhum comando de deploy.

O adaptador padrão retorna *indisponível na demonstração*. Os testes
Node cobrem normalização alfanumérica, entrada vazia, distinção entre
ausência/erro e resposta obsoleta. Não comprovam navegador, rede,
DuckDB, Protobuf, parser do SSG nem WCAG. Esses gates são **NOT_RUN**
até prova em executor. Sem comandos de deploy.
