# Ficha P3-A — protótipo isolado SSG Rust

Isto **não é a busca real**. Demonstra shell HTML/CSS, base `/ficha/`
e um adaptador de consulta JS substituível. Nenhum dado é baixado.
O frontend de produção permanece em `web/`.

Fontes: `content/index.md`, `templates/{template,index,page,post}.html`
(StaticWeaver), `templates/tera/*.html` (MiniJinja),
`static/css/style.css`, `static/js/*.mjs`.

**Versão-alvo do SSG: `0.0.76` (pin de especificação, build ainda NÃO executado).**
A série 0.0.x pode alterar comportamento entre patch releases. O binário
precisa vir de release verificável (checksum/assinatura) ou `cargo install
ssg --version 0.0.76 --locked` em executor autorizado, não de instalador
`latest` nem de contêiner sem digest.

Em executor autorizado com SSG e Node instalados:

```sh
cd experiments/ssg-p3a
ssg --version  # deve ser 0.0.76 antes de prosseguir
ssg -f config.toml
node --test tests/consult.test.mjs tests/smoke-built.test.mjs
node tests/smoke-built.mjs dist/ficha
```

Saída declarada: `dist/ficha/`. O preview deve servir `dist/` como raiz
e abrir `http://127.0.0.1:8000/ficha/`. O smoke pós-build **falha**
se o SSG não emitir `index.html`, CSS, os módulos JS, o formulário, a
live region e os caminhos `/ficha/`, ou se deixar tags de template cruas.
Ele não cria assets, nem consulta a rede e **não é prova de WCAG/SSG por si**.
Os testes `smoke-built.test.mjs` usam exclusivamente fixtures sintéticas;
nenhum PASS de fixture deve ser contado como build SSG aprovado.

**Estado no HEAD inicial da PoC:** `ssg build=NOT_RUN`, navegação,
acessibilidade real e cópia de assets `NOT_VERIFIED`. Só atribuir PASS
quando o comando real for executado no SHA exato, com logs e evidência
de output. Sem comandos de deploy.

O adaptador padrão retorna *indisponível na demonstração*. Os testes
Node cobrem normalização alfanumérica, entrada vazia, distinção entre
ausência/erro e resposta obsoleta. Não comprovam navegador, rede,
DuckDB, Protobuf, parser do SSG nem WCAG. Esses gates são **NOT_RUN**
até prova em executor. Sem comandos de deploy.
