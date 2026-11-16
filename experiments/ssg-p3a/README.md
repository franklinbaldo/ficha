# Ficha P3-A — protótipo isolado SSG Rust

Isto **não é a busca real**. Demonstra shell HTML/CSS, base `/ficha/`
e um adaptador de consulta JS substituível. Nenhum dado é baixado.
O frontend de produção permanece em `web/`.

Fontes: `content/index.md`, `templates/{template,index,page,post}.html`
(StaticWeaver), `templates/tera/*.html` (MiniJinja),
`static/css/style.css`, `static/js/*.mjs`.

Em executor autorizado com `ssg` de versão fixada e Node instalados:

```sh
cd experiments/ssg-p3a
ssg -f config.toml
node --test tests/consult.test.mjs
```

Saída configurada: `dist/ficha/`. O preview correto serve `dist/`
como raiz e visita `http://127.0.0.1:8000/ficha/`.
Confirmar assets copiados pelo SSG, HTML emitido e links `/ficha/`.
A versão executora ainda precisa ser pinada e verificada antes de CI.

O adaptador padrão retorna *indisponível na demonstração*. Os testes
Node cobrem normalização alfanumérica, entrada vazia, distinção entre
ausência/erro e resposta obsoleta. Não comprovam navegador, rede,
DuckDB, Protobuf, parser do SSG nem WCAG. Esses gates são **NOT_RUN**
até prova em executor. Sem comandos de deploy.
