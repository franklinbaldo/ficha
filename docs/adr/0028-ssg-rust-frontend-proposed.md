# ADR 0028 — Frontend Ficha com SSG Rust

**Status:** Proposed (direção autorizada pelo proprietário; corte pendente)
**Data:** 2026-11-16
**Owner:** 🦇 Morcego · P3-A · Ficha #247

## Contexto

O shell usa Astro 6 e Svelte 5, mas os dados são distribuídos por contratos
Parquet, Protobuf/ZIP e `manifest.json` publicados no Internet Archive.
O proprietário determinou trocar o gerador de páginas por `ssg` em Rust,
sem alterar o pipeline de dados para resolver complexidade da interface.

## Decisão

1. O **SSG Rust** gera HTML estático com MiniJinja e CSS nativo, inclusive
   navegação, formulário, ficha e páginas institucionais. Sem framework SPA.
2. O navegador executa apenas módulos JavaScript ES necessários à interação.
   Um adaptador separado poderá usar o leitor Protobuf para CNPJ pontual
   e DuckDB-WASM em Worker para pesquisas Pessoa, Endereço e CNAE. A PoC
   não faz essas consultas reais. Não gerar milhões de páginas HTML.
3. Preservar contratos, ETL Python/DuckDB, `manifest.json`, snapshot,
   checksums, URLs e Internet Archive. CNPJ alfanumérico permanece string,
   inclusive durante validação de dígito verificador em futura integração.
4. Prefixo `/ficha/` explícito para links, assets e manifesto. O protótipo
   vive em `experiments/ssg-p3a/`, fora de `web/` e dos workflows Pages.
5. O gerador não dispensa testes de acessibilidade, CORS/Range, compatibilidade
   com v1/v1.1 nem comparação de conteúdo entre versões.

**Supersessão:** esta proposta substituirá a ADR 0001 (Accepted: Astro +
Svelte) somente após decisão formal de corte. Preservar ADR 0001 no histórico.

## Gates de paridade antes do corte

- Formulário submetido ≠ termo em digitação; modalidade deve ser estável.
  Limpar resultado anterior e invalidar requests atrasadas.
- Busca por CNPJ numérico e alfanumérico, DV correto/incorreto, Empresa,
  Pessoa, Endereço, CNAE; ficha, sócios, filiais e contatos.
- Estados separados: não consultado, carregando, não encontrado confirmado,
  HTTP/CORS/Range indisponível, timeout e erro. Nunca exibir ausência
  cadastral quando a rede falhar.
- Acessibilidade real: 320/375/1440px, teclado, foco, leitor de tela,
  resultados e erros em live region, contraste e zoom 200%.
- URLs e assets sob `/ficha/`, fonte/competência/hashes corretos,
  desempenho medido (tempo, bytes, WASM, memória), testes negativos e
  CI completo no SHA exato.
- Shadow build e comparação do site anterior com o novo, sem promover
  manifest nem rodar ETL. Só depois trocar deploy por autorização separada.

## Rollback

Guardar artifact/commit e workflow Pages anteriores, ainda funcionais.
Falhando qualquer gate após autorização de corte, restaurar artifact e
workflow da versão anterior, sem tocar nas competências nem no IA.
Ensaiar rollback antes do primeiro deploy migrado.

## Limites desta PoC

Somente novos arquivos de ADR/protótipo. Não editar `web/`, componentes
da Raposa, ETL, workflows, manifesto ou ADR histórica. Não executar
produção, deploy, publicação de snapshots, download de base ou gasto.
Se build SSG ou browser não foi executado, registrar **NOT_RUN**.
