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

## Ensaio de recuperação offline (17/11/2026)

O gate P3-A executa `node scripts/rollback-drill.mjs dist/ficha` **depois do build SSG real**. O teste reúne o candidato SSG e um **artefato anterior sintético e explicitamente rotulado**, copia ambos para uma área temporária, confere inventários SHA-256 de paths e bytes, ativa o candidato **apenas na área de ensaio**, injeta uma falha após a troca e restaura os bytes do artefato anterior. A limpeza é local, sem Pages, ETL ou IA.

Controles negativos: hash anterior errado, adulteração do staging, manifesto de aplicativo colidindo com `manifest.json` de dados, symlink e diretórios sobrepostos. O teste nunca afirma ter recuperado um deploy Astro real. Para ensaio posterior com artefato Astro **genuíno** e hash obtido independentemente, fornecer `node scripts/rollback-drill.mjs dist/ficha /caminho/artefato-anterior` e `FICHA_PREVIOUS_TREE_SHA256`; sem hash a execução falha.

**Bloqueios antes do corte:** preservar e identificar o artefato real do Pages e o workflow anterior, executar restauração em ambiente homologado autorizado, verificar paridade e ensaiar rollback de workflow e publicação. O exercício atual demonstra somente reversibilidade de arquivos em sandbox; **não** demonstra reversão de GitHub Pages, DNS, cache/CDN, compatibilidade de dados nem a navegação antiga.

## Preservação do contrato de dados no ensaio offline (17/11/2026)

A PoC SSG não contém `manifest.json` da RFB: seu manifesto PWA foi
quarentenado. Substituir o diretório inteiro apagaria o contrato público.
O ensaio exige `current` e `snapshots` no artefato anterior, confere o
SHA-256 independente do inventário, recusa manifesto RFB divergente no
candidato e preserva os bytes exatos **apenas no slot temporário**. São
registrados hashes separados para SSG bruto, candidato montado e manifesto.
Controles negativos exercitam ausência, formato inválido e divergência.

Isso não altera `web/`, o manifesto produtivo, URLs ou snapshots. Não
prova paridade dos demais assets, rollback do Pages ou restauração Astro
real: os artefatos genuínos, hashes externos e autorização operacional
continuam necessários antes de qualquer corte.
