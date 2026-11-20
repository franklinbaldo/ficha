# FICHA

**F**ichário de **I**dentificação de **C**NPJ **H**ospedado no **A**rchive

O **FICHA** é um fichário aberto das empresas brasileiras e um Data Lakehouse *serverless* para os dados públicos de CNPJ da Receita Federal. Os snapshots mensais são preservados no Internet Archive e podem ser usados tanto pela interface pública quanto diretamente como dados abertos.

## Usar agora

- **Abrir o site público (SSG Rust):** https://franklinbaldo.github.io/ficha/. Na home, CNPJ numérico completo com DV válido abre a ficha Protobuf v1; DV inválido gera erro local antes de consulta ou navegação. Busca por nome e CNPJ alfanumérico continua experimental via Parquet/DuckDB.
- **Consultar um CNPJ numérico completo:** https://franklinbaldo.github.io/ficha/empresa/ (leitura Protobuf v1, independente do Parquet).
- **Comparar duas competências para um CNPJ numérico:** https://franklinbaldo.github.io/ficha/historia/ (somente competências com Protobuf v1; sem fallback automático).
- **Inspecionar o contrato público do snapshot vigente:** https://franklinbaldo.github.io/ficha/manifest.json

**Manifesto ≠ disponibilidade live.** O JSON versionado declara competências, URLs e identidades dos artefatos; isso **não comprova** que as URLs do Internet Archive estejam acessíveis, que a busca no navegador funcione ou que o mês mais recente da RFB já esteja publicado. No corte de **16/11/2026** (`main@98329ff`), `manifest.current` declara **2026-07**, não 2026-08. A investigação e a recuperação de agosto estão na [issue #247](https://github.com/franklinbaldo/ficha/issues/247). A disponibilidade exige conferência independente de URLs, dados e interface.

**Site público atual (20/11/2026):** o Pages usa **SSG Rust em `site/`**. Na home, um CNPJ numérico completo com DV válido é encaminhado à ficha Protobuf sem iniciar Parquet/DuckDB-WASM; um número com DV inválido é recusado antes de fetch, WASM ou navegação. Busca por nome e CNPJ alfanumérico ainda usam a rota analítica legada, cujo transporte remoto apresentou erro de CORS em teste de 19/11/2026. Uma falha de transporte **não** significa que o CNPJ não exista no snapshot. `web/` guarda o protótipo Astro/Svelte, não publicado.

A **ficha por CNPJ numérico completo** em `/ficha/empresa/` usa o leitor de **Protobuf v1** sem Parquet. A página `/ficha/historia/` compara duas competências que declaram essa camada para CNPJ numérico, também sem Parquet. CNPJ alfanumérico, competência sem Protobuf compatível e busca por nome **não** têm alternativa automática por Protobuf v1. Estados como arquivo indisponível, resposta inválida e ausência dentro de um snapshot íntegro precisam permanecer distintos.

**Evidência e limite:** o [Chromium na #367](https://github.com/franklinbaldo/ficha/actions/runs/44586268099) consultou Nubank e Petrobras com dados Protobuf; a [#208](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-7019004782) registra prova adicional no navegador após a publicação da rota numérica. O [Pages pós-merge da #387](https://github.com/franklinbaldo/ficha/actions/runs/44665995643) executou build, deploy e verificação HTTP, **não** um novo E2E de consulta nem testes de zoom nativo/leitor de tela. Isso não atesta os 300 ZIPs, disponibilidade contínua, versão de wire declarada em cada shard ou completude do acervo; [#383](https://github.com/franklinbaldo/ficha/issues/383) acompanha essa fronteira de custódia.

O `manifest.json` é a porta para reutilização fora do site: ele declara a competência vigente, snapshots publicados e URLs dos artefatos preservados, com identidade verificável quando disponível. Assim, quem precisa analisar a base inteira não depende da interface do Ficha.

---

## 🏗️ Arquitetura Híbrida

O projeto opera em duas camadas de acesso para máxima eficiência:

### 1. Camada Atômica (Key-Value Estático)

- **Formato:** shards `companies-XX.zip` por snapshot, contendo arquivos `{cnpj_base}.pb` (protobuf) individualizados por raiz de CNPJ. Snapshots históricos também podem usar o antigo `companies.zip` monolítico.
- **Tecnologia:** explora a funcionalidade de "unzip" transparente do Internet Archive.
- **Uso:** consultas diretas por raiz de CNPJ por URL estática previsível.
- **Vantagem:** acesso pontual sem precisar varrer a base analítica inteira; protobuf é compacto.
- **Status:** o leitor atômico está integrado à **ficha numérica em `/ficha/empresa/`** e à comparação numérica em `/ficha/historia/`, com provas específicas em Chromium. Na **home**, CNPJ numérico completo com DV válido é encaminhado à ficha Protobuf v1, sem consulta Parquet/DuckDB; DV inválido é recusado localmente. Nome e CNPJ alfanumérico permanecem na rota analítica experimental. O manifesto declara artefatos, mas não atesta por si só disponibilidade HTTP nem a versão de wire dos ZIPs ([#208](https://github.com/franklinbaldo/ficha/issues/208), [#383](https://github.com/franklinbaldo/ficha/issues/383)).

### 2. Camada Analítica (Data Lakehouse)

- **Formato:** Apache Parquet, com relações para CNPJs e conjuntos complementares publicados por snapshot.
- **Tecnologia:** DuckDB-WASM (adaptador experimental em `site/`; o teste de 19/11 observou CORS nos Parquets remotos). Não se confunde com a leitura Protobuf independente.
- **Uso:** análises externas dos Parquets e futura pesquisa por empresa, pessoa, endereço e CNAE.
- **Vantagem:** permite explorar e reutilizar dados sem backend próprio do Ficha.

## 🛠️ Stack Tecnológica

- **Site público:** SSG Rust (páginas estáticas), HTML e CSS.
- **Protótipo de busca não publicado:** Astro + Svelte 5 + TypeScript + Zod (`web/`).
- **Motor de Dados (busca experimental):** DuckDB-WASM.
- **Storage/preservação:** Internet Archive.
- **Formatos públicos:** Parquet, protobuf/ZIP e `manifest.json`.
- **ETL:** Python + DuckDB + GitHub Actions.

## 📂 Estrutura do Repositório

```text
ficha/
├── site/            # Site SSG Rust; ficha/história Protobuf e busca legada
├── web/             # Protótipo Astro/Svelte de busca (não publicado)
├── etl/             # Pipeline Python (RFB → artefatos → Internet Archive)
├── experiments/     # PoCs e benchmarks numerados
├── docs/            # Documentação técnica, ADRs e RFCs
└── .github/         # Workflows de CI, deploy e publicação
```

`site/` contém a home, ficha, história e catálogo públicos. `web/` contém a interface Astro preservada como protótipo; `etl/` produz os artefatos. O contrato entre produção e consumo é o manifesto e os schemas/artefatos publicados, mas sua presença não comprova a acessibilidade remota dos arquivos.

## 🚀 Desenvolvimento

```bash
# Protótipo legado de busca (não é o site publicado)
cd web
bun install
bun dev
```

Em **outro terminal**, a partir da raiz do repositório:

```bash
# ETL: instalar e consultar a ajuda sem processar snapshots
cd etl
uv venv && uv pip install -e ".[dev]"
uv run --no-sync ficha-etl --help
uv run --no-sync ficha-etl run --help
```

**Atenção:** `ficha-etl run --month YYYY-MM` **não é um comando de inspeção**. Com `IA_ACCESS_KEY` e `IA_SECRET_KEY`, a opção padrão (`skip_upload=False`) tenta enviar ZIPs RFB, Parquets e `companies.zip` ao Internet Archive e atualizar o manifesto **local**. Sem credenciais, o modo padrão recusa continuar. Mesmo com `--skip-upload`, ele ainda pode baixar/processar grandes volumes e escrever Parquets, ZIP local e o manifesto local: **não é teste barato nem somente leitura**. Consulte [etl/README.md](etl/README.md) antes de executá-lo.

O CLI `run` gera `companies.zip` monolítico; a [esteira mensal versionada](.github/workflows/etl-monthly.yml) usa `monthly_sharded`, 100 shards e etapas separadas de validação/promoção. Uma rota não valida a outra. Nenhum ETL real, backfill, upload ou promoção está autorizado por este quickstart.

## O conceito "Ficha"

Inspirado nos antigos fichários de metal, cada empresa pode ser lida como uma ficha vinculada a uma competência específica. A série de snapshots preserva estados cadastrais ao longo do tempo e mantém os dados disponíveis como infraestrutura aberta, não apenas como resultado de uma busca no site.

---

*Transformando dados abertos em infraestrutura aberta.*
