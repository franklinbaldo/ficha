# FICHA

**F**ichário de **I**dentificação de **C**NPJ **H**ospedado no **A**rchive

O **FICHA** é um fichário aberto das empresas brasileiras e um Data Lakehouse *serverless* para os dados públicos de CNPJ da Receita Federal. Os snapshots mensais são preservados no Internet Archive e podem ser usados tanto pela interface pública quanto diretamente como dados abertos.

## Usar agora

- **Abrir o site público (home SSG com formulário experimental):** https://franklinbaldo.github.io/ficha/. A presença do formulário não comprova acesso aos dados remotos.
- **Inspecionar o contrato público do snapshot vigente:** https://franklinbaldo.github.io/ficha/manifest.json

**Manifesto ≠ disponibilidade live.** O JSON versionado declara competências, URLs e identidades dos artefatos; isso **não comprova** que as URLs do Internet Archive estejam acessíveis, que a busca no navegador funcione ou que o mês mais recente da RFB já esteja publicado. No corte de **16/11/2026** (`main@98329ff`), `manifest.current` declara **2026-07**, não 2026-08. A investigação e a recuperação de agosto estão na [issue #247](https://github.com/franklinbaldo/ficha/issues/247). A disponibilidade exige conferência independente de URLs, dados e interface.

**Site público atual (18/11/2026):** home e Sobre geradas com SSG Rust. A [PR #297](https://github.com/franklinbaldo/ficha/pull/297) incorporou à fonte `site/` um **formulário experimental** para CNPJ numérico/alfanumérico ou razão social e módulos JavaScript derivados do shadow da [#276](https://github.com/franklinbaldo/ficha/pull/276). O diretório duplicado `site-ssg/` foi removido; `web/` conserva o protótipo Astro/Svelte, não publicado. Pesquisas por pessoa/sócio, endereço e CNAE não foram integradas à home SSG.

Depois da correção do smoke na [PR #298](https://github.com/franklinbaldo/ficha/pull/298), o [deploy pós-merge #44347752600](https://github.com/franklinbaldo/ficha/actions/runs/44347752600) concluiu **build, deploy e verify com sucesso**. Esse resultado comprova a entrega e os checks HTTP previstos pelo workflow, **não** consultas CNPJ funcionais. A carga real de DuckDB-WASM/Worker, CORS+Range sobre os Parquets remotos, identidade dos dados e paridade ainda carecem de prova independente ([issue #208](https://github.com/franklinbaldo/ficha/issues/208)). **Não anunciar resultados de busca como disponíveis até essa comprovação.**

O `manifest.json` é a porta para reutilização fora do site: ele declara a competência vigente, snapshots publicados e URLs dos artefatos preservados, com identidade verificável quando disponível. Assim, quem precisa analisar a base inteira não depende da interface do Ficha.

---

## 🏗️ Arquitetura Híbrida

O projeto opera em duas camadas de acesso para máxima eficiência:

### 1. Camada Atômica (Key-Value Estático)

- **Formato:** shards `companies-XX.zip` por snapshot, contendo arquivos `{cnpj_base}.pb` (protobuf) individualizados por raiz de CNPJ. Snapshots históricos também podem usar o antigo `companies.zip` monolítico.
- **Tecnologia:** explora a funcionalidade de "unzip" transparente do Internet Archive.
- **Uso:** consultas diretas por raiz de CNPJ por URL estática previsível.
- **Vantagem:** acesso pontual sem precisar varrer a base analítica inteira; protobuf é compacto.
- **Status:** o reader atômico existe no código, e o manifesto versionado declara artefatos atômicos em alguns snapshots. A declaração não prova acesso HTTP live; a home pública atual não usa leitor atômico nem oferece pesquisa. Não pressupor integração do reader atômico à jornada de busca.

### 2. Camada Analítica (Data Lakehouse)

- **Formato:** Apache Parquet, com relações para CNPJs e conjuntos complementares publicados por snapshot.
- **Tecnologia:** DuckDB-WASM (adaptador experimental incluído em `site/`; consulta remota ainda não comprovada).
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
├── site/            # Site público SSG Rust e módulos experimentais de busca
├── web/             # Protótipo Astro/Svelte de busca (não publicado)
├── etl/             # Pipeline Python (RFB → artefatos → Internet Archive)
├── experiments/     # PoCs e benchmarks numerados
├── docs/            # Documentação técnica, ADRs e RFCs
└── .github/         # Workflows de CI, deploy e publicação
```

`site/` é a home pública estática. `web/` contém a interface Astro preservada como protótipo; `etl/` produz os artefatos. O contrato entre produção e consumo é o manifesto e os schemas/artefatos publicados, mas sua presença não comprova a acessibilidade remota dos arquivos.

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
