# FICHA

**F**ichário de **I**dentificação de **C**NPJ **H**ospedado no **A**rchive

O **FICHA** é um fichário aberto das empresas brasileiras e um Data Lakehouse *serverless* para os dados públicos de CNPJ da Receita Federal. Os snapshots mensais são preservados no Internet Archive e podem ser usados tanto pela interface pública quanto diretamente como dados abertos.

## Usar agora

- **Consultar no navegador:** https://franklinbaldo.github.io/ficha/
- **Inspecionar o contrato público do snapshot vigente:** https://franklinbaldo.github.io/ficha/manifest.json

A interface atual consulta os Parquets no navegador com DuckDB-WASM e permite buscar por **empresa**, **pessoa/sócio**, **endereço** e **CNAE** quando os respectivos artefatos estão disponíveis no snapshot. A própria página mostra a competência em uso, origem, preservação e estado de verificação do conjunto publicado.

O `manifest.json` é a porta para reutilização fora do site: ele declara a competência vigente, snapshots publicados e URLs dos artefatos preservados, com identidade verificável quando disponível. Assim, quem precisa analisar a base inteira não depende da interface do Ficha.

---

## 🏗️ Arquitetura Híbrida

O projeto opera em duas camadas de acesso para máxima eficiência:

### 1. Camada Atômica (Key-Value Estático)

- **Formato:** shards `companies-XX.zip` por snapshot, contendo arquivos `{cnpj_base}.pb` (protobuf) individualizados por raiz de CNPJ. Snapshots históricos também podem usar o antigo `companies.zip` monolítico.
- **Tecnologia:** explora a funcionalidade de "unzip" transparente do Internet Archive.
- **Uso:** consultas diretas por raiz de CNPJ por URL estática previsível.
- **Vantagem:** acesso pontual sem precisar varrer a base analítica inteira; protobuf é compacto.
- **Status:** implementado e publicado como parte do contrato de snapshots. O reader atômico existe no frontend, mas a busca pública atual usa a camada analítica descrita abaixo.

### 2. Camada Analítica (Data Lakehouse)

- **Formato:** Apache Parquet, com relações para CNPJs e conjuntos complementares publicados por snapshot.
- **Tecnologia:** DuckDB-WASM no navegador.
- **Uso:** consulta pública por empresa, pessoa, endereço e CNAE, além de análises e cruzamentos externos sobre os Parquets.
- **Vantagem:** permite explorar e reutilizar dados sem backend próprio do Ficha.

## 🛠️ Stack Tecnológica

- **Frontend:** Astro + Svelte 5 + TypeScript + Zod.
- **Motor de Dados:** DuckDB-WASM.
- **Storage/preservação:** Internet Archive.
- **Formatos públicos:** Parquet, protobuf/ZIP e `manifest.json`.
- **ETL:** Python + DuckDB + GitHub Actions.

## 📂 Estrutura do Repositório

```text
ficha/
├── web/             # Frontend Astro (deploy → GitHub Pages)
├── etl/             # Pipeline Python (RFB → artefatos → Internet Archive)
├── experiments/     # PoCs e benchmarks numerados
├── docs/            # Documentação técnica, ADRs e RFCs
└── .github/         # Workflows de CI, deploy e publicação
```

`web/` e `etl/` são projetos auto-contidos. O contrato público entre produção e consumo é materializado pelo manifesto e pelos schemas/artefatos publicados; o frontend valida e consulta esse contrato sem introduzir um backend de aplicação.

## 🚀 Desenvolvimento

```bash
# Frontend
cd web
bun install
bun dev

# ETL
cd etl
uv venv && uv pip install -e ".[dev]"
ficha-etl run --month 2026-01
```

## O conceito "Ficha"

Inspirado nos antigos fichários de metal, cada empresa pode ser lida como uma ficha vinculada a uma competência específica. A série de snapshots preserva estados cadastrais ao longo do tempo e mantém os dados disponíveis como infraestrutura aberta, não apenas como resultado de uma busca no site.

---

*Transformando dados abertos em infraestrutura aberta.*
