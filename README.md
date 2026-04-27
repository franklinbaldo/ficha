# FICHA

**F**ichário de **I**dentificação de **C**NPJ **H**ospedado no **A**rchive

---

O **FICHA** é um Data Lakehouse *serverless* e atômico para dados do CNPJ da Receita Federal. Ele utiliza o Internet Archive (IA) não apenas como backup, mas como infraestrutura ativa de dados.

## 🏗️ Arquitetura Híbrida

O projeto opera em duas camadas de acesso para máxima eficiência:

### 1. Camada Atômica (Key-Value Estático)
*   **Formato:** Milhões de arquivos `{cnpj}.json` individuais, compactados em ZIPs por lote/estado.
*   **Tecnologia:** Explora a funcionalidade de "unzip" transparente do Internet Archive.
*   **Uso:** Consultas diretas por CNPJ. O frontend busca a "ficha" da empresa via uma URL estática previsível.
*   **Vantagem:** Custo zero de processamento no cliente; ideal para ferramentas de autocompletar e visualização rápida.

### 2. Camada Analítica (Data Lakehouse)
*   **Formato:** Apache Parquet otimizado e particionado.
*   **Tecnologia:** DuckDB-WASM no navegador.
*   **Uso:** Consultas complexas, filtros por CNAE, UF, Capital Social e análise de grafos de relacionamento (sócios).
*   **Vantagem:** Permite cruzamentos de dados avançados sem necessidade de um backend.

## 🛠️ Stack Tecnológica

*   **Frontend:** Astro + Svelte 5 + TypeScript + Zod.
*   **Motor de Dados:** DuckDB-WASM.
*   **Storage:** Internet Archive (arquivos .parquet e .zip/.json).
*   **ETL:** Python + DuckDB CLI + GitHub Actions.

## 📂 Estrutura do Repositório

```
ficha/
├── web/             # Frontend Astro (deploy → GitHub Pages)
├── etl/             # Pipeline Python (RFB → Parquet → Internet Archive)
├── experiments/     # PoCs e benchmarks numerados
├── docs/            # Documentação técnica e ADRs (futuro)
└── .github/         # Workflows de CI/deploy/cron (futuro)
```

`web/` e `etl/` são projetos auto-contidos com sua própria build, deps e config. Único contrato entre eles: o schema do Parquet declarado em `web/src/schemas/vN/`.

## 🚀 Desenvolvimento

```bash
# Frontend
cd web
bun install
bun dev

# ETL (futuro)
cd etl
uv venv && uv pip install -e ".[dev]"
ficha-etl run --month 2026-01
```

## 🚀 O Conceito "Ficha"

Inspirado nos antigos fichários de metal (Rolodex), cada empresa tem sua "lâmina" digital individualizada e imutável no tempo, permitindo o rastreamento histórico de alterações cadastrais através dos snapshots mensais.

---

*Transformando Dados Abertos em Infraestrutura Aberta.*
