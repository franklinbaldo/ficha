---
type: RFC
title: FICHA — visão greenfield de produto público
status: Proposta aprovada em direção de produto; implementação incremental pendente
date: 2026-11-18
---

# FICHA — visão greenfield de produto público

> **Cada empresa tem uma história.** O FICHA permite consultar registros públicos de empresas brasileiras, conhecer sua proveniência e explorar como suas informações cadastrais aparecem em diferentes competências.

## 1. Decisão e problema

Esta RFC registra a visão de produto endossada pelo responsável em 18/11/2026. O *greenfield* aplica-se à **experiência pública e à arquitetura de interação**, não à reconstrução indiscriminada do ETL ou do acervo. A tecnologia serve ao usuário; Parquet, Protobuf, DuckDB e o Internet Archive não devem ser pré-requisitos cognitivos para pesquisar uma empresa.

O site público deixou de ser apenas uma vitrine do repositório. Precisa ser um produto utilizável, com duas faces complementares:
1. **Fichário público:** consulta de empresa e de suas informações por competência, simples para pessoas não técnicas.
2. **Acervo aberto:** acesso a snapshots, manifestos, formatos, origem e integridade, útil a pesquisadores e sistemas.

A homepage deve privilegiar a ação de pesquisar; não a explicação do stack. Nenhuma chamada à ação deve prometer busca comprovadamente funcional sem um teste end-to-end real.

## 2. Princípios não negociáveis

- **O usuário antes do mecanismo.** Na primeira dobra: o que é, o que consultar, e a competência dos dados.
- **Competência como contexto obrigatório.** Toda ficha, resultado, comparação e gráfico identifica a(s) competência(s) e a origem.
- **Tempo sem invenção.** Uma série histórica representa snapshots realmente disponíveis; diferença entre duas competências não prova a data exata do evento, nem continuidade entre observações.
- **Evidência antes da aparência.** Disponibilidade de manifesto, sucesso de deploy e presença de formulário não comprovam consulta real.
- **Uma consulta estreita não deve varrer gigabytes.** Preferir índice/registro atômico para CNPJ completo; consultas textuais e agregadas exigem estratégia compatível com navegador e rede.
- **Preservação desacoplada da entrega.** Internet Archive continua fonte arquivística. Um eventual endpoint de consulta (CORS/Range) é derivado e explicitamente distinguido, nunca substituição silenciosa da custódia.
- **Simplicidade radical.** SSG Rust, HTML semântico, CSS nativo e JavaScript modular; sem Astro/Svelte no novo site. Reutilização seletiva de algoritmos pequenos e comprovados, não migração de toda a árvore `web/`.
- **Acessibilidade e dispositivo móvel desde o primeiro incremento.** Teclado, foco, contraste, estados assíncronos, mensagens de erro e URLs compartilháveis.
- **Custo controlado.** GitHub Pages e dados públicos onde viável; nenhuma infraestrutura paga, espelhamento de gigabytes ou promoção de manifesto sem decisão específica.
- **Não esconder falhas.** Erro de origem remota, competência ausente e consulta indisponível são estados distintos, comunicados de modo compreensível.

## 3. Arquitetura de informação

### 3.1 Início — buscar

- Marca FICHA, frase curta, busca principal por CNPJ ou nome.
- Indicador discreto, mas visível, da competência efetivamente servida.
- Dois acessos secundários: **Explorar o acervo** e **Como funciona**.
- Exemplos reais somente quando cobertos por dados e consentidos pela política de apresentação. Nada de métricas, resultados ou parceiros inventados.
- Em falha de dados: não mostrar botão como funcional; ainda permitir acesso ao catálogo e explicar o estado.

### 3.2 Ficha da empresa

- Identidade cadastral: CNPJ, razão social, nome fantasia quando houver, situação, atividade, localidade, natureza jurídica, capital e opções tributárias quando disponíveis e contextualizadas.
- Seções de estabelecimentos e quadro societário conforme o escopo do snapshot e regras de privacidade.
- Identificação de competência, origem e acesso à evidência do arquivo/snapshot.
- URL própria, copiável, com identificador textual de CNPJ, inclusive alfanumérico.
- Não inferir vínculos econômicos, controle societário, sucessão, conduta ou atualidade além do que o cadastro sustenta.

### 3.3 História

- Linha do tempo baseada exclusivamente em competências presentes no acervo.
- Comparação entre duas competências com alterações observadas e campos sem dados claramente diferenciados.
- Sem fabricar eventos precisos ou supor que a alteração ocorreu na data da captura.

### 3.4 Explorar o Brasil

- Filtros de município/UF, CNAE, situação, período e outros campos disponíveis.
- Mapas, indicadores e séries apenas quando dados agregados, proveniência, cobertura e método estiverem explicitados.
- Renderizar progressivamente; não carregar motor analítico na home.

### 3.5 Acervo

- Catálogo de snapshots e estados de publicação, sem confundir competência apontada pelo manifesto com última competência disponibilizada pela Receita.
- Formatos, tamanhos, hashes, proveniência, links de custódia, contratos, documentação e possibilidades de reutilização.
- Separar *declarado no manifesto*, *URL acessível* e *consulta validada*.

## 4. Experiência visual

Linguagem de **arquivo público contemporâneo**: tipografia editorial legível, hierarquia por alinhamento e espaços, poucas cores, linhas discretas, tabelas claras, fichas densas quando necessário. Não simular papel amarelado nem transformar cada campo em cartão ornamental.

O nome **FICHA** é a marca; o acrônimo fica em Sobre. Priorizar conteúdos, datas e evidências. Competência deve ser perceptível, não nota de rodapé. Não produzir navegação para páginas ainda inexistentes.

A gramática Cobogó já examinada em #141 pode orientar faixa de proveniência, inscrição, vão e ritmo sem impor dependências pesadas de framework.

## 5. Arquitetura de entrega

| Camada | Direção |
| --- | --- |
| Páginas e conteúdo | SSG Rust com HTML semântico |
| Estilo | CSS nativo, responsivo |
| Interações | ES modules sob demanda |
| CNPJ completo | Índice estático particionado + registro atômico/derivado quando validado |
| Razão social | Índice textual derivado e particionado, estratégia avaliada por benchmark |
| Consulta agregada | DuckDB-WASM sob demanda ou materializações estáticas comprovadas |
| Custódia | Internet Archive e manifesto versionado |
| Publicação | GitHub Pages, sem backend obrigatório |
| Testes | Unitários, HTTP smoke e jornada real em navegador, separadamente |

A forma exata dos índices é decisão de engenharia a validar com cardinalidade, latência, tráfego, tamanho, compatibilidade dos contratos e custo. Esta RFC não determina arquitetura física prematuramente.

**Dependência crítica:** #208 registrou falha real de CORS em consultas DuckDB-WASM sobre Parquets arquivados. O MVP não pode depender de que um arquivo remoto de múltiplos GB funcione diretamente no navegador. Antes de promover uma rota de consulta, provar respostas HTTP, CORS/Range quando aplicável, identidade dos dados e resultado correto em navegador real.

## 6. Etapas com valor público incremental

### Marco 0 — fundação honesta

Home nova, navegação mínima, identidade visual, página Sobre e catálogo do acervo com o que está realmente publicado. Sem falsa busca. Testes de responsividade e acessibilidade.

**Aceite:** uma pessoa entende em uma frase o propósito e encontra competência, manifesto e proveniência; links abrem; não há funções fictícias.

### Marco 1 — primeira ficha verdadeira (MVP)

Busca **por CNPJ completo** (inclusive alfanumérico), página de empresa e origem do dado; campos essenciais extraídos de um snapshot publicado e testado.

**Aceite:** em celular e desktop, pesquisa real por CNPJs de teste de tipos distintos; resposta correspondente a fonte; URL compartilhável; estados vazios, inválidos e indisponíveis; sem varrer Parquet de gigabytes para uma consulta pontual.

### Marco 2 — descoberta por nome

Busca por razão social com resultados, paginação/limite, distinção entre múltiplos registros e tolerância razoável de acentos, com índice e medições de desempenho.

**Aceite:** consultas reais e negativas documentadas, latência e bytes trafegados medidos, sem resultados inventados.

### Marco 3 — dimensão temporal

Seleção e comparação de competências efetivamente disponíveis, com diferenças observadas e ressalvas de evidência.

**Aceite:** reconciliação de dois snapshots reais e tratamento explícito de competência ausente.

### Marco 4 — exploração e dados

Mapas, indicadores e filtros somente com agregações documentadas; catálogo avançado para pesquisadores.

**Aceite:** cada visualização declara universo, competência, transformação, fonte e limitações.

## 7. Métricas de qualidade

Medir por marco, sem metas arbitrárias:
- jornada completa de busca para CNPJ numérico e alfanumérico;
- acurácia contra amostra reprodutível dos artefatos canônicos;
- bytes transferidos e tempo de primeira ficha em rede móvel;
- carga inicial de JS/WASM e tempo para interagir;
- falhas e recuperabilidade de consulta;
- navegação por teclado, leitores de tela e contraste;
- cobertura do catálogo versus manifesto;
- custo de armazenamento/egress se novos mecanismos forem propostos.

CI verde e HTTP 200 são condições operacionais, não provas suficientes de utilidade da pesquisa.

## 8. Limites e não objetivos

- Não reescrever o ETL, o manifesto vigente ou os dados por uma decisão de interface.
- Não promover competências, fazer upload ao Internet Archive, criar serviços pagos ou mover dados sem autorização própria.
- Não publicar inferências sobre pessoas ou relações que a base não sustenta.
- Não preservar o protótipo Astro/Svelte por inércia; tampouco descartá-lo antes de extrair regras e testes úteis.
- Não antecipar toda a experiência analítica antes de disponibilizar uma ficha real.
- Não declarar disponibilidade de buscas até validação funcional em ambiente publicado.

## 9. Relação com trabalho existente

- #141: design review greenfield anterior e gramática de apresentação. Fonte de hipóteses visuais, não substitui a visão presente.
- #269: simplificação da stack e recorte da interface. Esta RFC explicita a decisão de produto, preservando o direcionamento SSG.
- #208: origem de consulta CORS-safe e provas de navegador. Dependência funcional crítica.
- #203: materializações/índices para consultas estreitas e analíticas, com benchmark.
- #247: diagnóstico e roadmap geral do projeto. Fica como coordenação paralela.
- PR #297 e PR #298: shell SSG publicado, sem equivaler a busca funcional.

## 10. Próximos passos de execução

1. Desenhar *wireframes* concretos da home e da ficha, inclusive estados de erro e tela móvel.
2. Definir contrato mínimo de dados da ficha e casos de CNPJ numérico/alfanumérico por competência.
3. Prototipar consulta pontual real e medir latência e bytes antes de escolher o índice final.
4. Implementar Marco 0 e depois Marco 1 por PRs pequenas; cada merge deve resultar em valor público observável.
5. Manter o site atual até substituição validada, sem alegar funcionalidade que não passou no navegador.

**Tese:** menos páginas explicando o FICHA; mais páginas nas quais se utiliza o FICHA.
