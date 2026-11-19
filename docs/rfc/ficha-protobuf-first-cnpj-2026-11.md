---
type: RFC
title: "Consulta exata de CNPJ por Protobuf atômico"
description: "Rota Protobuf-first no site SSG, custos observados e critérios de promoção sem Parquet de 6 GB"
status: "Proposta técnica; teste remoto parcial concluído, implementação no produto pendente"
date: 2026-11-19
---

# RFC: abrir ficha de CNPJ exato sem Parquet

## Decisão proposta

Para uma **ficha individual por CNPJ completo**, usar primeiro o membro
Protobuf da raiz no ZIP atômico declarado pelo `manifest.json`. O Parquet
permanece no acervo para análises e buscas que exijam índice/varredura; não é
pré-requisito para abrir uma ficha. Esta decisão não autoriza despublicar
Parquets, alterar a competência vigente, promover Protobuf 1.1 ou implantar
relay/proxy.

O [greenfield aprovado](ficha-greenfield-visao-produto-2026-11.md) já prefere
acesso atômico. Esta RFC especifica o transporte, o decoder, as limitações e
os critérios de teste da implementação. A [issue #208](https://github.com/franklinbaldo/ficha/issues/208)
permanece aberta para a trilha analítica DuckDB-WASM quando ela for necessária.

## Evidência real, 19/11/2026

Em Ifro, foram recuperadas e decodificadas quatro fichas **v1 publicadas**
no Internet Archive do snapshot `2026-07`, acessando somente o membro
`companies-XX.zip/XX/XXX/XXX.pb`, após redirect para `view_archive.php`:

| Raiz | PB recebido | Primeiro GET, s | Estabelecimentos | Sócios |
| --- | ---: | ---: | ---: | ---: |
| `00000000` (Banco do Brasil) | 1.370.270 B | 3,291 | 7.902 | 41 |
| `33000167` (Petrobras) | 101.606 B | 2,608 | 661 | 9 |
| `60746948` (Itaú) | 1.542.666 B | 3,396 | 9.999 | 106 |
| `18236120` (Nubank) | 2.860 B | 1,910 | 10 | 18 |

Somatório de corpos PB: **3.017.402 B**. São quatro casos deliberadamente
escolhidos, não uma amostra representativa nem p95.

O [primeiro ensaio Chromium na origem real do
Pages](https://github.com/franklinbaldo/ficha/issues/208#issuecomment-7009984682)
recuperou membros de Nubank/Petrobras via `fetch` CORS sem flags especiais.
Um segundo ensaio reproduzível ampliou a prova: **Chromium real no
`https://franklinbaldo.github.io/ficha/empresa/`**, viewport 375×812,
módulo gerado `web/src/generated/company.pb.js` empacotado localmente com
esbuild, **98.383 B** de JS minificado (SHA-256
`04b05b3068141cfc3fea5256749f6bc6ad9d4829580a2cd611edf8cbf6b3d180`).
Em ambos os CNPJs completos, ocorreu exatamente um match de
estabelecimento dentro da raiz, zero match com dígito final alterado,
e a cidade/CNAE vieram de lookups `*.pb` do mesmo ZIP:

| CNPJ completo | Membro PB | Municípios PB | CNAEs PB | Resultado |
| --- | ---: | ---: | ---: | --- |
| `18236120000158` | 2.860 B | 103.604 B | 86.124 B | São Paulo/SP; atividade recuperada |
| `33000167000101` | 101.606 B | 103.604 B | 86.124 B | Rio de Janeiro/RJ; atividade recuperada |

Corpos totais por ficha do teste, incluindo esses dois lookups:
**192.588 B** e **291.334 B**. Os lookups `municipios` (5.572 entradas)
e `cnaes` (1.359 entradas) tiveram SHA-256 idêntico nos shards `18`
e `33`, mas isso não prova identidade entre todos os 100 shards.
O [script de replay](https://github.com/franklinbaldo/polvo/blob/main/scripts/ficha_atomic_pages_probe.mjs)
requer `--live`, paths explícitos de protobufjs/esbuild/Playwright e não
faz upload nem muda o site.

Alternativa medida: o `lookups.json` **já publicado**, de 272.145 B,
respondeu HTTP 200 e CORS a um `fetch` diretamente da origem GitHub Pages
(2,505 s, teste singular), com `snapshot_date=2026-07`,
`schema_version=1.0.0`, 5.572 municípios e 1.359 CNAEs. É uma
**requisição em vez de duas** para esses dicionários, porém com mais bytes
(272.145 B versus 189.728 B). A implementação pode usar o
`snapshot.files.lookups.url` do manifesto, validando competência e contrato,
sem inventar índice novo. O código de município `7107` desse snapshot
resolve para São Paulo; **não inferir que seja código IBGE de sete dígitos**.

## Contrato proposto para a interface

1. Validar e normalizar o **CNPJ completo de 14 caracteres** antes da
   consulta, sem perder letras. O fluxo v1 existente representa apenas
   raízes numéricas; **não** enviar alfanumérico para shard v1.
2. Selecionar competência existente e artifact `files.companies` v1
   com mapeamento `cnpj_base_prefix_2`. Resolver o shard declarado
   da raiz e conferir vínculo **shard ↔ URL ↔ competência** antes do fetch:
   no v1 publicado, o URL deve ser exatamente
   `https://archive.org/download/ficha-{date}/companies-{shard}.zip`.
   O schema genérico atual valida unicidade, mas isso sozinho não
   impede URLs de shards permutadas. Construir somente
   `XX/XXX/XXX.pb`; não transferir ZIP.
   Snapshots legados `companies_zip` exigem seleção explícita da sua rota;
   ausência do artefato não pode virar fallback silencioso ao Parquet.
3. Decodificar usando o schema gerado `proto/ficha/v1/company.proto`
   (a implementação v1 já existe em `web/src/lib/companies.ts`).
   O site SSG só deve receber um bundle **determinístico e estático** do
   decoder com protobufjs/minimal já existente, sem exigir Astro,
   Svelte, CDN externa ou DuckDB-WASM. Não reimplementar a gramática
   Protobuf manualmente.
4. Validar **raiz decodificada == raiz solicitada**, selecionar
   **exatamente um** estabelecimento cujo `base+ordem+DV` corresponde
   ao CNPJ completo; zero matches é ausência daquela filial na competência,
   não prova de inexistência da raiz. Duplicação, wire inválido,
   identidade diferente e versão não suportada são erros.
5. Adaptar separadamente os campos de `Company` (razão social,
   capital etc.) e do estabelecimento selecionado (nome fantasia,
   UF, município, CNAE, Simples/MEI). Resolver nomes com lookups
   **da mesma competência**. Respeitar campos ausentes e zero como
   dado ausente quando definido pelo schema; não fabricar descrições,
   datas nem indicadores. Há perda de informação no produtor v1:
   `_bool(None) → False` para Simples/MEI, de modo que um `false`
   no Protobuf **não prova que a fonte informou Não**. Não declarar
   negatividade definitiva sem contrato que preserve nulidade.
   Também não converter CNPJ ausente de sócio PF/estrangeiro no
   identificador fictício `00000000000000`.
6. Estados distintos e acessíveis: `404` de membro, `503`/falha de
   rede, resposta inválida, wire incompatível, CNPJ completo sem
   estabelecimento e sucesso. Na observação real, uma raiz artificial
   retornou **503**, nunca convertê-la em `404`. Não mostrar resultado
   velho em corrida A→B→A; respeitar a proteção de seleção da
   [PR #349](https://github.com/franklinbaldo/ficha/pull/349) quando integrada.
7. Preservar competência, fonte, URL arquivística do ZIP e versão no
   resultado. A identidade `size+SHA` declarada para o ZIP não é
   prova de SHA-256 do **membro individual** obtido via `view_archive`.
   Não anunciar integridade integral que não foi aferida.

## Divisão de consultas

- **CNPJ exato:** Protobuf-first e lookup, sem Parquet nem relay.
- **História da mesma empresa:** dois Protobufs, cada qual da própria
  competência e wire compatível; diferença observada não data a mudança.
- **Razão social, pessoa, município, CNAE e agregados:** índices
  apropriados ou trilha analítica separada, com orçamento real de
  bytes, requests e latência antes da promoção.
- **CNPJ alfanumérico:** somente contrato 1.1 com descoberta
  `manifest-atomic-v1.1.json` opt-in e URL versionada, quando publicado;
  não tratá-lo como suportado por ZIP numérico v1.

## Gates antes de anunciar busca funcional

- Reutilizar e testar o decoder canônico, em vez de copiar resultados
  de fixtures e confundir prova de transporte com UI.
- Testes offline de seleção de estabelecimento, lookups e falhas,
  incluindo CNPJ divergente, repetido, truncado, errado, alfa/v1,
  mapeamento de URL de shard trocado, Simples/MEI desconhecido e
  sócio PF sem CNPJ.
- E2E no Chromium **sem flags** e origem Pages em 375×812 e 1440×900
  para casos numéricos e negativos (404 ≠ 503), além de URL compartilhável,
  A→B tardia e A→B→A com seleção alterada.
- Medir carga do bundle e corpo por empresa, cold/warm, bytes totais
  (incluindo lookups, headers e redirecionamentos) e latência p50/p95
  com amostra declarada, sem assumir cache. As maiores empresas podem
  ter fichas significativamente maiores.
- Verificar que a busca **não** requisita `cnpjs.parquet` nem
  inicializa DuckDB para CNPJ exato; testes da UI e CI no SHA final.
- Não alterar ETL, manifesto, Internet Archive, cron nem provisionar
  proxy, CDN ou serviço pago. A integração respeita a fila e a
  coordenação do Elefante; a frente UI pertence ao Morcego/Raposa.

## Situação

**Prova de transporte + decode + busca exata de dois CNPJs completos em
Chromium: PASS. Produto SSG integrado: NÃO FEITO.** Os testes não
comprovaram lookup por nome, histórico, alfanumérico v1.1, hashes
integrais dos 100 ZIPs, disponibilidade permanente do IA ou orçamento
p95 global. Esta RFC é proposta; não muda o comportamento publicado.
