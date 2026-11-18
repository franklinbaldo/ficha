# Ficha #281 · Contrato P1 para completude dos dez estabelecimentos RFB

**Estado:** proposta e ensaio offline, 18/11/2026. **Não implementado na produção.**
**Responsável pelo desenho:** 🦌 Cervo. **Ordem:** posterior à correção de
preflight de wire [#277](https://github.com/franklinbaldo/ficha/issues/277).
**Dependência:** a PR de testes [#250](https://github.com/franklinbaldo/ficha/pull/250)
foi integrada, mas seu oráculo não é gate do ETL.

## Falha que o contrato precisa impedir

O gate atual `run_monthly_gate` em `monthly_estabelecimento_gate.py` valida
formato/DV/unicidade sobre `cnpjs.parquet` produzido. A fonte que informa sua
contagem e seus identificadores é o **mesmo** Parquet. Por isso, uma saída
vazia, uma parte omitida ou uma chave substituída por outra válida pode
satisfazer a unicidade isolada. O `rfb_origin_receipt.py` atual valida apenas
metadados de uma lista de **37 ZIPs**, incluindo dez estabelecimentos; seu
ETag de WebDAV **não** é digest de bytes.

Não existe evidência nesta proposta de que o mensal já publicou dados
incompletos. A distinção é entre lacuna de prova e incidente observado.

## Três identidades não intercambiáveis

1. **Transporte:** para cada `Estabelecimentos0.zip` até
   `Estabelecimentos9.zip`, registrar competência `YYYY-MM`, nome exato, origem,
   aquisição, caminho/objeto efetivamente consumido, tamanho e SHA-256 dos
   bytes ZIP. ETag/PROPFIND é apenas informação de origem, nunca SHA-256.
2. **Conteúdo interpretado:** registrar o único membro CSV aceito, SHA-256 dos
   bytes descomprimidos, contagem de registros interpretados, versão/código do
   parser e seu contrato de encoding. Usar a semântica real de `registry` e
   `transform.py` (CSV `;`, aspas, sem cabeçalho; chaves preservadas em VARCHAR;
   transcodificação UTF-8 estrita ou Latin-1 sem perdas). Digest do CSV não
   prova que o parser leu todas as linhas.
3. **Identidade das linhas:** capturar as chaves de origem como texto exato
   `cnpj_basico(8) || cnpj_ordem(4) || cnpj_dv(2)`, inclusive caracteres
   alfanuméricos. Persistir um fluxo independente de chaves para reconciliação
   com as chaves de `cnpjs.parquet` **do mesmo lote e da mesma competência**.
   Um hash do conjunto ordenado ajuda a autenticar recibos, mas a aprovação
   definitiva exige comparação exata de chaves e multiplicidades, não apenas
   hash comutativo, soma, XOR ou contagem. Não converter CNPJ em inteiro.

O recibo nasce na fronteira de ingestão, antes da escrita do Parquet, e fica
vinculado à **mesma** cópia de ZIP que o produtor consome. Não reconstruí-lo
ex post do arquivo a auditar. Registrar `run_id`/competência e um identificador
imutável para os dez recibos, preservando a origem independente e a
proveniência de qualquer retry. A identidade dos bytes pode detectar
alteração depois do registro, mas **não autentica a RFB por si só**: proveniência
de rede, espelho, retenção e cadeia de custódia precisam ser evidenciadas.
Um recibo e seu ZIP reescritos em conjunto sem âncora independente não são
identificáveis como fraude só por SHA-256.

### Risco de duas aquisições sem vínculo (auditoria de código, 18/11/2026)

No caminho existente de `monthly_sharded.command_produce`, a chamada a
`upload.stream_raw_zips_to_ia` antecede `transform.transform_snapshot`.
O primeiro caminho faz **GET RFB → PUT IA diretamente em streaming**, sem gravar
os bytes recebidos em um arquivo local comum com a transformação. Além disso,
`stream_raw_zips_to_ia` pode **pular** um `raw/<nome>.zip` já anunciado no
catálogo IA com tamanho positivo, sem comparar SHA-256 de seus bytes. A etapa
de transformação usa outra resolução, `fetcher.default_chain`:
**cache local → IA raw → RFB**. São duas decisões de origem potencialmente
distintas, não uma única aquisição imutável. Fontes no mesmo mês e com o
mesmo nome **não demonstram** identidade de conteúdo.

Esta é uma lacuna de vínculo no **desenho atual**, não evidência de que algum
snapshot real tenha sofrido divergência. Regras de imutabilidade histórica
declaradas na ADR não substituem conferência de bytes; ETag/PROPFIND também
não fornece o SHA-256. O preflight de wire da #277 protege a **ordem dos PUTs**
para nomes incompatíveis, mas não elimina esta segunda lacuna de custódia.

**Requisito para a implementação produtiva:** fixar, em armazenamento local
imutável durante a rodada, os **mesmos bytes ZIP** aceitos pela transformação
e destinados a `raw/`; calcular hashes nessa aquisição e vincular cada
recibo a `run_id`, competência, nome, origem e destino. Reuso de `raw/`
precisa de prova da identidade dos bytes (ou falhar fechado), nunca somente
de nome e tamanho. Qualquer diferença entre cache, IA e RFB deve ser
explicitamente resolvida **antes de novo PUT**, sem recalcular o testemunho
a partir da saída produzida.

**Contraprovas offline exigidas:** duas versões sintéticas do mesmo
`EstabelecimentosN.zip`, com nome e tamanho iguais e SHA-256 distinto;
cache A versus IA B; `raw/` remoto preexistente sem identidade confiável;
retomada que mistura lotes; e substituição de arquivo entre captura e
transformação. Instrumentar `PUT` por sentinelas e exigir zero chamadas
remotas em qualquer inconsistência, sem acessar RFB ou IA reais. A prova
deve exercitar `command_produce`/o caminho real, não apenas o oráculo
`Counter` desta PR. Medir o impacto de fixar até 37 ZIPs na política de
disco/memória e liberar execução produtiva somente com autorização específica.

## Algoritmo de integração proposto, sem efeitos remotos

A sequência produtiva futura deve ser analisada conjuntamente com #277:

1. **Preflight sem rede:** verificar compatibilidade wire/namespace, competência,
   nomes das dez partes, URLs/origens permitidas e plano de efeitos.
2. **Ingestão local e fixação:** verificar os dez ZIPs; um membro CSV por ZIP;
   integridade de descompressão; nenhum arquivo ausente, vazio inesperado,
   repetido, alias de caminho/inode ou digest idêntico incompatível. Persistir
   recibos atômicos e identificar parser/code fingerprint. Falhar em conflitos
   ou reuso entre competências.
3. **Transformação local:** produzir Parquet a partir dos *mesmos bytes* fixados;
   capturar testemunhos de chave em caminho separado da saída. Nenhum
   tratamento silencioso que descarte registros sem prova de política.
4. **Reconciliação exata:** projetar a chave de `cnpjs.parquet` e comparar com a
   origem como **multiconjuntos**, com ordenação externa e merge-join, ou
   particionamento determinístico por chave com comparação exata por partição.
   Verificar cardinalidade total/per-part, duplicatas, DV, recibos e identidade
   da saída no SHA final. Proibir mistura de materializações ou competências.
5. **Commit local de prova:** só marcar `OK` após gravação atômica de um
   recibo que vincula entradas, código, lotes, chave ordenada, saída e auditoria.
   Mesmo um `OK` local não autoriza publicar automaticamente.
6. **Efeitos remotos:** somente após **todas** as verificações de integridade,
   autorizações operacionais e guardas de wire. Isso inclui *raw ZIP upload*,
   `upload-outputs`, `submit-shard` e promoção. Uma guarda após o primeiro PUT
   não satisfaz fail-closed. Não usar #281 para mudar o cron #262.

### Memória, disco e determinismo

A comparação exata não deve construir `Counter` de dezenas de milhões de CNPJs
na RAM. Executar leitura streaming de fonte e Parquet em lotes, com ordenação
externa/spill ou buckets determinísticos, limite configurado de memória, espaço
temporário medido e limpeza em falha. Auditoria e recibos devem ser pequenos;
medir tempo, bytes lidos/escritos e maior uso de RAM em executor autorizado
antes de habilitar a produção. Testar concorrência, crash entre fases, I/O
interrompido, reexecução e substituição de ZIP depois de capturado; não aceitar
reutilizar um testemunho de um lote em outro.

## Matriz mínima de negativos

| Condição | Resultado esperado |
| --- | --- |
| 10 ZIPs coerentes, CNPJ alfa, permutação de linhas | Aceitar no **oráculo offline** |
| Parte ausente, duplicada, nome extra ou competência trocada | Rejeitar |
| Recibo de outro `run_id`, mesmo mês | Rejeitar no oráculo offline |
| ZIP ou CSV alterado após recibo, recibo adulterado | Rejeitar |
| ZIP recompactado com mesmas linhas, mas bytes diferentes | Rejeitar identidade ZIP; identidade semântica pode continuar igual |
| Zero chunks, CSV vazio, saída truncada | Rejeitar |
| Troca de chave válida, mesma cardinalidade | Rejeitar na igualdade de multiconjuntos |
| Chave duplicada entre partes, alias de caminhos/ZIP idêntico | Rejeitar |
| Mesmo CNPJ em partes com ZIPs de bytes distintos | Rejeitar; hash de ZIP sozinho não demonstra unicidade global |
| ZIP malformado, membro extra ou CSV com schema incorreto | Rejeitar |
| CNPJ alfanumérico/numérico com DV incorreto e formato correto | Rejeitar na captura sintética, antes do recibo |
| Incompatibilidade wire ou falha de completude | Zero chamadas remotas |

## Evidência disponível e próximos gates

O oráculo também vincula recibos a um `run_id` **sintético**, sem autenticar
quem declarou o identificador. Uma contraprova executável troca **em conjunto**
ZIP, recibo e saída e obtém aceitação: ela demonstra o limite epistemológico
já descrito acima, **não** prova origem RFB nem uma vulnerabilidade observada
no pipeline real. Só uma âncora externa de confiança e ligação aos mesmos bytes
consumidos pela ingestão poderá transformar consistência em custódia.

O oráculo offline agora verifica também os **dois dígitos verificadores (DV)**
na leitura da fonte sintética, por módulo 11 e valores ASCII menos 48,
antes de emitir recibo. Controles com CNPJ alfanumérico e numérico de
formato correto, mas DV adulterado, devem falhar na captura. Isso resolve
a ressalva de revisão sobre a autossuficiência do **oráculo de testes**;
**não demonstra** que o parser produtivo aplique a mesma verificação nem
substitui o gate de DV já existente sobre o Parquet de saída.

`etl/tests/test_cervo_rfb_completeness_p1.py` é uma **referência de teste**
com dez ZIPs/CSV artificiais; usa `Counter` apenas porque a fixture é pequena,
e `csv.reader` como leitor independente. Não substitui o parser DuckDB real,
o hash SHA-256 de todos os CSVs da RFB ou a etapa de produção. Não é gate de
publicação, não modifica `monthly_sharded.py`, `transform.py`, manifesto,
workflows, Internet Archive ou recursos pagos.

Para uma PR produtiva separada, depois de #277: (a) capturar testemunho real
na ingestão, (b) conectar comparação externa à saída e ao preflight anterior
**a todo I/O remoto**, (c) testar mutantes RED→GREEN no caminho real com
callbacks remotos espions sem redes, (d) executar gates Linux/Windows no SHA
exato, revisão Gavião/Castor/Lontra, e (e) solicitar decisão do Elefante.
Somente uma execução com fontes reais autorizada poderá dizer algo sobre
completude de uma competência real. Esta documentação **não autoriza** ETL,
Internet Archive, promoção, release nem despesas.
