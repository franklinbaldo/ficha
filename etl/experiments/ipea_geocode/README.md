# Experimento Ipea para endereços do Ficha

Instrumento opt-in, sem alteração de ETL, schema, publicação ou site. Ele separa **normalização lexical** de **geocodificação**. Referências: [Ipea geocodebr](https://github.com/ipea/geocodebr), [Ipea enderecobr](https://github.com/ipea/enderecobr), Ficha [#291](https://github.com/franklinbaldo/ficha/issues/291) e [#208](https://github.com/franklinbaldo/ficha/issues/208).

## Execução em sandbox

Fornecer CSV UTF-8 com colunas `tipo_logradouro,logradouro,numero,cep,bairro,municipio,uf` e, opcionalmente, `id`. A opção `--output` cria exclusivamente um arquivo novo; recusa qualquer caminho já existente, inclusive links para a entrada.

```bash
cd etl/experiments/ipea_geocode
python -m unittest -v test_experiment.py test_cohort.py
python -O -m unittest -v test_experiment.py test_cohort.py
python experiment.py /caminho/amostra.csv --output /caminho/relatorio.json
```

Para geocodificar, criar ambiente isolado e instalar `geocodebr` e `polars`; executar `--geocode` **somente após aprovação do download do CNEFE**. O cache inicial pode chegar a aproximadamente 1,2 GB e a API do pacote está em evolução. O experimento usa `definir_campos` e `geocode`, conforme a documentação Python do Ipea. Não incluir essa execução no CI nem despachar ETL mensal.

O CI existente (`pytest tests` em `etl/`) coleta `tests/test_caranguejo_ipea_output.py`: 12 casos de CLI (normal e `-O`) e dois replays da suíte original de onze testes. Nenhuma mudança de workflow ou dependência adicional.

## Interpretação e limitações

O baseline replica **parte da lógica lexical** de `write_enderecos_parquet` da versão contemporânea, mas não equivale a executar DuckDB no writer completo. Ele trata abreviações, acentos, prefixo explícito e tipo separado. A coluna `colliding_street_groups` conta grupos repetidos de UF/município/logradouro, **não** pares de endereços comprovadamente duplicados: número, CEP e bairro não entram nessa métrica.

Os resultados de `geocode` são intencionalmente salvos **sem join por ordem**. Uma etapa posterior precisa estabelecer correspondência estável entre entradas e saídas, examinar colunas reais (`precisao`, desvio e rejeições) e atribuir status `match/ambiguous/unmatched`. Sem essa etapa, não há taxa de acerto.

A próxima rodada deve selecionar amostra *real e autorizada* do snapshot RFB, estratificada por UF/município/tipo/CEP, registrar hash e competência, aferir custo de CPU/RAM/disco, comparar `enderecobr` (verificar API Python e R separadamente) e `geocodebr`, e medir perdas e falsos positivos com revisão manual. Não projetar o tipo de logradouro nos snapshots antigos quando ele não existe. Guardar a licença e a versão das bases CNEFE no relatório antes de eventual materialização. Dados de entrada não devem ser publicados em artefatos públicos sem avaliação de privacidade.

## Controle de coorte (R241)

O CSV inteiro é recusado se houver cabeçalho duplicado ou largura irregular. Linhas válidas preservam todos os campos originais, SHA256 da entrada e identificador interno independente de `id` externo. `writer_eligible_rows` replica o predicado não vazio de logradouro/UF; `query_eligible_rows` exige adicionalmente logradouro, UF e município não brancos (não valida o catálogo municipal). Use nome de município confirmado, não código RFB tratado como IBGE. Grupos lexicais agora usam somente a coorte comum. `queried_rows` permanece zero offline.

A composição do logradouro escolhe o prefixo explícito, quando reconhecido, sem apagar acentos nem expandir abreviações na entrada enviada ao Ipea. Conflitos ficam registrados. A consulta opcional passa `_ficha_row_id` apenas como tentativa de transporte: sua retenção pela API real ainda não foi demonstrada. Resultado continua sem join ou taxa automática. O oráculo sintético `reconcile_candidates` recebe pares previamente validados de ID de linha e ID de candidato, deduplica candidatos, rejeita IDs órfãos e distingue zero/um/vários candidatos. Um candidato único não prova acerto; identidade e critérios reais de precisão ainda precisam de validação. Normalização Ipea e CNEFE NOT_RUN nesta fase.
