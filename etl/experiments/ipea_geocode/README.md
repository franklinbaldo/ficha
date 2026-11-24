# Experimento Ipea para endereços do Ficha

Instrumento opt-in, sem alteração de ETL, schema, publicação ou site. Ele separa **normalização lexical** de **geocodificação**. Referências: [Ipea geocodebr](https://github.com/ipea/geocodebr), [Ipea enderecobr](https://github.com/ipea/enderecobr), Ficha [#291](https://github.com/franklinbaldo/ficha/issues/291) e [#208](https://github.com/franklinbaldo/ficha/issues/208).

## Execução em sandbox

Fornecer CSV UTF-8 com colunas `tipo_logradouro,logradouro,numero,cep,bairro,municipio,uf` e, opcionalmente, `id`. Os dados originais nunca são reescritos.

```bash
cd etl/experiments/ipea_geocode
python -m unittest -v test_experiment.py
python -O -m unittest -v test_experiment.py
python experiment.py /caminho/amostra.csv --output /caminho/relatorio.json
```

Para geocodificar, criar ambiente isolado e instalar `geocodebr` e `polars`; executar `--geocode` **somente após aprovação do download do CNEFE**. O cache inicial pode chegar a aproximadamente 1,2 GB e a API do pacote está em evolução. O experimento usa `definir_campos` e `geocode`, conforme a documentação Python do Ipea. Não incluir essa execução no CI nem despachar ETL mensal.

## Interpretação e limitações

O baseline replica **parte da lógica lexical** de `write_enderecos_parquet` da versão contemporânea, mas não equivale a executar DuckDB no writer completo. Ele trata abreviações, acentos, prefixo explícito e tipo separado. A coluna `colliding_street_groups` conta grupos repetidos de UF/município/logradouro, **não** pares de endereços comprovadamente duplicados: número, CEP e bairro não entram nessa métrica.

Os resultados de `geocode` são intencionalmente salvos **sem join por ordem**. Uma etapa posterior precisa estabelecer correspondência estável entre entradas e saídas, examinar colunas reais (`precisao`, desvio e rejeições) e atribuir status `match/ambiguous/unmatched`. Sem essa etapa, não há taxa de acerto.

A próxima rodada deve selecionar amostra *real e autorizada* do snapshot RFB, estratificada por UF/município/tipo/CEP, registrar hash e competência, aferir custo de CPU/RAM/disco, comparar `enderecobr` (pacote R) e `geocodebr`, e medir perdas e falsos positivos com revisão manual. Não projetar o tipo de logradouro nos snapshots antigos quando ele não existe. Guardar a licença e a versão das bases CNEFE no relatório antes de eventual materialização. Dados de entrada não devem ser publicados em artefatos públicos sem avaliação de privacidade.
