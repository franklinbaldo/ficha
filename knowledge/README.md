# Modelo relacional OKF do Ficha

Este bundle usa documentos OKF como linhas representativas das relações publicadas pelo Ficha. Ele não replica os milhões de registros dos Parquets e não cria uma camada de dados "canônica".

Os tipos de linha (`Cnpj`, `Raiz`, `Socio`, ...) descrevem o shape que também existe em `cnpjs.parquet`, `raizes.parquet`, `socios.parquet` etc. Os Parquets continuam sendo a materialização massiva consultada pelo DuckDB/DuckDB-WASM.

`types/` documenta os tipos e contém os sidecars RFC 0006 `.schema.sql`, que declaram o shape físico esperado. `examples/` contém linhas provenientes das fixtures já usadas pelo ETL, apenas para tornar o modelo observável pelo parser. `okf.schema.sql` declara chaves e referências entre tipos. `views/` contém queries nomeadas de conveniência; uma view pode ser executada ou materializada, mas não ganha autoridade especial por isso.

Para validar e gerar contratos:

```bash
bash scripts/generate-okf-contracts.sh
```

O script usa `okf-parser 0.42.0`, valida `PRIMARY KEY`/`FOREIGN KEY` do bundle e gera JSON Schema/Zod a partir dos schemas declarados. O ETL também testa que os sidecars de `Cnpj`, `Raiz` e `Socio` são idênticos ao `DESCRIBE` dos Parquets produzidos pelas fixtures de integração.

O próximo slice é consumir o Zod gerado no frontend e executar as primeiras `View` declaradas sobre as relações já registradas no DuckDB-WASM.
