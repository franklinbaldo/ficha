# Modelo relacional OKF do Ficha

Este bundle usa documentos OKF como linhas representativas das relações publicadas pelo Ficha. Ele não replica os milhões de registros dos Parquets e não cria uma camada de dados "canônica".

Os tipos de linha (`Cnpj`, `Raiz`, `Socio`, ...) descrevem o shape que também existe em `cnpjs.parquet`, `raizes.parquet`, `socios.parquet` etc. O `okf-parser` pode compilar essas linhas para relações e gerar JSON Schema/Zod; os Parquets continuam sendo a materialização massiva consultada pelo DuckDB/DuckDB-WASM.

`types/` documenta o vínculo entre tipo lógico e entrada do manifest. `examples/` contém linhas provenientes das fixtures já usadas pelo ETL, apenas para tornar o shape observável pelo parser. `views/` contém queries nomeadas de conveniência. Uma view pode ser executada ou materializada, mas não ganha autoridade especial por isso.

Para gerar contratos:

```bash
./scripts/generate-okf-contracts.sh
```

O primeiro slice cobre `Cnpj`, `Raiz` e `Socio`. O próximo gate é comparar os schemas gerados com o schema observado nos Parquets do `manifest.current` e então consumir o Zod gerado no frontend.
