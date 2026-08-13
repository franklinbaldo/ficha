# Modelo relacional OKF do Ficha

Este bundle usa documentos OKF como linhas representativas das relações publicadas pelo Ficha. Ele não replica os milhões de registros dos Parquets e não cria uma camada de dados canônica.

Os tipos de linha (`Cnpj`, `Raiz`, `Socio`, ...) descrevem o shape que também existe em `cnpjs.parquet`, `raizes.parquet`, `socios.parquet` etc. O `okf-parser` pode compilar essas linhas para relações e gerar JSON Schema/Zod; os Parquets continuam sendo a materialização massiva consultada pelo DuckDB/DuckDB-WASM.

`types/` documenta o vínculo entre tipo lógico e entrada do manifest. `examples/` contém linhas provenientes das fixtures já usadas pelo ETL, apenas para tornar o shape observável pelo parser. `views/` contém queries nomeadas de conveniência. Uma view pode ser executada ou materializada, mas não ganha autoridade especial por isso.

`okf.schema.sql` declara identidade e integridade entre os tipos com a RFC 0007 do `okf-parser`: `Raiz.cnpj_base` e `Cnpj.cnpj` são chaves, e `Cnpj.cnpj_base` e `Socio.cnpj_base` referenciam `Raiz.cnpj_base`.

Para validar o bundle e gerar contratos:

```bash
bash scripts/generate-okf-contracts.sh
```

O primeiro slice cobre `Cnpj`, `Raiz` e `Socio`. O próximo gate compara os schemas gerados com o schema dos Parquets produzidos pelo ETL e então consome o Zod gerado no frontend.
