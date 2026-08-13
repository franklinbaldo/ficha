---
type: TypeSpec
title: Socio
description: Uma linha publicada em socios.parquet; representa uma relação entre uma raiz de CNPJ e um sócio.
---

# Socio

Contrato relacional de uma linha de `socios.parquet`.

`cnpj_base` relaciona cada linha à [Raiz](raiz.md) correspondente. `cpf_mascarado` e `cnpj_socio` são projeções condicionais do identificador publicado pela RFB.
