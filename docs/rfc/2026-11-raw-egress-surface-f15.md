---
type: RFC
title: "Ficha #433: F15, cobertura dos caminhos de escrita raw"
description: "Auditoria estática dos pontos de saída raw e negativos para evitar que um journal novo seja contornado por um helper legado."
status: Draft
date: 2026-11-22
---

# F15: cobertura dos caminhos de escrita raw

**Estado:** proposta, 🐙 Polvo, 22/11/2026. Complementa a [RFC de fence F](2026-11-raw-remote-fence-falsification.md) e a [Ficha #433](https://github.com/franklinbaldo/ficha/issues/433); **não autoriza publicação**.

## Pergunta falsificável

Mesmo que `stream_raw_zips_to_ia` passe a persistir `POSSIBLE_SEND_UNKNOWN`, **qualquer outra função ainda pode escrever `raw/*` sem a mesma admissão da coorte, reserva durável e verificação de versão?**

Um journal colocado em somente um entrypoint protege aquele entrypoint, não o conjunto de escritores. F15 avalia *cobertura de caminhos*, que é pré-condição de confiança no controle de escrita; **não** demonstra o fence remoto F, a selagem S nem a leitura R.

## Evidência de código, não de execução

Leitura estática da `main` do Ficha no commit [`6862c3708d4a8be12c133d4cd949b803eadf9b4e`](https://github.com/franklinbaldo/ficha/commit/6862c3708d4a8be12c133d4cd949b803eadf9b4e). As referências de código a seguir ficam pinadas nesse commit. Busca por usos diretos na árvore versionada **não** inclui importações de consumidores externos, chamadas dinâmicas, outros refs ou processos com credenciais.

| Ponto de entrada | Saída de bytes | Observação estática |
| --- | --- | --- |
| [`upload.stream_raw_zips_to_ia`](https://github.com/franklinbaldo/ficha/blob/6862c3708d4a8be12c133d4cd949b803eadf9b4e/etl/src/ficha_etl/upload.py) | `_ia_s3_put` | Preflight 37/37 e digest da entrada; **sem** journal UNKNOWN produtivo ou readback. Invocado por `monthly_sharded.py` e `cli.py`. O CLI legado não passa `spool_dir`, portanto com objetos faltantes tende a recusar antes do PUT; com todos os nomes presentes pode retornar `ALL_NAMES_PRESENT_UNPROVEN`. |
| [`upload.upload_raw_zips`](https://github.com/franklinbaldo/ficha/blob/6862c3708d4a8be12c133d4cd949b803eadf9b4e/etl/src/ficha_etl/upload.py) | `ia.upload` com chaves `raw/*` | Aceita **qualquer subconjunto não vazio** de ZIPs encontrados no cache e invoca o cliente de upload. Não exige 37/37, journal, pin set ou readback. Busca na `main` localizou definição e testes, **não um chamador de produção versionado**; a função permanece importável. |
| [`upload._stream_one_zip_with_retry`](https://github.com/franklinbaldo/ficha/blob/6862c3708d4a8be12c133d4cd949b803eadf9b4e/etl/src/ficha_etl/upload.py) | Delegação a `_stream_one_zip` | Reabre o fluxo GET+PUT após falhas transitórias; uso estático encontrado na definição e em testes, sem chamada produtiva identificada na `main`. Não confundir código legível com rota operacional ativa. |

Os envios de Parquet/`companies.zip` pertencem ao circuito de **derivados** e seus gates próprios; não devem ser classificados automaticamente como escrita `raw/*`. Esta enumeração **não é** o inventário de credenciais/principals/refs externas exigido por F02/F10.

## Falsificação offline e condições de aceite

A [PR #479](https://github.com/franklinbaldo/ficha/pull/479) inclui agora o teste `test_polvo_legacy_raw_egress_xfail.py`, que prepara **um ZIP válido em 37 nomes canônicos**, substitui `ia.upload` por um sentinel local e chama diretamente `upload_raw_zips`. O oráculo de uma implementação protegida é **nenhuma chamada ao sink** e recusa explícita antes de I/O. No código atual, o sink sintético é alcançado com 1/37; o caso foi marcado `XFAIL(strict=True)`, e **XFAIL não é correção**. A CI no novo SHA deve ser conferida antes de alegar execução satisfatória.

Aceite de F15 requer:

1. Inventário versionado de **todas as rotas** que possam escrever `raw/*`, inclusive importações externas e refs executáveis. Distinguir definição, alcançabilidade, permissão, admissão e efeito remoto.
2. Uma **mesma autoridade de admissão** para cada rota permitida; nenhuma rotina pode chamar o sink raw se não houver coorte fixada, spool S selado, claim distribuído e journal U persistido antes do egress. Não usar ausência de arquivos na listagem como permissão para PUT.
3. Regressão negativa para o helper de cache com 1/37, para nova rota inserida sem guard e para reutilização de credenciais após revogação. Os testes puramente locais apenas verificam cliente/orquestração.
4. Evidência independente de que o armazenamento impõe fence no ponto de visibilidade para **todos os writers** e recusará sobrescrita/DELETE depois do release. Um allowlist de funções Python ou grupos de CI **não** oferece essa garantia.

Rotas possíveis de remediação: remover helper legado **após** provar que não existem consumidores dependentes; torná-lo indisponível em produção; ou rotear cada saída raw por uma única capacidade que exija autorização externa atestada. A escolha é arquitetural, sujeita à coordenação do 🐘 e revisão 🦅 antes de mudar ETL.

**Status:** `F15_EGRESS_COVERAGE_UNATTESTED` e `F_UNAVAILABLE/HOLD`. A evidência atual é de leitura estática e teste com cliente falso. Sem RFB/IAS3 real, ETL produtivo, credenciais, upload, manifesto, custos ou merge.
