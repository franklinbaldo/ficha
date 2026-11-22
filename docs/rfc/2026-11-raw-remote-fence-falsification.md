---
type: RFC
title: "Ficha #433: aceitação falsificável da finalização remota F"
status: Draft
date: 2026-11-22
---

# RFC: aceitação falsificável da finalização remota F

**Estado:** Draft, 🐙 Polvo, 22/11/2026. [Ficha #433](https://github.com/franklinbaldo/ficha/issues/433). Esta RFC não libera PUT, ETL nem promoção.

## Problema e fronteira de evidência

Depois de confirmar B mediante GET e SHA-256, um PUT A antigo ainda pode se efetivar e sobrescrever B. GETs repetidos não certificam ausência de escrita *futura*: os históricos "nenhuma escrita posterior" e "A se aplica após o último GET" produzem as mesmas respostas até esse instante. [Modelo R159](https://github.com/franklinbaldo/polvo/blob/main/workspace/ficha_remote_fence_r159.py). Se A/B usam nomes com SHA, não colidem **somente sob escritores conformes**; o servidor não passa a ser write-once pelo simples nome. [Modelo R160](https://github.com/franklinbaldo/polvo/blob/main/workspace/ficha_content_addressed_r160.py).

Os 37 GETs individuais da [PR #463](https://github.com/franklinbaldo/ficha/pull/463) podem estabelecer, no máximo, `R_MATCH_AT_T_ONLY`. [Castor R160](https://github.com/franklinbaldo/ficha/issues/433#issuecomment-7078908637) apresentou execução em que todos os 37 GETs estão corretos e **não existe instante comum** de 37 objetos corretos. Não inferir snapshot atômico do lote.

**Frescor do origin não decorre de SHA do corpo recebido.** [Castor R162](https://github.com/franklinbaldo/ficha/pull/466#issuecomment-7080248779) construiu dois mundos **sintéticos** observacionalmente indistinguíveis: 37 corpos B corretos são servidos (i) pelo origin B ou (ii) por cache adversarial B enquanto o origin está em A. A requisição `Cache-Control: no-cache` exige revalidação por cache conforme [RFC 9111 §5.2.1.4](https://www.rfc-editor.org/rfc/rfc9111.html#section-5.2.1.4); o mundo (ii) representa um intermediário hipoteticamente **não conforme**, não evidência de comportamento do IA nem defeito demonstrado na #463. O recibo `R_MATCH_AT_T_ONLY` descreve os **bytes entregues pelo endpoint observado**, não a versão ativa do origin.

**R/origin e F são propriedades ortogonais.** `R_ORIGIN_EVIDENCED` exigiria evidência independente de que os bytes recebidos correspondem ao estado do origin no instante relevante; `F_EVIDENCED` exige evidência independente de que escritas/remoções tardias ou rollback não invalidam a geração publicada. Um cache adversarial pode invalidar inferência de R/origin **mesmo com F verdadeiro**; inversamente, origin atestado não impede PUT tardio. F11 é **pré-condição de evidência para R/origin na promoção**, não um ensaio que falsifique F. Ver [revisão semântica 🐝 Abelha R157](https://github.com/franklinbaldo/ficha/pull/466#issuecomment-7080869461).

[IAS3](https://archive.org/developers/ias3.html) descreve ingestão assíncrona e versões antigas opcionais; [Tasks API](https://archive.org/developers/tasks.html) descreve serialização de tarefas por item. Nenhuma dessas descrições, isoladamente, prova create-only, CAS, drenagem de escritas pendentes, proteção a DELETE, exclusividade de todos os escritores ou monotonicidade da raiz. Ausência de garantia na documentação consultada tampouco prova impossibilidade técnica do serviço.

## Estados decisórios

- **F_UNAVAILABLE / HOLD:** primitivas insuficientes ou sem prova pertinente. Estado inicial.
- **F_CANDIDATE / HOLD:** contrato externo específico é identificável, mas ainda não passou aos ensaios autorizados com implementação e credenciais efetivas.
- **F_EVIDENCED / STILL_HOLD:** houve prova específica no backend escolhido, incluindo negativos e mutantes. Ainda não autoriza publicação: S (fonte/coorte), spool físico, U (intent UNKNOWN) e R são garantias separadas, além da estabilidade simultânea dos 37 objetos.

Falta de registro ou demonstração **da própria autoridade de escrita F**, documentação ambígua sobre o fence, falha nos ensaios F ou mudança de sua configuração => **F_UNAVAILABLE**. Ausência de atestação de R/origin é **`R_ORIGIN_UNATTESTED`**, não altera automaticamente um estado F obtido independentemente. Não existe caminho desta RFC para declarar `REMOTE_VERSION_CONFIRMED`.

| Evidência F | Evidência R/origin | Decisão |
| --- | --- | --- |
| `F_EVIDENCED` | `R_ORIGIN_UNATTESTED` | **HOLD**: fence pode estar provado; leitura de origin não |
| `F_UNAVAILABLE` | `R_ORIGIN_EVIDENCED` | **HOLD**: origem pode estar atestada; escrita futura não |
| `F_EVIDENCED` | `R_ORIGIN_EVIDENCED` | **STILL_HOLD** enquanto S, spool, U e coorte não estiverem comprovados |

Nenhuma linha promove os 37 ZIPs.

## Identidade que precisa existir antes do primeiro PUT

Descritor canônico: `(competencia, run_id, run_attempt, inventory37=[(nome,tamanho,SHA256,chave_remota)], digest_manifesto, generation, authority_id)`.

O inventário tem 37 entradas distintas. O journal UNKNOWN fica ligado a esse descritor fixado **fora** do spool e da própria listagem mutável do destino. Persistir `POSSIBLE_SEND_UNKNOWN` antes da possibilidade de transmissão individual; timeout, 503, crash e restart não concedem novo PUT por ausência aparente no catálogo. Uma cópia anterior do banco local não pode conceder nova admissão: a autoridade necessita de proteção distribuída antirollback.

O SHA do manifesto salvo junto ao manifesto, um marcador `ready` ou uma chave `by-sha256/` não são prova externa independente.

## Rotas candidatas a F

| Rota | Garantias que o destino/autoridade precisam impor, não apenas o cliente | Falha que refuta |
| --- | --- | --- |
| **A. Mesma chave + fence remoto** | Drenagem/rejeição de todas as escritas anteriores em voo; revogação abrangente de escritores antigos; negação de PUT/DELETE posteriores à barreira; release monotônico | PUT A pendente conclui depois da leitura e promoção B |
| **B. Objetos por SHA + release externo** | Create-only atômico e imutabilidade real dos bytes e da existência da chave, inclusive contra escritor não conforme; cobertura de todas as credenciais; descritor de 37 pins em autoridade monotônica, append-only/CAS | Outro escritor modifica/deleta a chave B ou ponteiro `current` volta a A |

Se nenhuma rota puder ser demonstrada no armazenamento e autoridade realmente escolhidos, manter **HOLD**. Prefixar nomes com SHA não substitui write-once; CAS apenas no coordenador não impede PUT pendente no destino; TTL de lease não drena requisições antigas.

## Plano de falsificação

**Somente especificação de experimentos.** Rodar primeiro com scheduler e transporte sintéticos. Qualquer PUT/GET/DELETE em backend remoto, mesmo item descartável, exige autorização explícita e revisão de segurança.

| Teste | Ação adversarial | Oráculo necessário |
| --- | --- | --- |
| F01 tardio | Enfileirar PUT A, suspender a conclusão; escrever B, fazer readback B, fixar release B; liberar A | A não altera B nem o release |
| F02 escritor alternativo | Outra credencial tenta escrever C, mesmo tamanho e SHA diferente, na chave por SHA de B | **Backend** rejeita, não somente validador cliente |
| F03 remoção | Tentar apagar ou trocar objeto publicado | Bytes e vínculo da chave continuam imutáveis |
| F04 coordenação | Dois runners leem geração g e tentam releases divergentes g+1 | Um CAS vence; perdedor não publica nem reenvia |
| F05 rollback | Restaurar journal e manifesto locais anteriores à primeira reserva; tentar reemitir | Autoridade externa monotônica impede segunda admissão |
| F06 ponteiro | Aplicar atualização tardia de `current` para versão antiga | Leitura de release não regride |
| F07 37 leituras | Alternar objetos entre GETs, mantendo até 1/37 correto de cada vez | `37_GET_OK` não produz afirmação `37_AT_ONCE` |
| F08 crash | Interromper depois de intent, antes do PUT; e depois de PUT/503 ambíguo | UNKNOWN persistido, zero replay |
| F09 identidade | GET com SHA correto mas URL/chave/redirect divergente | Recusa mesmo com bytes corretos |
| F10 mudança | Alterar ACL, cliente, regra de storage, backend ou authority generation | Invalidar prova antiga, qualificar novamente |
| F11 cache/origin (**limite R, não teste de F**) | Construir duas execuções com os mesmos 37 HTTP 200 e SHA B: origin B legítimo versus origin A com cache B adversarial (inclusive `no-cache` ignorado no modelo); variar observação independente do origin | Recusar `HTTP_BODY_MATCH => ORIGIN_CURRENT_B`; sem atestação específica da origem, `R_ORIGIN_UNATTESTED => HOLD`, **sem rebaixar automaticamente um F independentemente demonstrado** |

Pelo menos um **mutante negativo** por propriedade testada deve ser detectado (desligar CAS, permitir overwrite, aceitar alias mutável, remover journal, excluir writer da política). Mock é apenas ensaio de lógica; não é teste do Internet Archive. Os testes de cliente/HTTPX da #463 e o cenário F11 pertencem à fronteira de **R/origin**, não demonstram nem refutam isoladamente F. Em especial, nenhum número de GETs, `Cache-Control: no-cache`, SHA de corpo ou redirect TLS autenticado **sozinho** certifica o estado atual do origin ou ausência de writes futuros.

**Fronteira adicional do adaptador R produtivo, distinta de F11:** o futuro adaptador deve **criar e possuir exclusivamente** seu `httpx.Client`, com transporte direto e TLS verificado, rejeitando transporte de teste/injetado (`MockTransport`), cliente compartilhado e cabeçalhos/proxies não autorizados antes de qualquer I/O. Em negativo offline, injetar resposta sintética B ou replay obsoleto com 37 SHA corretos e provar **zero promoção**. Um `MockTransport` continua legítimo **somente nos testes** da #463; rejeitá-lo no adaptador futuro não é mudança retroativa nem prova de fidelidade do origin. Registrar endpoint, redirects, autoridade TLS, headers cache/validação e origem da resposta; cabeçalhos `Age`/`Via`/`ETag` são observáveis auxiliares, não atestação infalível por si. Ver [decisão 🐘 R168](https://github.com/franklinbaldo/ficha/issues/433#issuecomment-7080295264).

## Pacote de evidência exigido

Registrar os 37 pins e digest do descritor, chaves físicas, geração da autoridade, esquema de controle de escritores, versão e configuração do backend, SHA do commit, cenários F01–F10 para F e F11 para R/origin, ordem real de submissão/aplicação, status e headers, digest independente de GET, log de recusa do storage, resultado CAS e controles mutantes. Os dois campos de evidência devem ser independentes. Guardar hora, duração e limites; não registrar credenciais.

Distinguir explicitamente três níveis: **documentação do provedor**, **simulação offline** e **teste remoto autorizado**. Um teste sob uma credencial não cobre administradores, writers legados ou DELETE por outras credenciais. Se a política mudar, a prova perde validade. Para promoção futura, S/U/R/F e estabilidade da coorte precisam ser demonstrados **conjuntamente**; 37 respostas 200 não constituem transação de lote.

## Encaminhamento por responsabilidade

**🐘 Elefante:** decidir armazenamento/autoridade e política de todos os escritores antes de implementar publicação. **🐙 Polvo:** contrato de admission/UNKNOWN e prova antirollback. **🦦 Lontra + 🦫 Castor:** testes de falsificação do fence, readback e contraexemplos. **🦀 Caranguejo + 🐢 Tartaruga:** selagem física do spool, coorte 37/37, orçamento de FD e negativo 37º inválido => 0 PUT. **🦅 Gavião:** revisão excepcional SHA-exata pré-merge para qualquer mudança ETL, upload ou manifesto.

A #463 pode ser considerada **isoladamente** após revisão final, pois seu resultado é R restrito. Esta RFC é somente um contrato de decisão: não muda uploader, CLI, cron, manifestos ou armazenamento. [#433](https://github.com/franklinbaldo/ficha/issues/433) permanece OPEN/HOLD.
