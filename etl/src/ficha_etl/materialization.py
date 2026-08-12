"""Identidade lógica de materialização de um shard (#147).

Responde uma pergunta, e só ela:

    este shard remoto é reutilizável para estes inputs e para a semântica
    atual do pack?

É a identidade que a retomada precisa **antes de gerar**. Ela não é, e não
tenta ser, o hash do ZIP produzido — essa é outra identidade:

| | pergunta | computável |
|---|---|---|
| materialização (aqui) | dá para reutilizar o shard remoto? | antes de gerar |
| artefato (`sha1`/`sha256`) | que objeto exatamente foi publicado? | só depois de gerar |

Confundir as duas foi um erro real do desenho inicial: por querer prever o
SHA-256, a identidade lógica acabava carregando determinantes de container que
não afetam a empresa representada.

O critério de inclusão é sempre o mesmo:

    se só isto mudar, um shard já publicado continua semanticamente válido e
    seguro para reutilizar?

Se a resposta for "sim", o campo **não** pertence aqui.

Ficam de fora, com a verificação que sustenta cada exclusão:

- **ordem de iteração de `LOOKUP_KINDS`** — `row_to_company` não referencia
  lookups em nenhum ponto; a ordem só decide a posição física dos membros
  `_lookups/*.pb` dentro do ZIP;
- **`_schema.proto`** — é a fonte `.proto` empacotada para leitura humana, não
  participa da serialização; quem governa é o descritor compilado, que é
  exatamente o que `descriptor_sha256` cobre;
- timestamp ZIP canônico, nível DEFLATE, `external_attr`, ordem física dos
  membros — container puro;
- versões de zlib, protobuf e Python — mudam bytes, não a empresa
  representada. Se alguma delas puder mudar a **semântica**, aí precisa ser
  coberta por `packer_format_version`, explicitamente e não por precaução;
- hostname, path local, runner, relógio — não são propriedades dos dados;
- hashes do ZIP final — são a outra identidade.

Este módulo é puro: não faz rede, não consulta o Internet Archive e não decide
`SKIP`/`UPLOAD`. Essa decisão é da máquina de estados de reuse, que só será
ligada depois que o probe #155 disser se o `_meta.json` remoto é legível como
mecanismo operacional.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

MATERIALIZATION_VERSION = 1
"""Versão do formato *deste documento*. Muda se a estrutura do spec mudar."""

PACKER_FORMAT_VERSION = 1
"""Fronteira de compatibilidade semântica do pack.

Muda quando uma alteração torna materializações anteriores **inadequadas para
reuse**: o schema muda, o protobuf representado muda, a regra de construção
muda semanticamente, a ordenação contratada muda, o `_meta.json` muda de forma
incompatível, ou o formato público do shard muda.

**Não** precisa mudar porque zlib ou Python foram atualizados, nem porque o
compressor produziu bytes diferentes mas semanticamente equivalentes — isso
invalidaria todos os shards sem nenhum ganho.

A ressalva que torna a regra segura: se uma dependência puder mudar a
*semântica* — protobuf alterando o valor decodificado, não apenas os bytes —
ela precisa ser coberta aqui, explicitamente.
"""


@dataclass(frozen=True)
class ShardRange:
    """A faixa exata de raízes que o shard contém.

    `kind` existe para que a geometria não fique implícita: se um dia o shard
    for definido por outra coisa que não prefixo de `cnpj_base`, materializações
    antigas não podem colidir com as novas por acidente. A largura do prefixo
    está contida em `value` — `"07"` e `"070"` são faixas diferentes e produzem
    identidades diferentes.
    """

    value: str
    kind: str = "cnpj_base_prefix"

    def as_document(self) -> dict:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True)
class MaterializationSpec:
    """Os determinantes semânticos de um shard, e nada além deles."""

    snapshot: str
    shard_range: ShardRange
    inputs: Mapping[str, str]
    descriptor_sha256: str
    packer_format_version: int = PACKER_FORMAT_VERSION
    materialization_version: int = MATERIALIZATION_VERSION

    def as_document(self) -> dict:
        """O documento canônico, antes da serialização.

        `inputs` é ordenado por nome aqui, e não onde é construído: a ordem em
        que os metadados do Internet Archive chegam não pode influenciar a
        identidade.
        """
        return {
            "materialization_version": self.materialization_version,
            "packer_format_version": self.packer_format_version,
            "snapshot": self.snapshot,
            "range": self.shard_range.as_document(),
            "inputs": {nome: {"sha1": self.inputs[nome]} for nome in sorted(self.inputs)},
            "schema": {"descriptor_sha256": self.descriptor_sha256},
        }

    def canonical_json(self) -> bytes:
        return canonical_json(self.as_document())

    def materialization_id(self) -> str:
        return hashlib.sha256(self.canonical_json()).hexdigest()

    def meta_payload(self) -> dict:
        """O bloco que vai dentro do `_meta.json` do shard.

        Guarda o documento **e** o id: o id compara em O(1), e os componentes
        permitem que uma divergência diga `input socios.parquet mudou` em vez de
        apenas `materialization_id diferente`.

        `artifact` fica explicitamente separado e vazio: ele é a identidade dos
        bytes produzidos, preenchida depois da geração, e não participa da
        decisão de reuse.
        """
        return {
            "materialization": {"spec": self.as_document(), "id": self.materialization_id()},
            "artifact": None,
        }


def canonical_json(documento: dict) -> bytes:
    """Serialização canônica: mesma estrutura → mesmos bytes, sempre.

    Os parâmetros são explícitos porque a estabilidade da identidade não pode
    depender dos defaults de `json.dumps`, que são propriedade da biblioteca e
    não deste contrato:

    - `sort_keys` — a ordem de inserção num dict não pode mudar o hash;
    - `separators` sem espaços — os defaults de `dumps` incluem um espaço depois
      da vírgula, e mudá-los mudaria toda identidade já emitida;
    - `ensure_ascii` — fixa o escape de não-ASCII, para não depender de locale;
    - `allow_nan=False` — `NaN`/`Infinity` não são JSON válido e produziriam um
      documento que outro parser rejeitaria.
    """
    return json.dumps(
        documento,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
