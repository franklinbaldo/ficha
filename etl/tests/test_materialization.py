"""Propriedades metamórficas do `materialization_id` (#147).

A identidade lógica de materialização decide se um shard remoto pode ser
reutilizado **sem regerar**. Um falso "igual" publica um shard que não
corresponde aos inputs; um falso "diferente" joga fora trabalho durável. As
duas falhas são caras, então as propriedades são fixadas aqui como testes, não
como intenção no docstring.

A forma dos testes é metamórfica: em vez de fixar hashes literais — que
mudariam a cada ajuste do documento e não provariam nada sobre o mecanismo —
cada teste altera **uma** coisa e verifica se o id deveria ou não mudar.
"""

from __future__ import annotations

import json

import pytest

from ficha_etl.materialization import (
    MaterializationSpec,
    ShardRange,
    canonical_json,
)

INPUTS = {
    "cnpjs.parquet": "a" * 40,
    "raizes.parquet": "b" * 40,
    "socios.parquet": "c" * 40,
    "lookups/cnaes.parquet": "d" * 40,
    "lookups/motivos.parquet": "e" * 40,
    "lookups/municipios.parquet": "f" * 40,
    "lookups/naturezas.parquet": "0" * 40,
    "lookups/paises.parquet": "1" * 40,
    "lookups/qualificacoes.parquet": "2" * 40,
}


def spec(**over) -> MaterializationSpec:
    base = {
        "snapshot": "2026-05",
        "shard_range": ShardRange("07"),
        "inputs": dict(INPUTS),
        "descriptor_sha256": "9" * 64,
    }
    base.update(over)
    return MaterializationSpec(**base)


# --- o que NÃO pode influenciar a identidade --------------------------------


def test_a_ordem_das_chaves_do_dict_nao_muda_o_id():
    invertido = dict(reversed(list(INPUTS.items())))
    assert list(invertido) != list(INPUTS)
    assert spec(inputs=invertido).materialization_id() == spec().materialization_id()


def test_a_ordem_em_que_o_metadata_do_ia_chegou_nao_muda_o_id():
    """Os `sha1` vêm da lista `files` do item, cuja ordem não é contratada."""
    embaralhado = {k: INPUTS[k] for k in sorted(INPUTS, key=lambda s: s[::-1])}
    assert list(embaralhado) != list(INPUTS)
    assert spec(inputs=embaralhado).materialization_id() == spec().materialization_id()


def test_o_id_e_estavel_entre_chamadas():
    """Nada de relógio, contador ou fonte de entropia dentro do cálculo."""
    s = spec()
    assert len({s.materialization_id() for _ in range(5)}) == 1
    assert s.materialization_id() == spec().materialization_id()


def test_nada_de_path_hostname_runner_ou_relogio_no_documento():
    """Varredura do documento inteiro, não só das chaves de topo.

    Uma inclusão acidental de caminho local ou timestamp tornaria o id
    dependente de onde o pack rodou, e a retomada nunca reconheceria nada.
    """
    texto = canonical_json(spec().as_document()).decode()
    for proibido in ("/home", "/tmp", "C:\\", "runner", "hostname", "localhost"):
        assert proibido.lower() not in texto.lower(), proibido

    documento = spec().as_document()
    chaves: list[str] = []

    def coleta(no):
        if isinstance(no, dict):
            for k, v in no.items():
                chaves.append(k)
                coleta(v)
        elif isinstance(no, list):
            for v in no:
                coleta(v)

    coleta(documento)
    for suspeita in ("timestamp", "generated_at", "date", "time", "path", "host", "mtime"):
        assert not any(suspeita in c.lower() for c in chaves), suspeita


def test_detalhes_de_container_nao_entram_no_documento():
    """Fixa a decisão de classificação de #147.

    Se algum destes voltar a aparecer, a identidade passa a invalidar shards
    semanticamente válidos — por exemplo, uma atualização de zlib descartaria
    todos os shards já publicados.
    """
    documento = spec().as_document()
    texto = json.dumps(documento).lower()
    for container in ("zlib", "deflate", "compresslevel", "external_attr", "python", "zip_epoca"):
        assert container not in texto, container
    assert "schema" in documento
    assert set(documento["schema"]) == {"descriptor_sha256"}, (
        "_schema.proto não participa da serialização e não pode entrar na identidade"
    )


# --- o que PRECISA mudar a identidade ---------------------------------------


@pytest.mark.parametrize("arquivo", sorted(INPUTS))
def test_mudar_o_sha1_de_qualquer_input_muda_o_id(arquivo):
    """Inclui os lookups: o conteúdo deles é payload decodificável do shard."""
    outros = dict(INPUTS)
    outros[arquivo] = "z" * 40
    assert spec(inputs=outros).materialization_id() != spec().materialization_id()


def test_mudar_o_range_muda_o_id():
    assert spec(shard_range=ShardRange("08")).materialization_id() != spec().materialization_id()


def test_larguras_de_prefixo_diferentes_sao_identidades_diferentes():
    """`07` e `070` cobrem faixas diferentes — não podem colidir."""
    assert spec(shard_range=ShardRange("070")).materialization_id() != (
        spec(shard_range=ShardRange("07")).materialization_id()
    )


def test_mudar_o_tipo_de_range_muda_o_id():
    """Protege contra colisão se a geometria deixar de ser prefixo de cnpj_base."""
    outro = ShardRange("07", kind="outra_geometria")
    assert spec(shard_range=outro).materialization_id() != spec().materialization_id()


def test_mudar_a_competencia_muda_o_id():
    assert spec(snapshot="2026-04").materialization_id() != spec().materialization_id()


def test_mudar_o_packer_format_version_muda_o_id():
    assert spec(packer_format_version=2).materialization_id() != spec().materialization_id()


def test_mudar_o_descriptor_sha256_muda_o_id():
    assert spec(descriptor_sha256="8" * 64).materialization_id() != spec().materialization_id()


def test_remover_um_input_muda_o_id():
    """Um shard gerado sem um lookup não é o mesmo artefato."""
    faltando = {k: v for k, v in INPUTS.items() if k != "lookups/paises.parquet"}
    assert spec(inputs=faltando).materialization_id() != spec().materialization_id()


# --- canonicalização explícita ----------------------------------------------


def test_a_canonicalizacao_nao_depende_dos_defaults_de_json_dumps():
    """Se a estabilidade dependesse dos defaults, uma mudança na stdlib mudaria
    toda identidade já emitida sem ninguém alterar uma linha do Ficha."""
    documento = {"b": 1, "a": {"z": "ã", "y": 2}}
    saida = canonical_json(documento)
    assert saida == b'{"a":{"y":2,"z":"\\u00e3"},"b":1}'
    assert b", " not in saida, "separadores precisam ser sem espaço"
    assert saida.decode().index('"a"') < saida.decode().index('"b"'), "chaves ordenadas"


def test_a_canonicalizacao_rejeita_o_que_nao_e_json_valido():
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_documentos_iguais_com_ordens_diferentes_geram_os_mesmos_bytes():
    assert canonical_json({"a": 1, "b": 2}) == canonical_json({"b": 2, "a": 1})


# --- o payload do _meta.json ------------------------------------------------


def test_o_meta_payload_separa_materializacao_de_artefato():
    """A fronteira é o ponto do desenho: `materialization` responde reuse,
    `artifact` responde identidade dos bytes concretos e só existe depois da
    geração — por isso nasce nulo."""
    payload = spec().meta_payload()
    assert set(payload) == {"materialization", "artifact"}
    assert set(payload["materialization"]) == {"spec", "id"}
    assert payload["materialization"]["id"] == spec().materialization_id()
    assert payload["artifact"] is None


def test_o_meta_payload_guarda_os_componentes_para_diagnostico():
    """Sem os componentes, uma divergência só saberia dizer que o id mudou —
    não qual input mudou."""
    documento = spec().meta_payload()["materialization"]["spec"]
    assert set(documento["inputs"]) == set(INPUTS)
    assert documento["inputs"]["socios.parquet"] == {"sha1": INPUTS["socios.parquet"]}
    assert documento["snapshot"] == "2026-05"
    assert documento["range"] == {"kind": "cnpj_base_prefix", "value": "07"}
