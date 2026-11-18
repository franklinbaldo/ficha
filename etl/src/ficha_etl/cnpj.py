"""CNPJ identifiers are ASCII text, including RFB's alphanumeric format.

RFB: https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2026/julho/receita-federal-gera-o-primeiro-cnpj-em-formato-alfanumerico
"""

import re
from itertools import product

CNPJ_PATTERN = r"[A-Z0-9]{12}[0-9]{2}"
SHARD_BY = "cnpj_base_prefix_or_base36_mod_100_v1"
SHARD_RANGE_KIND = "cnpj_base_prefix_or_base36_mod_v1"
_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def root_text(value: int | str) -> str:
    if isinstance(value, bool):
        raise ValueError("cnpj_base must be 8 ASCII characters")
    text = f"{value:08d}" if isinstance(value, int) else value
    if not isinstance(text, str) or not re.fullmatch(r"[A-Z0-9]{8}", text):
        raise ValueError(f"cnpj_base must be 8 uppercase ASCII characters: {value!r}")
    return text


def shard_of(value: int | str, width: int = 2) -> str:
    prefix = root_text(value)[:width]
    # Preserve every historical numeric prefix. Map the remaining prefixes
    # deterministically into the same 100 (or experimental 1000) buckets.
    number = int(prefix) if prefix.isdigit() else int(prefix, 36) % (10**width)
    return f"{number:0{width}d}"


def source_prefixes(bucket: str) -> list[str]:
    width = len(bucket)
    return [
        prefix
        for parts in product(_ALPHABET, repeat=width)
        if shard_of((prefix := "".join(parts)) + "0" * (8 - width), width) == bucket
    ]
