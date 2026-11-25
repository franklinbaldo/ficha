"""Offline controls for the experimental address comparison."""

import csv
import hashlib
import io
import re
import unicodedata
from collections import Counter

PREFIX = {
    "R": "RUA", "AV": "AVENIDA", "TV": "TRAVESSA", "AL": "ALAMEDA",
    "PCA": "PRACA", "PC": "PRACA", "EST": "ESTRADA", "ROD": "RODOVIA",
    "VL": "VILA", "LG": "LARGO",
}
KNOWN = set(PREFIX) | set(PREFIX.values())
REQUIRED = {"tipo_logradouro", "logradouro", "numero", "cep", "bairro", "municipio", "uf"}


def folding(value):
    value = unicodedata.normalize("NFKD", str(value or "").strip())
    return re.sub(r"\s+", " ", "".join(
        c for c in value if not unicodedata.combining(c)
    ).upper()).strip()


def compose(row):
    """Share only prefix selection, not lexical normalization, with Ipea."""
    street = row["logradouro"].strip()
    kind = (row.get("tipo_logradouro") or "").strip()
    first = re.match(r"^([A-Z]+)\.?\s+", folding(street))
    if first and first.group(1) in KNOWN:
        explicit = first.group(1)
        separate = folding(kind).rstrip(".")
        conflict = bool(separate and PREFIX.get(separate, separate) != PREFIX.get(explicit, explicit))
        return street, conflict
    return f"{kind} {street}".strip(), False


def ficha_key(row):
    street = folding(compose(row)[0])
    match = re.match(r"^([A-Z]+)\.?\s+(.*)$", street)
    if match and match.group(1) in PREFIX:
        return f"{PREFIX[match.group(1)]} {match.group(2)}"
    return street


def read_cohort(path):
    raw = path.read_bytes()
    reader = csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True)
    header = next(reader, [])
    if len(set(header)) != len(header):
        raise ValueError("Duplicate CSV columns")
    missing = REQUIRED - set(header)
    if missing:
        raise ValueError(f"Missing CSV columns: {', '.join(sorted(missing))}")
    records = []
    for values in reader:
        if not values:  # Match DictReader's treatment of blank physical lines.
            continue
        if len(values) != len(header):
            raise ValueError(f"Invalid CSV width at physical line {reader.line_num}")
        row = dict(zip(header, values))
        # Exact predicate of the writer, not a whitespace-trimmed approximation.
        writer = row["logradouro"] != "" and row["uf"] != ""
        query = writer and all(row[k].strip() for k in ("logradouro", "uf", "municipio"))
        street, conflict = compose(row)
        index = len(records)
        records.append({
            "row": index, "row_id": f"row-{index}", "id": row.get("id", str(index)),
            "original": row["logradouro"], "original_fields": row,
            "ficha_normalizado": ficha_key(row), "query_street": street,
            "prefix_conflict": conflict, "writer_eligible": writer, "query_eligible": query,
        })
    if not records:
        raise ValueError("Empty CSV")
    groups = Counter(
        (folding(r["original_fields"]["uf"]), folding(r["original_fields"]["municipio"]),
         r["ficha_normalizado"])
        for r in records if r["query_eligible"]
    )
    return {
        "input_sha256": hashlib.sha256(raw).hexdigest(),
        "input_rows": len(records), "valid_rows": len(records),
        "writer_eligible_rows": sum(r["writer_eligible"] for r in records),
        "query_eligible_rows": sum(r["query_eligible"] for r in records),
        "queried_rows": 0, "prefix_conflict_rows": sum(r["prefix_conflict"] for r in records),
        "unique_street_groups": len(groups),
        "colliding_street_groups": sum(value > 1 for value in groups.values()),
        "geocode_status": "not_run", "records": records,
    }


def reconcile_candidates(row_ids, pairs):
    """Count distinct, already validated candidate IDs per input ID.

    This is a synthetic oracle, not a decoder for the geocodebr response.
    """
    row_ids = list(row_ids)
    if len(set(row_ids)) != len(row_ids):
        raise ValueError("Duplicate input row ID")
    candidates = {key: set() for key in row_ids}
    for key, candidate in pairs:
        if key not in candidates:
            raise ValueError("Unknown output row ID")
        if candidate is not None:
            if not isinstance(candidate, str) or not candidate.strip():
                raise ValueError("Invalid candidate ID")
            candidates[key].add(candidate)
    status = {
        key: "unmatched" if not values else "unique" if len(values) == 1 else "ambiguous"
        for key, values in candidates.items()
    }
    return {"status": status, "counts": {s: list(status.values()).count(s)
            for s in ("unmatched", "unique", "ambiguous")}}
