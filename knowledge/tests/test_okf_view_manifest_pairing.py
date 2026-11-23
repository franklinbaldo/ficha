"""Read-only pairing audit for declared OKF view inputs in the public manifest.

This checks *manifest metadata*, not the bytes or institutional provenance of
the published Parquets. It is an experimental oracle for ADR 0027, not a new
consumer/ETL contract or permission to replace the canonical pipeline.
"""

import copy
import hashlib
import json
import re
import unittest
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "web/public/manifest.json"
VIEWS = ROOT / "web/src/generated/ficha-okf.views.ts"
RELATION = {"Cnpj": "cnpjs", "Raiz": "raizes", "Socio": "socios"}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _views():
    text = VIEWS.read_text(encoding="utf-8")
    marker = "export const okfConvenienceViews = "
    if text.count(marker) != 1:
        raise ValueError("Expected one generated OKF view list")
    items = json.loads(
        text.split(marker, 1)[1].split(" as const satisfies", 1)[0]
    )
    names = [view["name"] for view in items]
    if names != ["empresas_ativas", "socios_de_raizes_ativas"]:
        raise ValueError("Generated OKF view inventory changed; review this oracle")
    return {view["name"]: view for view in items}


def _binding(manifest, date, view):
    """Build an input-set identity; no remote I/O, no assertion of custody."""
    selected = [s for s in manifest["snapshots"] if s["date"] == date]
    if len(selected) != 1:
        raise ValueError("Expected exactly one snapshot for competence")
    snapshot = selected[0]
    version = snapshot.get("schema_version")
    if not isinstance(version, str) or not version:
        raise ValueError("Missing declared schema_version")

    inputs = {}
    for logical in view["inputs"]:
        if logical not in RELATION:
            raise ValueError(f"Unmapped OKF relation: {logical}")
        relation = RELATION[logical]
        file = snapshot["files"][relation]
        digest, size, url = file["sha256"], file["size"], file["url"]
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise ValueError(f"Invalid declared SHA-256: {relation}")
        if type(size) is not int or size < 1:
            raise ValueError(f"Invalid declared size: {relation}")
        if not isinstance(url, str) or urlparse(url).scheme != "https":
            raise ValueError(f"Invalid declared URL: {relation}")
        inputs[relation] = {"sha256": digest, "size": size, "url": url}

    document = {
        "competence": date,
        "schema_version": version,
        "view": view["name"],
        "inputs": inputs,
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


class ManifestOkfPairingTest(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.views = _views()

    def test_all_declared_snapshot_view_inputs_have_distinct_metadata_bindings(self):
        dates = [s["date"] for s in self.manifest["snapshots"]]
        self.assertGreaterEqual(len(dates), 2)
        self.assertEqual(len(dates), len(set(dates)))
        self.assertIn(self.manifest["current"], dates)
        for date in dates:
            for view in self.views.values():
                with self.subTest(date=date, view=view["name"]):
                    self.assertRegex(_binding(self.manifest, date, view), SHA256)

    def test_cross_competence_raiz_swap_changes_join_input_identity(self):
        join = self.views["socios_de_raizes_ativas"]
        date = self.manifest["current"]
        other = next(s["date"] for s in self.manifest["snapshots"] if s["date"] != date)
        original = _binding(self.manifest, date, join)
        swapped = copy.deepcopy(self.manifest)
        current_snap = next(s for s in swapped["snapshots"] if s["date"] == date)
        other_snap = next(s for s in swapped["snapshots"] if s["date"] == other)
        self.assertNotEqual(
            current_snap["files"]["raizes"]["sha256"],
            other_snap["files"]["raizes"]["sha256"],
            "A counterexample requires distinct declared inputs",
        )
        current_snap["files"]["raizes"] = copy.deepcopy(other_snap["files"]["raizes"])
        # All three entries remain syntactically valid; only their pairing
        # against the original declaration is different.
        self.assertNotEqual(_binding(swapped, date, join), original)
        # empresas_ativas reads only Cnpj, so changing Raiz cannot affect it.
        self.assertEqual(
            _binding(swapped, date, self.views["empresas_ativas"]),
            _binding(self.manifest, date, self.views["empresas_ativas"]),
        )

    def test_schema_version_is_part_of_input_identity_not_only_parquet_hash(self):
        date = self.manifest["current"]
        view = self.views["socios_de_raizes_ativas"]
        before = _binding(self.manifest, date, view)
        changed = copy.deepcopy(self.manifest)
        snapshot = next(s for s in changed["snapshots"] if s["date"] == date)
        snapshot["schema_version"] += "-hypothetical"
        self.assertNotEqual(_binding(changed, date, view), before)

    def test_self_consistent_forgery_cannot_authenticate_an_untrusted_manifest(self):
        # A recomputed identity is not a signature or a custodian's receipt:
        # without a separately trusted reference, a modified manifest still
        # computes its own internally consistent identity.
        date = self.manifest["current"]
        view = self.views["socios_de_raizes_ativas"]
        changed = copy.deepcopy(self.manifest)
        snap = next(s for s in changed["snapshots"] if s["date"] == date)
        snap["files"]["raizes"]["sha256"] = "f" * 64
        self.assertNotEqual(
            _binding(changed, date, view), _binding(self.manifest, date, view)
        )


    def test_self_consistent_same_size_rewrite_requires_prior_receipt(self):
        # Fully synthetic byte fixtures: neither payload is a real RFB Parquet.
        # A previously pinned receipt is modeled independently of the
        # mutable manifest; the test does not establish such a receipt exists.
        original = b"11111111,1\n"
        rewritten = b"11111111,2\n"
        self.assertEqual(len(original), len(rewritten))
        date = self.manifest["current"]
        view = self.views["socios_de_raizes_ativas"]
        candidate = copy.deepcopy(self.manifest)
        snapshot = next(s for s in candidate["snapshots"] if s["date"] == date)
        raiz = snapshot["files"]["raizes"]
        raiz["size"] = len(original)
        raiz["sha256"] = hashlib.sha256(original).hexdigest()

        prior_receipt = {
            "competence": date,
            "schema_version": snapshot["schema_version"],
            "url": raiz["url"],
            "size": len(original),
            "sha256": hashlib.sha256(original).hexdigest(),
        }

        def matches_prior_receipt(manifest, physical_bytes):
            snap = next(s for s in manifest["snapshots"] if s["date"] == date)
            entry = snap["files"]["raizes"]
            return (
                date == prior_receipt["competence"]
                and snap["schema_version"] == prior_receipt["schema_version"]
                and entry["url"] == prior_receipt["url"]
                and entry["size"] == prior_receipt["size"] == len(physical_bytes)
                and entry["sha256"]
                == prior_receipt["sha256"]
                == hashlib.sha256(physical_bytes).hexdigest()
            )

        self.assertTrue(matches_prior_receipt(candidate, original))
        before = _binding(candidate, date, view)

        forged = copy.deepcopy(candidate)
        forged_raiz = next(
            s for s in forged["snapshots"] if s["date"] == date
        )["files"]["raizes"]
        forged_raiz["sha256"] = hashlib.sha256(rewritten).hexdigest()
        # URL, schema version, competence and byte length stay unchanged.
        self.assertEqual(forged_raiz["size"], prior_receipt["size"])
        self.assertEqual(forged_raiz["url"], prior_receipt["url"])
        self.assertNotEqual(_binding(forged, date, view), before)
        self.assertRegex(_binding(forged, date, view), SHA256)
        self.assertEqual(forged_raiz["sha256"], hashlib.sha256(rewritten).hexdigest())
        self.assertFalse(matches_prior_receipt(forged, rewritten))


if __name__ == "__main__":
    unittest.main()
