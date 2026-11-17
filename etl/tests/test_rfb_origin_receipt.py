"""Offline positive/negative contract checks for the 2026-08 RFB PROPFIND receipt.

No ZIP GET, no ETL, no Internet Archive request or production write.
"""

import copy
import json
import unittest
from pathlib import Path

from ficha_etl.rfb_origin_receipt import validate_origin_receipt
from ficha_etl.sources import canonical_inventory

FIXTURE = Path(__file__).parent / "fixtures" / "rfb" / "2026-08-propfind.json"


class TestRfbOriginReceipt(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.observed = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_real_month_inventory_metadata_only(self):
        receipt = self.observed
        validate_origin_receipt(receipt)
        self.assertEqual(receipt["month"], "2026-08")
        self.assertEqual(len(receipt["files"]), 37)
        self.assertEqual(
            {entry["name"] for entry in receipt["files"]},
            {spec.name for spec in canonical_inventory()},
        )
        self.assertEqual(receipt["total_size_bytes"], 7_692_070_350)
        self.assertFalse(receipt["content_hash_verified"])
        self.assertEqual(receipt["identity_level"], "metadata-only")
        self.assertTrue(all(row["size_bytes"] > 0 for row in receipt["files"]))
        self.assertTrue(
            all(row["ia_raw_name_expected"] == f"raw/{row['name']}" for row in receipt["files"])
        )

    def test_weak_opaque_etag_is_accepted_but_never_a_hash(self):
        receipt = copy.deepcopy(self.observed)
        receipt["files"][0]["etag_raw"] = 'W/"opaque-revision-3"'
        validate_origin_receipt(receipt)
        self.assertFalse(receipt["content_hash_verified"])

    def test_reject_missing_duplicate_extra_or_corrupt_source_metadata(self):
        cases = {
            "missing file": lambda r: r["files"].pop(),
            "duplicate file": lambda r: r["files"].__setitem__(0, dict(r["files"][1])),
            "unexpected file": lambda r: r["files"][0].__setitem__("name", "Extra.zip"),
            "zero bytes": lambda r: r["files"][0].__setitem__("size_bytes", 0),
            "boolean size": lambda r: r["files"][0].__setitem__("size_bytes", True),
            "empty etag": lambda r: r["files"][0].__setitem__("etag_raw", ""),
            "unquoted etag": lambda r: r["files"][0].__setitem__("etag_raw", "abc"),
            "wrong MIME": lambda r: r["files"][0].__setitem__("content_type", "text/html"),
            "wrong IA mapping": lambda r: r["files"][0].__setitem__(
                "ia_raw_name_expected", "other.zip"
            ),
            "pretend checksum": lambda r: r.__setitem__("content_hash_verified", True),
            "unknown hash state": lambda r: r.__setitem__("content_hash_verified", None),
            "mislabel etag hash": lambda r: r.__setitem__("etag_semantics", "md5"),
            "inflated byte sum": lambda r: r.__setitem__("total_size_bytes", 1),
            "different month": lambda r: r.__setitem__("month", "2026-09"),
            "wrong method": lambda r: r["source"].__setitem__("method", "GET"),
            "wrong HTTP status": lambda r: r["source"].__setitem__("http_status", 200),
            "wrong depth": lambda r: r["source"].__setitem__("depth", 0),
            "wrong source": lambda r: r["source"].__setitem__("url", "https://example.invalid"),
            "missing timestamp": lambda r: r["source"].__setitem__("observed_at_utc", ""),
            "missing server date": lambda r: r["source"].__setitem__("server_date", ""),
            "observation a day late": lambda r: r["source"].__setitem__(
                "observed_at_utc", "2026-11-17T21:37:51+00:00"
            ),
            "server date a day early": lambda r: r["source"].__setitem__(
                "server_date", "Sun, 15 Nov 2026 21:37:49 GMT"
            ),
            "observed non-UTC": lambda r: r["source"].__setitem__(
                "observed_at_utc", "2026-11-16T17:37:51-04:00"
            ),
            "extra digest claim": lambda r: r["files"][0].__setitem__("sha256", "0" * 64),
            "receipt fake digest": lambda r: r.__setitem__("sha256_verified", True),
            "source fake digest": lambda r: r["source"].__setitem__("sha256", "0" * 64),
        }
        for title, mutate in cases.items():
            with self.subTest(title=title):
                receipt = copy.deepcopy(self.observed)
                mutate(receipt)
                with self.assertRaises(ValueError):
                    validate_origin_receipt(receipt)


if __name__ == "__main__":
    unittest.main()
