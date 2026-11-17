"""Offline P1-C tests: no ZIP download, IA write, or network request."""

from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from ficha_etl.ia_raw_receipt import check_raw_head, reconcile_raw_metadata
from ficha_etl.sources import canonical_inventory


def fixtures() -> tuple[dict, dict]:
    names = [s.name for s in canonical_inventory()]
    origin = {
        "schema_version": "rfb-propfind-metadata-v1",
        "month": "2026-08",
        "source": {
            "url": "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-08/",
            "method": "PROPFIND",
            "depth": 1,
            "http_status": 207,
            "server_date": "Mon, 16 Nov 2026 21:37:49 GMT",
            "observed_at_utc": "2026-11-16T21:37:51+00:00",
        },
        "identity_level": "metadata-only",
        "content_hash_verified": False,
        "etag_semantics": "opaque-validator-not-content-hash",
        "total_size_bytes": len(names) * 100,
        "files": [
            {
                "name": name,
                "size_bytes": 100,
                "etag_raw": '"opaque-etag"',
                "content_type": "application/zip",
                "ia_raw_name_expected": f"raw/{name}",
            }
            for name in names
        ],
    }
    archive = {
        "metadata": {"identifier": "ficha-2026-08"},
        "files": [{"name": f"raw/{name}", "size": "100", "sha1": "a" * 40} for name in names]
        + [{"name": "metadata.xml", "size": "1000"}],
    }
    return origin, archive


class RawReconciliationTests(unittest.TestCase):
    def test_matching_metadata_does_not_attest_byte_identity(self):
        origin, archive = fixtures()
        report = reconcile_raw_metadata(origin, archive)
        self.assertEqual(len(report.files), 37)
        self.assertTrue(report.metadata_consistent)
        self.assertFalse(report.content_identity_verified)
        self.assertEqual({f.status for f in report.files}, {"size_and_sha1_observed"})
        self.assertEqual({f.remote_sha1 for f in report.files}, {"a" * 40})

    def test_size_mismatch_is_not_identical(self):
        origin, archive = fixtures()
        archive["files"][0]["size"] = "99"
        report = reconcile_raw_metadata(origin, archive)
        self.assertFalse(report.metadata_consistent)
        self.assertIn("size_mismatch", {f.status for f in report.files})

    def test_missing_file_is_not_identical(self):
        origin, archive = fixtures()
        archive["files"].pop(0)
        report = reconcile_raw_metadata(origin, archive)
        self.assertFalse(report.metadata_consistent)
        self.assertIn("missing", {f.status for f in report.files})

    def test_missing_or_invalid_sha1_never_proves_identity(self):
        for sha in (None, "", "b" * 39, "not-a-sha1"):
            with self.subTest(sha=sha):
                origin, archive = fixtures()
                archive["files"][0]["sha1"] = sha
                report = reconcile_raw_metadata(origin, archive)
                self.assertFalse(report.metadata_consistent)
                self.assertIn("incomplete_metadata", {f.status for f in report.files})

    def test_invalid_size_is_incomplete(self):
        for size in ("", "0", "-3", "12.2", True, None):
            with self.subTest(size=size):
                origin, archive = fixtures()
                archive["files"][0]["size"] = size
                report = reconcile_raw_metadata(origin, archive)
                self.assertFalse(report.metadata_consistent)
                self.assertIn("incomplete_metadata", {f.status for f in report.files})

    def test_extra_raw_file_is_not_a_clean_inventory(self):
        origin, archive = fixtures()
        archive["files"].append({"name": "raw/other.zip", "size": "50", "sha1": "b" * 40})
        report = reconcile_raw_metadata(origin, archive)
        self.assertFalse(report.metadata_consistent)
        self.assertEqual(report.unexpected_raw_names, ("raw/other.zip",))

    def test_duplicate_ia_entry_fails_closed(self):
        origin, archive = fixtures()
        archive["files"].append(copy.copy(archive["files"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate IA"):
            reconcile_raw_metadata(origin, archive)

    def test_duplicate_or_missing_origin_fails_closed(self):
        origin, archive = fixtures()
        origin["files"][-1] = copy.copy(origin["files"][0])
        with self.assertRaisesRegex(ValueError, "unexpected or duplicated ZIP"):
            reconcile_raw_metadata(origin, archive)
        origin, archive = fixtures()
        origin["files"].pop()
        with self.assertRaisesRegex(ValueError, "must contain all 37"):
            reconcile_raw_metadata(origin, archive)

    def test_wrong_item_and_incomplete_archive_fail_closed(self):
        origin, archive = fixtures()
        archive["metadata"]["identifier"] = "ficha-2026-07"
        with self.assertRaisesRegex(ValueError, "wrong IA item"):
            reconcile_raw_metadata(origin, archive)
        origin, archive = fixtures()
        del archive["files"]
        with self.assertRaisesRegex(ValueError, "no usable"):
            reconcile_raw_metadata(origin, archive)

    def test_etag_is_not_a_digest_and_cannot_make_hash_verified(self):
        origin, archive = fixtures()
        origin["files"][0]["etag_raw"] = '"a"'
        report = reconcile_raw_metadata(origin, archive)
        self.assertFalse(report.content_identity_verified)
        origin["content_hash_verified"] = True
        with self.assertRaisesRegex(ValueError, "metadata-only receipt cannot attest"):
            reconcile_raw_metadata(origin, archive)

    def test_disagreeing_origin_total_fails_closed(self):
        origin, archive = fixtures()
        origin["total_size_bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "total byte count contradicts"):
            reconcile_raw_metadata(origin, archive)

    def test_head_size_only_does_not_attest_remote_content(self):
        origin, archive = fixtures()
        report = reconcile_raw_metadata(origin, archive)
        observed = check_raw_head(report, "raw/Cnaes.zip", http_status=200, content_length="100")
        self.assertTrue(observed.head_size_consistent)
        self.assertEqual(observed.catalog_size, 100)
        self.assertEqual(observed.head_size, 100)
        self.assertFalse(observed.content_identity_verified)
        self.assertFalse(report.content_identity_verified)

    def test_head_non_200_is_not_a_size_attestation(self):
        origin, archive = fixtures()
        report = reconcile_raw_metadata(origin, archive)
        for status in (True, None, 206, 301, 302, 304, 403, 404, 500):
            with self.subTest(status=status):
                observation = check_raw_head(
                    report, "raw/Cnaes.zip", http_status=status, content_length="100"
                )
                self.assertEqual(observation.status, "http_not_ok")
                self.assertFalse(observation.head_size_consistent)

    def test_head_missing_invalid_or_disagreeing_length(self):
        origin, archive = fixtures()
        report = reconcile_raw_metadata(origin, archive)
        for length in (None, True, "", "0", "-1", "1.2", " 100", "１２３"):
            with self.subTest(length=length):
                observation = check_raw_head(
                    report, "raw/Cnaes.zip", http_status=200, content_length=length
                )
                self.assertEqual(observation.status, "missing_or_invalid_length")
        observation = check_raw_head(report, "raw/Cnaes.zip", http_status=200, content_length="101")
        self.assertEqual(observation.status, "head_size_mismatch")
        self.assertFalse(observation.head_size_consistent)

    def test_head_encoding_and_target_must_be_unambiguous(self):
        origin, archive = fixtures()
        report = reconcile_raw_metadata(origin, archive)
        for encoding in ("gzip", "br", ""):
            with self.subTest(encoding=encoding):
                observation = check_raw_head(
                    report,
                    "raw/Cnaes.zip",
                    http_status=200,
                    content_length="100",
                    content_encoding=encoding,
                )
                self.assertEqual(observation.status, "unsupported_encoding")
        self.assertTrue(
            check_raw_head(
                report,
                "raw/Cnaes.zip",
                http_status=200,
                content_length="100",
                content_encoding="Identity",
            ).head_size_consistent
        )
        with self.assertRaisesRegex(ValueError, "not in the canonical"):
            check_raw_head(report, "raw/other.zip", http_status=200, content_length="100")

    def test_head_cannot_rescue_partial_or_extra_ia_inventory(self):
        for mutation in ("missing_other_file", "unexpected_raw_file"):
            with self.subTest(mutation=mutation):
                origin, archive = fixtures()
                if mutation == "missing_other_file":
                    archive["files"] = [
                        file for file in archive["files"] if file["name"] != "raw/Empresas0.zip"
                    ]
                else:
                    archive["files"].append(
                        {"name": "raw/rogue.zip", "size": "100", "sha1": "a" * 40}
                    )
                report = reconcile_raw_metadata(origin, archive)
                self.assertFalse(report.metadata_consistent)
                observation = check_raw_head(
                    report, "raw/Cnaes.zip", http_status=200, content_length="100"
                )
                self.assertEqual(observation.status, "catalog_incomplete")
                self.assertFalse(observation.head_size_consistent)
                self.assertFalse(observation.content_identity_verified)

    def test_head_cannot_rescue_invalid_ia_catalog(self):
        origin, archive = fixtures()
        archive["files"][0]["sha1"] = None
        report = reconcile_raw_metadata(origin, archive)
        observation = check_raw_head(
            report, archive["files"][0]["name"], http_status=200, content_length="100"
        )
        self.assertEqual(observation.status, "catalog_incomplete")
        self.assertFalse(observation.head_size_consistent)

    def test_real_rfb_receipt_fixture_reaches_ia_without_byte_identity(self):
        fixture = Path(__file__).parent / "fixtures" / "rfb" / "2026-08-propfind.json"
        origin = json.loads(fixture.read_text(encoding="utf-8"))
        archive = {
            "metadata": {"identifier": "ficha-2026-08"},
            "files": [
                {
                    "name": file["ia_raw_name_expected"],
                    "size": str(file["size_bytes"]),
                    "sha1": "a" * 40,
                }
                for file in origin["files"]
            ],
        }
        report = reconcile_raw_metadata(origin, archive)
        self.assertEqual(len(report.files), 37)
        self.assertTrue(report.metadata_consistent)
        self.assertFalse(report.content_identity_verified)

    def test_reject_rfb_claims_not_covered_by_old_reconciler(self):
        cases = {
            "missing server date": lambda r: r["source"].pop("server_date"),
            "stale observation": lambda r: r["source"].__setitem__(
                "observed_at_utc", "2026-11-17T21:37:51+00:00"
            ),
            "invalid etag": lambda r: r["files"][0].__setitem__(
                "etag_raw", '"bad' + chr(0) + 'etag"'
            ),
            "extra unverified hash claim": lambda r: r.__setitem__("sha256_verified", True),
            "extra file hash claim": lambda r: r["files"][0].__setitem__("sha256", "0" * 64),
            "boolean HTTP status": lambda r: r["source"].__setitem__("http_status", True),
        }
        for title, mutate in cases.items():
            with self.subTest(title=title):
                origin, archive = fixtures()
                mutate(origin)
                with self.assertRaises(ValueError):
                    reconcile_raw_metadata(origin, archive)


if __name__ == "__main__":
    unittest.main()
