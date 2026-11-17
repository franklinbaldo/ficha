"""Offline contract checks: synthetic GitHub Pages artifact, no network or deploy."""
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
import zipfile

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("pages_importer", HERE.parent / "scripts/import-pages-artifact.py")
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


def archive(entries):
    """Create nested ZIP/TAR fixture; entries map names to bytes or link markers."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, value in entries:
            info = tarfile.TarInfo(name)
            if value == "SYMLINK":
                info.type = tarfile.SYMTYPE
                info.linkname = "../../escape"
                tar.addfile(info)
            else:
                info.size = len(value)
                tar.addfile(info, io.BytesIO(value))
    dst = io.BytesIO()
    with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED) as zipfile_obj:
        zipfile_obj.writestr("artifact.tar", buf.getvalue())
    return dst.getvalue()


BASE = [("./index.html", b"<h1>Astro test</h1>"),
        ("./manifest.json", b'{"current":"TEST","snapshots":[]}'),
        ("./_astro/app.js", b"console.log('fixture')")]


class PagesArtifactTest(unittest.TestCase):
    def test_existing_astro_fixture_import_and_preserve_bytes(self):
        blob = archive(BASE)
        tree = mod.inspect_archive(blob)[1]
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "previous"
            receipt = mod.extract(blob, target, mod.digest(blob), tree)
            self.assertEqual(receipt["files"], 3)
            self.assertFalse(receipt["deployed_verified"])
            self.assertEqual((target / "manifest.json").read_bytes(), BASE[1][1])
            self.assertEqual((target / "_astro/app.js").read_bytes(), BASE[2][1])

    def test_fail_closed_on_zip_checksum_mismatch(self):
        blob = archive(BASE)
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, "ZIP divergente"):
                mod.extract(blob, Path(root) / "previous", "0" * 64, mod.inspect_archive(blob)[1])
            self.assertFalse((Path(root) / "previous").exists())

    def test_fail_closed_on_tree_digest_mismatch(self):
        blob = archive(BASE)
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, "árvore Astro divergente"):
                mod.extract(blob, Path(root) / "previous", mod.digest(blob), "0" * 64)
            self.assertFalse((Path(root) / "previous").exists())

    def test_reject_symlink(self):
        with self.assertRaisesRegex(ValueError, "link ou tipo proibido"):
            mod.inspect_archive(archive(BASE + [("./escape", "SYMLINK")]))

    def test_reject_traversal(self):
        with self.assertRaisesRegex(ValueError, "caminho TAR inseguro"):
            mod.inspect_archive(archive(BASE + [("../outside", b"bad")]))

    def test_reject_duplicate_entry(self):
        with self.assertRaisesRegex(ValueError, "duplicada"):
            mod.inspect_archive(archive(BASE + [("./index.html", b"different")]))

    def test_reject_missing_manifest(self):
        with self.assertRaisesRegex(ValueError, "manifesto RFB ausente"):
            mod.inspect_archive(archive([BASE[0]]))

    def test_reject_unrelated_webmanifest(self):
        with self.assertRaisesRegex(ValueError, "sem contrato RFB"):
            mod.inspect_archive(archive([BASE[0], ("./manifest.json", b'{"name":"SSG"}')]))

    def test_reject_overwrite_existing_output(self):
        blob = archive(BASE)
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "previous"
            target.mkdir()
            with self.assertRaisesRegex(ValueError, "novo e inexistente"):
                mod.extract(blob, target, mod.digest(blob), mod.inspect_archive(blob)[1])


if __name__ == "__main__":
    unittest.main()
