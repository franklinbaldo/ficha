"""Executable contract for monthly upload-artifact paths and candidate evidence."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/etl-monthly.yml"
MONTH_EXPR = "${{ needs.resolve.outputs.month }}"
PREFIX_EXPR = "${{ matrix.prefix }}"
MONTH = "2026-09"

# (job, step) -> (artifact name, exact paths, missing-file policy, step condition)
# Keep paths explicit; uploads of directories, traversal, and globs are not allowed.
EXPECTED_UPLOADS = {
    ("produce", "Preserve estabelecimento key-gate evidence"): (
        f"estabelecimento-key-gate-{MONTH_EXPR}",
        (
            f"etl/.cache/{MONTH_EXPR}/evidence/monthly-estabelecimento-key-gate.json",
            f"etl/.cache/{MONTH_EXPR}/evidence/monthly-estabelecimento-source-bag.json",
        ),
        "warn",
        "always()",
    ),
    ("produce", "Persist mandatory source-bag attestation"): (
        f"estabelecimento-source-bag-{MONTH_EXPR}",
        (f"etl/.cache/{MONTH_EXPR}/evidence/monthly-estabelecimento-source-bag.json",),
        "error",
        None,
    ),
    ("produce", "Persist production descriptor before upload"): (
        f"production-descriptor-{MONTH_EXPR}",
        (f"etl/.cache/{MONTH_EXPR}/production-descriptor.json",),
        "error",
        None,
    ),
    ("produce", "Upload transform metrics"): (
        f"transform-metrics-{MONTH_EXPR}",
        (f"etl/.cache/{MONTH_EXPR}/metrics/transform_metrics.json",),
        "warn",
        "always()",
    ),
    ("shards", "Persist shard production receipt before PUT"): (
        f"shard-receipt-{PREFIX_EXPR}-{MONTH_EXPR}",
        (f"etl/.publication/{MONTH_EXPR}/receipt-{PREFIX_EXPR}.json",),
        "error",
        None,
    ),
    ("shards", "Preserve submit result"): (
        f"shard-submit-{PREFIX_EXPR}-{MONTH_EXPR}",
        (f"etl/.publication/{MONTH_EXPR}/submit-{PREFIX_EXPR}.json",),
        "warn",
        "always()",
    ),
    ("finalize", "Persist exact promoted candidate and evidence"): (
        f"manifest-candidate-{MONTH_EXPR}",
        (
            "web/public/manifest.json",
            f"etl/.publication/{MONTH_EXPR}/production-descriptor.json",
            f"etl/.publication/{MONTH_EXPR}/source/monthly-estabelecimento-source-bag.json",
            f"etl/.publication/{MONTH_EXPR}/snapshot-entry.json",
            f"etl/.publication/{MONTH_EXPR}/verification.json",
        ),
        "error",
        None,
    ),
}
GUARD_NAME = "Assert all manifest candidate files exist and are nonempty"
CANDIDATE_NAME = "Persist exact promoted candidate and evidence"
PUBLISH_NAME = "Publish verified manifest"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _paths(value: object) -> tuple[str, ...]:
    _require(isinstance(value, str), "artifact paths must be literal strings")
    lines = tuple(line.strip() for line in value.splitlines())
    _require(bool(lines) and all(lines), "artifact paths must be nonempty")
    return lines


def _assert_guard_shell(run: str, candidate_paths: tuple[str, ...]) -> None:
    """Run the actual Bash step on 1 full tree and 10 failing trees, offline."""
    paths = tuple(path.replace(MONTH_EXPR, MONTH) for path in candidate_paths)
    _require(len(paths) == 5, "manifest candidate must have five evidence paths")
    _require("set -euo pipefail" in run, "shell must be fail-fast")

    for defect in (None, *[(kind, i) for kind in ("missing", "empty") for i in range(5)]):
        with tempfile.TemporaryDirectory(prefix="ficha-candidate-evidence-") as directory:
            root = Path(directory)
            workdir = root / "etl"
            workdir.mkdir()
            for i, path in enumerate(paths):
                dest = root / path
                dest.parent.mkdir(parents=True, exist_ok=True)
                if defect == ("missing", i):
                    continue
                dest.write_bytes(b"" if defect == ("empty", i) else b"evidence\n")

            env = {**os.environ, "MONTH": MONTH}
            result = subprocess.run(
                ["bash", "-c", run],
                cwd=workdir,
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if defect is None:
                _require(
                    result.returncode == 0,
                    f"complete candidate rejected (rc={result.returncode}): {result.stdout!r}; {result.stderr!r}",
                )
            else:
                _require(
                    result.returncode != 0,
                    f"guard accepted {defect} evidence, rc=0: {result.stdout}",
                )


def _assert_artifact_policy(source: str) -> None:
    jobs = yaml.safe_load(source)["jobs"]
    actual = {}
    for job_name, job in jobs.items():
        for step in job.get("steps", []):
            if str(step.get("uses", "")).startswith("actions/upload-artifact@"):
                key = (job_name, step.get("name"))
                _require(key not in actual, f"duplicate artifact upload {key}")
                actual[key] = step

    _require(set(actual) == set(EXPECTED_UPLOADS), "artifact inventory changed")
    for key, (name, paths, missing_policy, condition) in EXPECTED_UPLOADS.items():
        step = actual[key]
        config = step.get("with", {})
        _require(step.get("uses") == "actions/upload-artifact@v4", f"wrong action: {key}")
        _require(config.get("include-hidden-files") is True, f"hidden files disabled: {key}")
        _require(config.get("name") == name, f"artifact name changed: {key}")
        _require(_paths(config.get("path")) == paths, f"artifact path not allowlisted: {key}")
        _require(config.get("if-no-files-found") == missing_policy, f"missing-file policy: {key}")
        _require(step.get("if") == condition, f"conditional upload: {key}")
        _require("continue-on-error" not in step, f"failure suppressed: {key}")

    final = jobs["finalize"]["steps"]
    names = [step.get("name") for step in final]
    for name in (GUARD_NAME, CANDIDATE_NAME, PUBLISH_NAME):
        _require(names.count(name) == 1, f"missing or duplicated finalize step: {name}")
    _require(
        names.index(GUARD_NAME) < names.index(CANDIDATE_NAME) < names.index(PUBLISH_NAME),
        "candidate guard must precede artifact and promotion",
    )
    guard = final[names.index(GUARD_NAME)]
    _require(guard.get("shell") == "bash", "candidate guard must run in Bash")
    _require("if" not in guard, "candidate guard cannot be conditional")
    _require("continue-on-error" not in guard, "candidate guard must fail closed")
    _require(isinstance(guard.get("run"), str), "candidate guard needs an executable script")
    _assert_guard_shell(guard["run"], EXPECTED_UPLOADS[("finalize", CANDIDATE_NAME)][1])


def _replace_one(original: str, before: str, after: str) -> str:
    _require(original.count(before) == 1, f"mutation anchor changed: {before}")
    return original.replace(before, after, 1)


class TestMonthlyUploadArtifactPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_monthly_artifacts_and_executable_candidate_guard(self) -> None:
        _assert_artifact_policy(self.workflow)

    def test_five_previously_false_green_mutations_are_rejected(self) -> None:
        guard_header = f"      - name: {GUARD_NAME}\n        shell: bash\n"
        mutations = {
            "skipped_guard": (guard_header, guard_header + "        if: false\n"),
            "ignored_guard_error": (
                guard_header,
                guard_header + "        continue-on-error: true\n",
            ),
            "disabled_bash_check": ('if [ ! -s "$file" ]; then', "if false; then"),
            "path_traversal": (
                f"          path: etl/.cache/{MONTH_EXPR}/metrics/transform_metrics.json",
                f"          path: etl/.cache/{MONTH_EXPR}/../../.github/secrets.json",
            ),
            "character_class_glob": (
                f"          path: etl/.cache/{MONTH_EXPR}/metrics/transform_metrics.json",
                f"          path: etl/.cache/{MONTH_EXPR}/metrics/transform_metrics[ab].json",
            ),
        }
        for label, (before, after) in mutations.items():
            with self.subTest(mutant=label):
                mutant = _replace_one(self.workflow, before, after)
                with self.assertRaises(AssertionError):
                    _assert_artifact_policy(mutant)

    def test_required_upload_cannot_turn_into_warning(self) -> None:
        before = (
            "          name: production-descriptor-"
            + MONTH_EXPR
            + "\n"
            + ("          path: etl/.cache/" + MONTH_EXPR + "/production-descriptor.json\n")
            + "          if-no-files-found: error"
        )
        mutant = _replace_one(
            self.workflow,
            before,
            before.replace("if-no-files-found: error", "if-no-files-found: warn"),
        )
        with self.assertRaises(AssertionError):
            _assert_artifact_policy(mutant)


if __name__ == "__main__":
    unittest.main()
