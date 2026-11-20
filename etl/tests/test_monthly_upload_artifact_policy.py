"""Contract checks for upload-artifact hidden source paths and promotion evidence."""

from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/etl-monthly.yml"


def _assert_artifact_policy(source: str) -> None:
    jobs = yaml.safe_load(source)["jobs"]
    uploads = []
    for job in jobs.values():
        for step in job.get("steps", []):
            if str(step.get("uses", "")).startswith("actions/upload-artifact@"):
                uploads.append(step)

    assert uploads, "monthly workflow must preserve producer evidence"
    required = {
        "Persist mandatory source-bag attestation",
        "Persist production descriptor before upload",
        "Persist exact promoted candidate and evidence",
    }
    assert required <= {step.get("name") for step in uploads}

    prefixes = ("etl/.cache/", "etl/.publication/", "web/public/manifest.json")
    for step in uploads:
        config = step["with"]
        assert config.get("include-hidden-files") is True
        paths = str(config["path"]).splitlines()
        assert paths
        for item in paths:
            path = item.strip()
            assert path.startswith(prefixes)
            assert path.endswith(".json")
            assert not any(x in path for x in ("*", "?", "!"))

    final = jobs["finalize"]["steps"]
    named = {step.get("name"): i for i, step in enumerate(final)}
    guard_name = "Assert all manifest candidate files exist and are nonempty"
    artifact_name = "Persist exact promoted candidate and evidence"
    publish_name = "Publish verified manifest"
    assert named[guard_name] < named[artifact_name] < named[publish_name]

    check = final[named[guard_name]]["run"]
    candidate = final[named[artifact_name]]
    paths = str(candidate["with"]["path"]).splitlines()
    for item in paths:
        path = item.strip().replace("${{ needs.resolve.outputs.month }}", "${MONTH}")
        assert f'"{path}"' in check
    assert "set -euo pipefail" in check
    assert "[ ! -s" in check


def test_monthly_uploads_preserve_explicit_hidden_evidence_and_all_candidate_paths() -> None:
    _assert_artifact_policy(WORKFLOW.read_text(encoding="utf-8"))


def test_missing_hidden_files_opt_in_is_rejected() -> None:
    original = WORKFLOW.read_text(encoding="utf-8")
    mutant = original.replace("include-hidden-files: true\n", "", 1)
    assert mutant != original
    with pytest.raises(AssertionError):
        _assert_artifact_policy(mutant)
