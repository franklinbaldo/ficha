"""Require every CI job to test the exact source commit without retained credentials."""

from copy import deepcopy
from pathlib import Path
import unittest

import yaml

CI = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
EXPECTED = "${{ github.event.pull_request.head.sha || github.sha }}"
JOBS = ("web", "etl", "etl-windows", "ficha-py")


def assert_checkout_policy(workflow):
    assert set(JOBS) <= set(workflow["jobs"])
    for job in JOBS:
        steps = workflow["jobs"][job]["steps"]
        assert steps[0]["uses"] == "actions/checkout@v4", job
        assert steps[0]["with"] == {
            "ref": EXPECTED,
            "persist-credentials": False,
        }, job
        verify = steps[1]
        assert verify["name"] == "Verify exact CI checkout SHA", job
        assert verify["env"] == {"EXPECTED_SHA": EXPECTED}, job
        assert verify["run"] == 'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"', job
        assert "if" not in verify, job
        assert verify.get("continue-on-error") in (None, False), job


class CiHeadCheckoutTests(unittest.TestCase):
    def test_every_job_has_fail_closed_head_checkout(self):
        assert_checkout_policy(yaml.safe_load(CI.read_text(encoding="utf-8")))

    def test_guard_rejects_six_mutations_per_job(self):
        baseline = yaml.safe_load(CI.read_text(encoding="utf-8"))
        for job in JOBS:
            for kind in (
                "remove-ref",
                "merge-ref",
                "credentials",
                "skip-check",
                "suppress-error",
                "no-assert",
            ):
                mutated = deepcopy(baseline)
                checkout, verify = mutated["jobs"][job]["steps"][:2]
                if kind == "remove-ref":
                    del checkout["with"]["ref"]
                elif kind == "merge-ref":
                    checkout["with"]["ref"] = "${{ github.sha }}"
                elif kind == "credentials":
                    checkout["with"]["persist-credentials"] = True
                elif kind == "skip-check":
                    verify["if"] = "github.event_name == 'push'"
                elif kind == "suppress-error":
                    verify["continue-on-error"] = True
                else:
                    verify["run"] += " || true"
                with self.subTest(job=job, kind=kind):
                    with self.assertRaises((AssertionError, KeyError)):
                        assert_checkout_policy(mutated)
