"""Static, offline safety contract for the temporary monthly schedule hold.

Does not execute an Action, contact RFB/IA, or grant production permission.
"""

from copy import deepcopy
from pathlib import Path
import unittest

import yaml


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/etl-monthly.yml"
DISPATCH_ONLY = "github.event_name == 'workflow_dispatch'"


def _load_workflow():
    # BaseLoader preserves the YAML key `on` (which SafeLoader treats as a bool in YAML 1.1).
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _blocks_schedule(document):
    jobs = document["jobs"]
    if jobs["resolve"].get("if") != DISPATCH_ONLY:
        return False

    def depends_on_resolve(name, seen=frozenset()):
        if name == "resolve":
            return True
        if name not in jobs or name in seen:
            return False
        job = jobs[name]
        condition = str(job.get("if", "")).replace(" ", "")
        # Status functions on a downstream job can override the normal skip on failed needs.
        if any(f"{status}(" in condition for status in ("always", "failure", "cancelled")):
            return False
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        return bool(needs) and all(
            depends_on_resolve(dep, seen | {name}) for dep in needs
        )

    return all(depends_on_resolve(name) for name in jobs)


class MonthlyScheduleHoldTests(unittest.TestCase):
    def setUp(self):
        self.workflow = _load_workflow()

    def test_full_yaml_and_triggers(self):
        self.assertEqual(self.workflow["name"], "ETL — monthly snapshot")
        self.assertEqual(self.workflow["on"]["schedule"], [{"cron": "0 3 5,10,15,20 * *"}])
        self.assertIn("workflow_dispatch", self.workflow["on"])

    def test_lock_and_manual_route_unchanged(self):
        self.assertEqual(self.workflow["concurrency"]["group"], "ficha-monthly-publication")
        self.assertEqual(self.workflow["concurrency"]["cancel-in-progress"], "false")
        self.assertIn("skip_upload", self.workflow["on"]["workflow_dispatch"]["inputs"])

    def test_schedule_jobs_are_all_blocked_transitively(self):
        self.assertTrue(_blocks_schedule(self.workflow))
        self.assertTrue(
            {"produce", "inputs-visible", "shards", "finalize"}.issubset(self.workflow["jobs"])
        )

    def test_negative_missing_or_inverted_guard(self):
        for bad in (None, "github.event_name == 'schedule'", "github.event_name != 'schedule'"):
            with self.subTest(bad=bad):
                mutant = deepcopy(self.workflow)
                if bad is None:
                    mutant["jobs"]["resolve"].pop("if")
                else:
                    mutant["jobs"]["resolve"]["if"] = bad
                self.assertFalse(_blocks_schedule(mutant))

    def test_negative_missing_dependency_or_status_override(self):
        for job in ("produce", "shards", "finalize"):
            with self.subTest(job=job):
                mutant = deepcopy(self.workflow)
                mutant["jobs"][job].pop("needs", None)
                self.assertFalse(_blocks_schedule(mutant))
        mutant = deepcopy(self.workflow)
        mutant["jobs"]["finalize"]["if"] = "always()"
        self.assertFalse(_blocks_schedule(mutant))


if __name__ == "__main__":
    unittest.main()
