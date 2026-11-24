"""Verify child pytest execution via structured JUnit results, not log substrings."""

from collections import Counter
from pathlib import Path
from xml.etree import ElementTree

import pytest


def require_replay(report: Path, cases: list[tuple[str, str, str]]) -> None:
    expected = Counter((Path(file).stem, node, status) for file, node, status in cases)
    if not report.is_file():
        pytest.fail("optimized replay did not produce JUnit XML")
    observed = Counter()
    for case in ElementTree.parse(report).iter("testcase"):
        module = case.attrib.get("classname", "").rsplit(".", 1)[-1]
        name = case.attrib.get("name", "")
        skipped = case.find("skipped")
        status = "PASSED"
        if case.find("failure") is not None or case.find("error") is not None:
            status = "FAILED"
        elif skipped is not None:
            status = "XFAIL" if skipped.get("type") == "pytest.xfail" else "SKIPPED"
        observed[module, name, status] += 1
    if observed != expected:
        pytest.fail(f"optimized replay expected {expected!r}, got {observed!r}")
