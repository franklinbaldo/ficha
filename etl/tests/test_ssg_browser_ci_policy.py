"""Falsification tests for the selective SSG browser/axe/keyboard gate (#268).

The published site lives in site/, not web/. Keep the PR audit on the
exact head SHA, while the main Pages deploy stays a lightweight HTTP smoke.
This is a structural guard, not evidence that Chromium actually ran.
"""

from copy import deepcopy
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "visual-capture.yml"
AUDITOR = ROOT / "web" / "scripts" / "check-accessibility.mjs"
EXPECTED_SHA = "${{ github.event.pull_request.head.sha || github.sha }}"


def _load() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _step(steps: list[dict], name: str) -> dict:
    return next(s for s in steps if s.get("name") == name)


def violations(workflow: dict, auditor: str) -> list[str]:
    errors = []
    # YAML 1.1 PyYAML resolves 'on' to the boolean key True.
    events = workflow.get("on", workflow.get(True, {}))
    if set(events) != {"pull_request", "workflow_dispatch"}:
        errors.append("PR-only/manual gate; no second browser audit on main push")
    paths = set((events.get("pull_request") or {}).get("paths") or [])
    if not {
        "site/**",
        "web/public/manifest.json",
        "web/scripts/check-accessibility.mjs",
        ".github/workflows/visual-capture.yml",
    }.issubset(paths) or "web/**" in paths:
        errors.append("must follow published SSG sources, not all legacy Astro")
    jobs = workflow.get("jobs") or {}
    if set(jobs) != {"capture"}:
        errors.append("one capture job required")
        return errors
    job = jobs["capture"]
    if job.get("if") is not None or job.get("continue-on-error"):
        errors.append("capture cannot be skipped or tolerate failure")
    if job.get("runs-on") != "ubuntu-24.04":
        errors.append("pinned runner required")
    steps = job.get("steps") or []
    if not steps or not str(steps[0].get("uses", "")).startswith("actions/checkout@"):
        errors.append("first step must check out source")
        return errors
    checkout = steps[0].get("with") or {}
    if checkout.get("ref") != EXPECTED_SHA or checkout.get("persist-credentials") is not False:
        errors.append("checkout must pin PR head and discard credentials")
    if any(s.get("continue-on-error") or s.get("if") == "${{ false }}" for s in steps):
        errors.append("no failure-masking or disabled steps")
    try:
        sha_step = _step(steps, "Conferir SHA do checkout")
        if sha_step.get("env", {}).get("EXPECTED_SHA") != EXPECTED_SHA or (
            'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"' not in sha_step.get("run", "")
        ):
            errors.append("checkout SHA must actually be compared")
        install = _step(steps, "Install pinned SSG Rust and verify SHA-256")
        if "f6305109e9bec5895e9c90f97a5c569c74048b3d50823d40175e15ca99c50843" not in install.get("run", "") or "sha256sum --check -" not in install.get("run", ""):
            errors.append("SSG v0.0.75 artifact must be digest pinned")
        build = _step(steps, "Build the published SSG artifact (not Astro)")
        cmd = build.get("run", "")
        if build.get("working-directory") != "site" or not all(
            x in cmd for x in ("ssg check --config", "ssg build --config", "node scripts/finalize.mjs")
        ):
            errors.append("must build and finalize the real SSG Pages artifact")
        serve = _step(steps, "Serve built Pages artifact locally")
        if "site/dist/ficha/." not in serve.get("run", "") or "web/dist" in serve.get("run", ""):
            errors.append("must serve site/dist, not historical Astro")
        browser = _step(steps, "Install Chromium")
        if "playwright install chromium" not in browser.get("run", ""):
            errors.append("Chromium installation required")
        audit = _step(steps, "Audit actual SSG home with Chromium, axe and keyboard")
        if audit.get("env", {}).get("FICHA_SITE_MODE") != "ssg" or not audit.get("run", "").startswith(
            "bun run scripts/check-accessibility.mjs "
        ):
            errors.append("real browser auditor must run in SSG mode")
        upload = _step(steps, "Upload browser accessibility evidence")
        if upload.get("if") != "always()" or upload.get("with", {}).get("if-no-files-found") != "error":
            errors.append("fail closed when evidence is unavailable")
    except StopIteration:
        errors.append("mandatory step missing")
    if "siteMode === 'ssg'" not in auditor or "Pesquisa por CNPJ ainda indisponível" not in auditor or "axe.run(document" not in auditor or "keyboard.press('Tab')" not in auditor:
        errors.append("auditor must recognize SSG and really check axe and Tab")
    return errors


def test_ssg_gate_policy() -> None:
    assert violations(_load(), AUDITOR.read_text(encoding="utf-8")) == []


def test_reject_mutations_that_restore_false_green() -> None:
    baseline = _load()
    auditor = AUDITOR.read_text(encoding="utf-8")
    variants = {}

    def mutated():
        return deepcopy(baseline)

    w = mutated()
    w.get("on", w.get(True))["pull_request"]["paths"] = ["web/**"]
    variants["old astro paths"] = w
    w = mutated()
    w.get("on", w.get(True))["push"] = {"branches": ["main"]}
    variants["duplicate browser on main"] = w
    w = mutated()
    w["jobs"]["capture"]["steps"][0]["with"]["ref"] = "main"
    variants["wrong checkout"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Conferir SHA do checkout")["run"] = 'echo "$(git rev-parse HEAD)"'
    variants["fake SHA check"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Build the published SSG artifact (not Astro)")["run"] = "echo ssg build"
    variants["fake SSG build"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Serve built Pages artifact locally")["run"] = "cp -a web/dist/. .capture-root/ficha/"
    variants["wrong artifact"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Audit actual SSG home with Chromium, axe and keyboard")["env"]["FICHA_SITE_MODE"] = "astro"
    variants["astro mode on SSG"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Audit actual SSG home with Chromium, axe and keyboard")["continue-on-error"] = True
    variants["masked failure"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Install Chromium")["run"] = "echo chromium"
    variants["no real browser"] = w

    for name, variant in variants.items():
        assert violations(variant, auditor), name

    assert violations(baseline, auditor.replace("siteMode === 'ssg'", "siteMode === 'astro'"))
