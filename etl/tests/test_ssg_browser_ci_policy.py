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
    concurrency = workflow.get("concurrency")
    if concurrency != {
        "group": "ssg-browser-${{ github.event.pull_request.number || github.run_id }}",
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }:
        errors.append("superseded PR audits cancel; manual replays stay independent")
    if set(events) != {"pull_request", "workflow_dispatch"}:
        errors.append("PR-only/manual gate; no second browser audit on main push")
    paths = set((events.get("pull_request") or {}).get("paths") or [])
    required_paths = {
        "site/**",
        "web/public/manifest.json",
        "web/scripts/check-accessibility.mjs",
        "web/scripts/check-greenfield.mjs",
        "web/scripts/check-atomic-ssg.mjs",
        "web/src/generated/company.pb.js",
        "web/package.json",
        "web/bun.lock",
        ".github/workflows/visual-capture.yml",
    }
    if not required_paths.issubset(paths) or "web/**" in paths:
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
    if any(
        s.get("continue-on-error")
        or (s.get("if") is not None and s.get("name") != "Upload browser accessibility evidence")
        for s in steps
    ):
        errors.append("no failure-masking or conditional mandatory steps")
    try:
        sha_step = _step(steps, "Conferir SHA do checkout")
        sha_command = 'test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"'
        if sha_step.get("env", {}).get(
            "EXPECTED_SHA"
        ) != EXPECTED_SHA or sha_command not in sha_step.get("run", ""):
            errors.append("checkout SHA must actually be compared")
        install = _step(steps, "Install pinned SSG Rust and verify SHA-256")
        if "f6305109e9bec5895e9c90f97a5c569c74048b3d50823d40175e15ca99c50843" not in install.get(
            "run", ""
        ) or "sha256sum --check -" not in install.get("run", ""):
            errors.append("SSG v0.0.75 artifact must be digest pinned")
        build = _step(steps, "Build the published SSG artifact (not Astro)")
        cmd = build.get("run", "")
        commands = (
            "ssg check --config",
            "ssg build --config",
            "node scripts/finalize.mjs",
        )
        if build.get("working-directory") != "site" or not all(
            command in cmd for command in commands
        ):
            errors.append("must build and finalize the real SSG Pages artifact")
        serve = _step(steps, "Serve built Pages artifact locally")
        if "site/dist/ficha/." not in serve.get("run", "") or "web/dist" in serve.get("run", ""):
            errors.append("must serve site/dist, not historical Astro")
        browser = _step(steps, "Install Chromium")
        if "playwright install chromium" not in browser.get("run", ""):
            errors.append("Chromium installation required")
        audit = _step(steps, "Audit actual SSG home with Chromium, axe and keyboard")
        expected_audit_run = (
            "bun run scripts/check-accessibility.mjs "
            "http://127.0.0.1:4321/ficha/ ../accessibility-evidence"
        )
        if (
            audit.get("env", {}).get("FICHA_SITE_MODE") != "ssg"
            or audit.get("run", "") != expected_audit_run
        ):
            errors.append("real browser auditor must run in SSG mode")
        greenfield = _step(
            steps,
            "Exercise product pages in Chromium (manifest real, no Parquet fetch)",
        )
        expected_greenfield_run = (
            "node scripts/check-greenfield.mjs "
            "http://127.0.0.1:4321/ficha/ ../accessibility-evidence"
        )
        if (
            greenfield.get("working-directory") != "web"
            or greenfield.get("run", "") != expected_greenfield_run
        ):
            errors.append("greenfield Chromium smoke must execute and propagate failure")
        atomic = _step(
            steps,
            "Exercise numeric Protobuf SSG against public IA member endpoints",
        )
        expected_atomic_run = (
            "node scripts/check-atomic-ssg.mjs "
            "http://127.0.0.1:4321/ficha/ ../accessibility-evidence"
        )
        if atomic.get("working-directory") != "web" or atomic.get("run", "") != expected_atomic_run:
            errors.append("numeric Protobuf Chromium gate must propagate failure")
        upload = _step(steps, "Upload browser accessibility evidence")
        if (
            upload.get("if") != "always()"
            or upload.get("with", {}).get("if-no-files-found") != "error"
        ):
            errors.append("fail closed when evidence is unavailable")
    except StopIteration:
        errors.append("mandatory step missing")
    for token in (
        "siteMode === 'ssg'",
        "Pesquisa por CNPJ ainda indisponível",
        "axe.run(document",
        "keyboard.press('Tab')",
        "focused.border !== unfocused.border",
        "focused.outline !== unfocused?.outline",
        "await page.keyboard.press('Enter')",
        "nextTabInsideMain",
        "skipLinkResult && !skipLinkResult.passed",
    ):
        if token not in auditor:
            errors.append("auditor must recognize SSG and really check axe and Tab")
            break
    return errors


def test_ssg_gate_policy() -> None:
    assert violations(_load(), AUDITOR.read_text(encoding="utf-8")) == []


def test_ssg_templates_have_explicit_utf8_charset() -> None:
    for template_name in ("index.html", "page.html"):
        source = (ROOT / "site" / "templates" / template_name).read_text(encoding="utf-8")
        assert '<meta charset="utf-8">' in source
        assert "{{charset}}" not in source


def test_reject_mutations_that_restore_false_green() -> None:
    baseline = _load()
    auditor = AUDITOR.read_text(encoding="utf-8")
    variants = {}

    def mutated():
        return deepcopy(baseline)

    # One PR number groups successive commits; dispatch gets a unique run ID.
    # Any alternative must be reviewed for manual cancellation and collisions.
    w = mutated()
    w.pop("concurrency")
    variants["no concurrency guard"] = w
    for value in (True, False, "${{ github.event_name == 'workflow_dispatch' }}"):
        w = mutated()
        w["concurrency"]["cancel-in-progress"] = value
        variants[f"wrong cancellation policy: {value}"] = w
    for group in (
        "ssg-browser-${{ github.sha }}",
        "ssg-browser-${{ github.event.pull_request.number || github.ref }}",
        "${{ github.event.pull_request.number || github.run_id }}",
    ):
        w = mutated()
        w["concurrency"]["group"] = group
        variants[f"wrong run grouping: {group}"] = w

    w = mutated()
    w.get("on", w.get(True))["pull_request"]["paths"] = ["web/**"]
    variants["old astro paths"] = w
    # The browser auditor installs through web/package.json + web/bun.lock.
    # Their changes must not bypass the actual SSG audit.
    for missing in (
        "web/package.json",
        "web/bun.lock",
        "web/scripts/check-greenfield.mjs",
        "web/scripts/check-atomic-ssg.mjs",
        "web/src/generated/company.pb.js",
    ):
        w = mutated()
        w.get("on", w.get(True))["pull_request"]["paths"].remove(missing)
        variants[f"missing browser audit dependency: {missing}"] = w
    w = mutated()
    w.get("on", w.get(True))["push"] = {"branches": ["main"]}
    variants["duplicate browser on main"] = w
    w = mutated()
    w["jobs"]["capture"]["steps"][0]["with"]["ref"] = "main"
    variants["wrong checkout"] = w
    w = mutated()
    sha_check = _step(w["jobs"]["capture"]["steps"], "Conferir SHA do checkout")
    sha_check["run"] = 'echo "$(git rev-parse HEAD)"'
    variants["fake SHA check"] = w
    w = mutated()
    build = _step(w["jobs"]["capture"]["steps"], "Build the published SSG artifact (not Astro)")
    build["run"] = "echo ssg build"
    variants["fake SSG build"] = w
    w = mutated()
    serve = _step(w["jobs"]["capture"]["steps"], "Serve built Pages artifact locally")
    serve["run"] = "cp -a web/dist/. .capture-root/ficha/"
    variants["wrong artifact"] = w
    w = mutated()
    audit = _step(
        w["jobs"]["capture"]["steps"],
        "Audit actual SSG home with Chromium, axe and keyboard",
    )
    audit["env"]["FICHA_SITE_MODE"] = "astro"
    variants["astro mode on SSG"] = w
    w = mutated()
    audit = _step(
        w["jobs"]["capture"]["steps"],
        "Audit actual SSG home with Chromium, axe and keyboard",
    )
    audit["continue-on-error"] = True
    variants["masked failure"] = w
    for suffix in (" || true", " || echo ignored"):
        w = mutated()
        audit = _step(
            w["jobs"]["capture"]["steps"],
            "Audit actual SSG home with Chromium, axe and keyboard",
        )
        audit["run"] += suffix
        variants[f"auditor command tolerates failure: {suffix}"] = w
    w = mutated()
    _step(w["jobs"]["capture"]["steps"], "Install Chromium")["run"] = "echo chromium"
    variants["no real browser"] = w
    w = mutated()
    w["jobs"]["capture"]["steps"] = [
        step
        for step in w["jobs"]["capture"]["steps"]
        if step.get("name")
        != "Exercise product pages in Chromium (manifest real, no Parquet fetch)"
    ]
    variants["no product pages Chromium"] = w
    w = mutated()
    _step(
        w["jobs"]["capture"]["steps"],
        "Exercise product pages in Chromium (manifest real, no Parquet fetch)",
    )["run"] += " || true"
    variants["masked product pages Chromium"] = w

    w = mutated()
    w["jobs"]["capture"]["steps"] = [
        step
        for step in w["jobs"]["capture"]["steps"]
        if step.get("name") != "Exercise numeric Protobuf SSG against public IA member endpoints"
    ]
    variants["no numeric Protobuf Chromium"] = w
    for suffix in (" || true", " || echo ignored"):
        w = mutated()
        atomic = _step(
            w["jobs"]["capture"]["steps"],
            "Exercise numeric Protobuf SSG against public IA member endpoints",
        )
        atomic["run"] += suffix
        variants[f"masked numeric Protobuf Chromium: {suffix}"] = w

    # A conditionally skipped step can make a success-status browser gate misleading.
    for step_name in (
        "Build the published SSG artifact (not Astro)",
        "Audit actual SSG home with Chromium, axe and keyboard",
        "Exercise product pages in Chromium (manifest real, no Parquet fetch)",
        "Exercise numeric Protobuf SSG against public IA member endpoints",
    ):
        w = mutated()
        _step(w["jobs"]["capture"]["steps"], step_name)["if"] = "${{ github.event_name == 'push' }}"
        variants[f"conditionally skipped mandatory step: {step_name}"] = w

    for name, variant in variants.items():
        assert violations(variant, auditor), name

    broken = auditor.replace("siteMode === 'ssg'", "siteMode === 'astro'")
    assert violations(baseline, broken)

    # These two mutants previously escaped a Tab-count-only accessibility gate.
    static_border = auditor.replace("focused.border !== unfocused.border", "focused.border !== ''")
    assert violations(baseline, static_border)
    missing_skip_enter = auditor.replace(
        "await page.keyboard.press('Enter')", "await page.keyboard.press('Tab')"
    )
    assert violations(baseline, missing_skip_enter)
