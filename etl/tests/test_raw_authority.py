"""Disk-backed U-local tests; no uploader, network or external credentials."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from ficha_etl.raw_authority import AuthorityRefused, LocalAuthority, Pin
from ficha_etl.sources import canonical_inventory

MONTH = "2026-11"
DEST = "ficha-2026-11"
INTENT = "a" * 32
GENERATION = "b" * 32
PINS = tuple(Pin(spec.name, "c" * 64, 10) for spec in canonical_inventory())
ARGS = (MONTH, DEST, INTENT, GENERATION, PINS)


@pytest.fixture
def journal(tmp_path):
    path = tmp_path / "authority.sqlite"
    authority = LocalAuthority.create(path)
    authority.reserve(*ARGS)
    return path, authority


def rows(path, sql):
    with closing(sqlite3.connect(path)) as observer:
        return observer.execute(sql).fetchall()


def test_committed_before_callback_and_late_receipt(journal):
    path, authority = journal
    observed = []

    def effect(claim):
        observed.append(claim)
        assert rows(path, "SELECT name,token,state FROM claim") == [
            (claim.pin.name, claim.token, "UNKNOWN")
        ]

    a = authority.perform_for_test(*ARGS, PINS[0].name, effect)
    with pytest.raises(AuthorityRefused):
        authority.perform_for_test(*ARGS, PINS[1].name, observed.append)
    for mutated in (
        replace(a, month="2026-12"),
        replace(a, destination="other"),
        replace(a, intent="d" * 32),
        replace(a, generation="e" * 32),
        replace(a, pin=replace(a.pin, sha256="d" * 64)),
        replace(a, pin=replace(a.pin, size=11)),
        replace(a, pin=PINS[1]),
        replace(a, token="f" * 32),
    ):
        with pytest.raises(AuthorityRefused):
            authority.record_test_match(mutated, quiescent_and_matched=True)
    authority.record_test_match(a, quiescent_and_matched=True)
    b = authority.perform_for_test(*ARGS, PINS[1].name, observed.append)
    with pytest.raises(AuthorityRefused):
        authority.record_test_match(a, quiescent_and_matched=True)
    with pytest.raises(AuthorityRefused):
        authority.perform_for_test(*ARGS, PINS[2].name, observed.append)
    assert observed == [a, b]
    assert rows(path, "SELECT state FROM claim ORDER BY name") == [
        ("TEST_MATCHED",),
        ("UNKNOWN",),
    ]
    authority.record_test_match(b, quiescent_and_matched=True)
    with pytest.raises(AuthorityRefused):
        authority.perform_for_test(*ARGS, PINS[0].name, observed.append)
    assert observed == [a, b]


@pytest.mark.parametrize("month", ["２０２６-11", "٢٠٢٦-11", "2０26-11", "2026-13"])
def test_month_aliases(journal, month):
    path, authority = journal
    with pytest.raises(AuthorityRefused):
        authority.reserve(month, f"ficha-{month}", "d" * 32, GENERATION, PINS)
    assert rows(path, "SELECT month FROM intent") == [(MONTH,)]


@pytest.mark.parametrize(
    "pins",
    [
        PINS[:-1],
        (*PINS[:-1], PINS[0]),
        (replace(PINS[0], sha256="x" * 64), *PINS[1:]),
        (replace(PINS[0], size=True), *PINS[1:]),
    ],
)
def test_invalid_intent_atomic(tmp_path, pins):
    path = tmp_path / "empty.sqlite"
    authority = LocalAuthority.create(path)
    with pytest.raises(AuthorityRefused):
        authority.reserve(MONTH, DEST, INTENT, GENERATION, pins)
    assert rows(path, "SELECT * FROM intent") == []
    assert rows(path, "SELECT * FROM pin") == []


def test_rollback_on_insert_failure(tmp_path):
    path = tmp_path / "rollback.sqlite"
    authority = LocalAuthority.create(path)
    with sqlite3.connect(path) as db:
        db.execute(
            """CREATE TRIGGER fail_pin BEFORE INSERT ON pin
               WHEN NEW.name='Socios9.zip'
               BEGIN SELECT RAISE(ABORT,'injected'); END"""
        )
    with pytest.raises(AuthorityRefused):
        authority.reserve(*ARGS)
    assert rows(path, "SELECT * FROM intent") == []
    assert rows(path, "SELECT * FROM pin") == []


def test_reopen_and_timeout(journal):
    path, authority = journal
    seen = []

    def timeout(claim):
        seen.append(claim)
        raise TimeoutError("synthetic")

    with pytest.raises(TimeoutError):
        authority.perform_for_test(*ARGS, PINS[0].name, timeout)
    with pytest.raises(AuthorityRefused):
        LocalAuthority.reopen_for_test(path)
    with pytest.raises(AuthorityRefused):
        LocalAuthority.create(path)
    with pytest.raises(AuthorityRefused):
        LocalAuthority(path)
    recovered = LocalAuthority.reopen_for_test(path, current_unique_authority=True)
    for name in (PINS[0].name, PINS[1].name):
        with pytest.raises(AuthorityRefused):
            recovered.perform_for_test(*ARGS, name, seen.append)
    assert len(seen) == 1
    assert rows(path, "SELECT token,state FROM claim") == [(seen[0].token, "UNKNOWN")]
    other = ("2026-12", "ficha-2026-12", "d" * 32, GENERATION, PINS)
    recovered.reserve(*other)
    recovered.perform_for_test(*other, PINS[0].name, seen.append)
    assert len(seen) == 2
    assert rows(path, "SELECT count(*) FROM claim WHERE state='UNKNOWN'") == [(2,)]


def test_different_pins_refused_before_effect(journal):
    path, authority = journal
    seen = []
    changed = (replace(PINS[0], size=11), *PINS[1:])
    with pytest.raises(AuthorityRefused):
        authority.perform_for_test(
            MONTH, DEST, INTENT, GENERATION, changed, PINS[1].name, seen.append
        )
    assert seen == []
    assert rows(path, "SELECT * FROM claim") == []


def test_commit_failure_no_callback(journal):
    path, authority = journal
    seen = []
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE guard (id INTEGER PRIMARY KEY)")
        db.execute(
            """CREATE TABLE deferred (id INTEGER REFERENCES guard(id)
               DEFERRABLE INITIALLY DEFERRED)"""
        )
        db.execute(
            """CREATE TRIGGER fail_commit AFTER INSERT ON claim
               BEGIN INSERT INTO deferred VALUES (1); END"""
        )
    with pytest.raises(sqlite3.IntegrityError):
        authority.perform_for_test(*ARGS, PINS[0].name, seen.append)
    assert seen == []
    assert rows(path, "SELECT * FROM claim") == []


_WORKER = """
import os
import sys
import time
from pathlib import Path
from contextlib import contextmanager
from ficha_etl.raw_authority import AuthorityRefused, LocalAuthority, Pin
from ficha_etl.sources import canonical_inventory

path, gate, marker, name, crash = sys.argv[1:]
authority = LocalAuthority.reopen_for_test(Path(path), current_unique_authority=True)
pins = tuple(Pin(spec.name, 'c' * 64, 10) for spec in canonical_inventory())
Path(marker + '.ready').write_text(str(__debug__))
while not Path(gate).exists():
    time.sleep(0.01)

def effect(claim):
    with open(marker, 'x') as out:
        out.write(claim.pin.name + ' ' + claim.token)
    if crash == '1':
        os._exit(79)

if crash == '2':
    original = authority._transaction
    @contextmanager
    def die_after_commit():
        with original() as db:
            yield db
        os._exit(78)
    authority._transaction = die_after_commit

try:
    authority.perform_for_test('2026-11', 'ficha-2026-11', 'a'*32, 'b'*32,
                               pins, name, effect)
except AuthorityRefused:
    print('REFUSED', flush=True)
else:
    print('DONE', flush=True)
"""


@pytest.mark.parametrize("same_pin", [False, True])
@pytest.mark.parametrize("optimized", [False, True])
def test_process_race(journal, tmp_path, same_pin, optimized):
    path, _ = journal
    markers = [tmp_path / "effect-a", tmp_path / "effect-b"]
    gate = tmp_path / "gate"
    names = [PINS[0].name, PINS[0 if same_pin else 1].name]
    workers = []
    try:
        for marker, name in zip(markers, names, strict=True):
            command = [sys.executable, *(["-O"] if optimized else []), "-c", _WORKER]
            workers.append(
                subprocess.Popen(
                    [*command, str(path), str(gate), str(marker), name, "0"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        deadline = time.monotonic() + 20
        while not all(Path(str(m) + ".ready").exists() for m in markers):
            if time.monotonic() > deadline:
                pytest.fail("workers failed to reach admission barrier")
            time.sleep(0.01)
        gate.touch()
        results = [w.communicate(timeout=20) for w in workers]
        assert [w.returncode for w in workers] == [0, 0], results
        debug_values = [Path(str(m) + ".ready").read_text() for m in markers]
        assert debug_values == [str(not optimized)] * 2
        outcomes = [out.strip() for out, _ in results]
        assert sorted(outcomes) == ["DONE", "REFUSED"]
        winner = outcomes.index("DONE")
        assert markers[winner].exists()
        assert not markers[1 - winner].exists()
        name, token = markers[winner].read_text().split()
        assert name == names[winner]
        assert rows(path, "SELECT name,token,state FROM claim") == [(name, token, "UNKNOWN")]
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=10)


@pytest.mark.parametrize("crash", ["1", "2"])
def test_process_death_around_effect(journal, tmp_path, crash):
    path, _ = journal
    gate = tmp_path / "gate"
    gate.touch()
    marker = tmp_path / "effect"
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-c",
            _WORKER,
            str(path),
            str(gate),
            str(marker),
            PINS[0].name,
            crash,
        ],
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == (79 if crash == "1" else 78), result.stderr
    assert Path(str(marker) + ".ready").read_text() == "False"
    persisted = rows(path, "SELECT name,token,state FROM claim")
    assert len(persisted) == 1
    name, token, state = persisted[0]
    assert (name, state) == (PINS[0].name, "UNKNOWN")
    if crash == "1":
        assert marker.read_text().split() == [name, token]
    else:
        assert not marker.exists()
    authority = LocalAuthority.reopen_for_test(path, current_unique_authority=True)
    identity = authority.lookup_unknown_for_test(*ARGS)
    assert identity is not None
    assert (identity.pin.name, identity.token) == (name, token)
    seen = []
    for candidate in (PINS[0].name, PINS[1].name):
        with pytest.raises(AuthorityRefused):
            authority.perform_for_test(*ARGS, candidate, seen.append)
    assert seen == []
    assert authority.lookup_unknown_for_test(*ARGS) == identity
    # Independent synthetic attestation supplied by this fixture, not inferred
    # from presence or absence of the crash marker or from lookup alone.
    authority.record_test_match(identity, quiescent_and_matched=True)
    assert authority.lookup_unknown_for_test(*ARGS) is None
    next_claim = authority.perform_for_test(*ARGS, PINS[1].name, seen.append)
    assert seen == [next_claim]


def test_active_effect_without_attestation_cannot_resolve(journal):
    path, authority = journal
    seen = []

    def effect(claim):
        seen.append(claim)
        other = LocalAuthority.reopen_for_test(path, current_unique_authority=True)
        assert other.lookup_unknown_for_test(*ARGS) == claim
        with pytest.raises(TypeError):
            other.record_test_match(claim)
        for invalid in (False, 1, None, "yes"):
            with pytest.raises(AuthorityRefused):
                other.record_test_match(claim, quiescent_and_matched=invalid)
        with pytest.raises(AuthorityRefused):
            other.perform_for_test(*ARGS, PINS[1].name, seen.append)

    claim = authority.perform_for_test(*ARGS, PINS[0].name, effect)
    assert seen == [claim]
    assert authority.lookup_unknown_for_test(*ARGS) == claim
    authority.record_test_match(claim, quiescent_and_matched=True)
    assert authority.lookup_unknown_for_test(*ARGS) is None


def test_lookup_requires_full_identity_and_never_changes_state(journal):
    path, authority = journal
    assert authority.lookup_unknown_for_test(*ARGS) is None
    claim = authority.perform_for_test(*ARGS, PINS[0].name, lambda _: None)
    changed = (PINS[0], replace(PINS[1], size=11), *PINS[2:])
    for args in (
        (MONTH, DEST, "d" * 32, GENERATION, PINS),
        (MONTH, DEST, INTENT, "d" * 32, PINS),
        (MONTH, DEST, INTENT, GENERATION, changed),
        (MONTH, "other", INTENT, GENERATION, PINS),
    ):
        with pytest.raises(AuthorityRefused):
            authority.lookup_unknown_for_test(*args)
    assert authority.lookup_unknown_for_test(*ARGS) == claim
    assert authority.lookup_unknown_for_test(*ARGS) == claim
    assert rows(path, "SELECT name,token,state FROM claim") == [
        (claim.pin.name, claim.token, "UNKNOWN")
    ]


def test_resolution_commit_failure_retains_unknown(journal):
    path, authority = journal
    claim = authority.perform_for_test(*ARGS, PINS[0].name, lambda _: None)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE guard (id INTEGER PRIMARY KEY)")
        db.execute(
            """CREATE TABLE deferred (id INTEGER REFERENCES guard(id)
               DEFERRABLE INITIALLY DEFERRED)"""
        )
        db.execute(
            """CREATE TRIGGER fail_resolution AFTER UPDATE ON claim
               BEGIN INSERT INTO deferred VALUES (1); END"""
        )
    with pytest.raises(sqlite3.IntegrityError):
        authority.record_test_match(claim, quiescent_and_matched=True)
    authority = LocalAuthority.reopen_for_test(path, current_unique_authority=True)
    assert authority.lookup_unknown_for_test(*ARGS) == claim
    seen = []
    with pytest.raises(AuthorityRefused):
        authority.perform_for_test(*ARGS, PINS[1].name, seen.append)
    assert seen == []
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("DROP TRIGGER fail_resolution")
    authority.record_test_match(claim, quiescent_and_matched=True)
    assert rows(path, "SELECT token,state FROM claim") == [(claim.token, "TEST_MATCHED")]


@pytest.mark.parametrize("phase", ["BEGIN", "COMMIT"])
def test_busy_claim_never_calls_effect(journal, phase, monkeypatch):
    path, authority = journal
    trace = []
    original = authority._connection

    @contextmanager
    def traced_connection():
        with original() as db:
            db.execute("PRAGMA busy_timeout=0")
            db.set_trace_callback(trace.append)
            yield db

    monkeypatch.setattr(authority, "_connection", traced_connection)
    seen = []
    with closing(sqlite3.connect(path, isolation_level=None)) as locker:
        assert locker.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        if phase == "BEGIN":
            locker.execute("BEGIN IMMEDIATE")
        else:
            locker.execute("BEGIN")
            locker.execute("SELECT * FROM claim").fetchall()
        try:
            with pytest.raises(sqlite3.OperationalError) as error:
                authority.perform_for_test(*ARGS, PINS[0].name, seen.append)
            assert error.value.sqlite_errorcode == sqlite3.SQLITE_BUSY
            assert seen == []
        finally:
            locker.execute("ROLLBACK")
    assert "BEGIN IMMEDIATE" in trace
    assert ("COMMIT" in trace) == (phase == "COMMIT")
    assert ("ROLLBACK" in trace) == (phase == "COMMIT")
    assert rows(path, "SELECT * FROM claim") == []
    authority = LocalAuthority.reopen_for_test(path, current_unique_authority=True)
    claim = authority.perform_for_test(*ARGS, PINS[0].name, seen.append)
    assert seen == [claim]
    assert authority.lookup_unknown_for_test(*ARGS) == claim


_RECEIPT_WORKER = """
import sys
import time
from pathlib import Path
from ficha_etl.raw_authority import AuthorityRefused, Claim, LocalAuthority, Pin

path, gate, ready, name, token = sys.argv[1:]
authority = LocalAuthority.reopen_for_test(Path(path), current_unique_authority=True)
claim = Claim('2026-11', 'ficha-2026-11', 'a'*32, 'b'*32, Pin(name, 'c'*64, 10), token)
Path(ready).write_text(str(__debug__))
while not Path(gate).exists():
    time.sleep(0.01)
try:
    authority.record_test_match(claim, quiescent_and_matched=True)
except AuthorityRefused:
    print('REFUSED', token, flush=True)
else:
    print('MATCHED', token, flush=True)
"""


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("optimized", [False, True])
def test_receipt_cas_process_race(journal, tmp_path, stale, optimized):
    path, authority = journal
    first = authority.perform_for_test(*ARGS, PINS[0].name, lambda _: None)
    current = first
    if stale:
        authority.record_test_match(first, quiescent_and_matched=True)
        current = authority.perform_for_test(*ARGS, PINS[1].name, lambda _: None)
    candidates = [first, current]
    ready = [tmp_path / "receipt-a", tmp_path / "receipt-b"]
    gate = tmp_path / "receipt-gate"
    workers = []
    try:
        for marker, claim in zip(ready, candidates, strict=True):
            command = [sys.executable, *(["-O"] if optimized else []), "-c", _RECEIPT_WORKER]
            workers.append(
                subprocess.Popen(
                    [*command, str(path), str(gate), str(marker), claim.pin.name, claim.token],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        deadline = time.monotonic() + 20
        while not all(marker.exists() for marker in ready):
            if time.monotonic() > deadline:
                pytest.fail("receipt workers failed to reach barrier")
            time.sleep(0.01)
        gate.touch()
        results = [worker.communicate(timeout=20) for worker in workers]
        assert [worker.returncode for worker in workers] == [0, 0], results
        assert [marker.read_text() for marker in ready] == [str(not optimized)] * 2
        outcomes = []
        for (out, _), claim in zip(results, candidates, strict=True):
            outcome, token = out.split()
            assert token == claim.token
            outcomes.append(outcome)
        assert sorted(outcomes) == ["MATCHED", "REFUSED"]
        assert candidates[outcomes.index("MATCHED")] == current
        if stale:
            assert outcomes == ["REFUSED", "MATCHED"]
        assert authority.lookup_unknown_for_test(*ARGS) is None
        expected = {(claim.pin.name, claim.token, "TEST_MATCHED") for claim in candidates}
        assert set(rows(path, "SELECT name,token,state FROM claim")) == expected
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
                worker.communicate(timeout=10)
