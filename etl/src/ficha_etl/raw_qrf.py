"""Offline Q/R/CAS composition, never a publication authority.

One live synthetic sink, one current database, cooperating threads only.
No arbitrary SQL, database replacement/rollback, cross-process sink, remote
transport, uploader or publication capability is covered by this module.
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from uuid import uuid4

import httpx

from .raw_authority import (
    AuthorityRefused,
    Claim,
    Pin,
    _hex_id,
    _validate_month,
    _validate_pins,
)
from .raw_readback import RawPin, readback_raw_pin

_SCHEMA = """
CREATE TABLE qrf_domain (epoch TEXT NOT NULL PRIMARY KEY) STRICT;
CREATE TABLE qrf_intent (
    month TEXT PRIMARY KEY NOT NULL,
    destination TEXT UNIQUE NOT NULL,
    intent TEXT UNIQUE NOT NULL,
    generation TEXT NOT NULL
) STRICT;
CREATE TABLE qrf_pin (
    month TEXT NOT NULL REFERENCES qrf_intent(month),
    name TEXT NOT NULL,
    digest TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size > 0),
    PRIMARY KEY(month,name)
) STRICT;
CREATE TABLE qrf_claim (
    month TEXT NOT NULL,
    name TEXT NOT NULL,
    token TEXT UNIQUE NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('UNKNOWN','TEST_MATCHED')),
    PRIMARY KEY(month,name),
    FOREIGN KEY(month,name) REFERENCES qrf_pin(month,name)
) STRICT;
CREATE UNIQUE INDEX qrf_one_unknown ON qrf_claim(month) WHERE state='UNKNOWN';
CREATE TABLE qrf_receipt (
    token TEXT PRIMARY KEY NOT NULL REFERENCES qrf_claim(token),
    epoch TEXT NOT NULL REFERENCES qrf_domain(epoch),
    cut INTEGER NOT NULL,
    observed INTEGER NOT NULL CHECK(observed > cut),
    digest TEXT NOT NULL,
    size INTEGER NOT NULL,
    url TEXT NOT NULL
) STRICT;
"""


@dataclass(frozen=True)
class Grant:
    """Identity alone is not a capability: the sink checks object identity."""

    claim: Claim


@dataclass(frozen=True)
class Receipt:
    token: str
    epoch: str
    cut: int
    observed: int
    digest: str
    size: int
    url: str

    @property
    def publication_authorized(self) -> bool:
        return False


class SyntheticSink:
    """A single in-memory protection domain with no external callbacks.

    The private fields are trusted implementation state, not an adversarial
    Python sandbox. A fresh instance cannot inherit continuity.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._epoch = uuid4().hex
        self._path: Path | None = None
        self._sequence = 0
        self._grants: dict[str, tuple[Grant, str]] = {}
        self._objects: dict[tuple[str, str], bytes] = {}

    def _tick(self) -> int:
        self._sequence += 1
        return self._sequence

    def apply(self, grant: Grant, body: bytes, *, fail_after_consume: bool = False) -> None:
        """Consume before attempting a mutation; a failure cannot rearm it."""
        if type(grant) is not Grant or type(body) is not bytes:
            raise AuthorityRefused("invalid synthetic effect")
        with self._lock:
            current = self._grants.get(grant.claim.token)
            if current is None or current[0] is not grant or current[1] != "active":
                raise AuthorityRefused("grant not active")
            self._grants[grant.claim.token] = (grant, "consumed")
            self._tick()
            if fail_after_consume:
                raise AuthorityRefused("synthetic ambiguous failure")
            claim = grant.claim
            self._objects[claim.month, claim.pin.name] = body

    def _cut(self, claim: Claim) -> int:
        current = self._grants.get(claim.token)
        if current is None or current[0].claim != claim:
            raise AuthorityRefused("no continuous grant history")
        self._grants[claim.token] = (current[0], "revoked")
        return self._tick()

    def _read(self, claim: Claim):
        def handler(request: httpx.Request) -> httpx.Response:
            expected = f"https://archive.org/download/{claim.destination}/raw/{claim.pin.name}"
            if request.method != "GET" or str(request.url) != expected:
                raise AuthorityRefused("synthetic physical key mismatch")
            body = self._objects.get((claim.month, claim.pin.name))
            if body is None:
                return httpx.Response(404)
            return httpx.Response(200, stream=httpx.ByteStream(body))

        # Fresh client for each observation, same sink and lock.
        with httpx.Client(transport=httpx.MockTransport(handler), trust_env=False) as client:
            return readback_raw_pin(
                client,
                month=claim.month,
                pin=RawPin(name=claim.pin.name, size=claim.pin.size, sha256=claim.pin.sha256),
            )


class OfflineReconciler:
    """Schema-isolated authority, lock order sink then SQLite.

    No operation releases UNKNOWN without a fresh read under the same sink
    lock and an atomic durable receipt. The 37-pin intent is not a certificate.
    """

    def __init__(self, path: Path, sink: SyntheticSink, *, create: bool = False) -> None:
        if type(sink) is not SyntheticSink or not path.is_absolute():
            raise AuthorityRefused("explicit absolute path and synthetic sink required")
        self._path = path.resolve()
        self._sink = sink
        with sink._lock:
            if create:
                if sink._path is not None:
                    raise AuthorityRefused("sink already bound")
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
                with self._connection() as db:
                    db.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA + "\nCOMMIT;")
                    db.execute("INSERT INTO qrf_domain VALUES (?)", (sink._epoch,))
                sink._path = self._path
            else:
                # The live binding is required, not just a matching label.
                if sink._path != self._path:
                    raise AuthorityRefused("sink continuity lost")
                with self._connection() as db:
                    self._verify_domain(db)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self._path.as_uri() + "?mode=rw", uri=True, isolation_level=None)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            if db.execute("PRAGMA foreign_keys").fetchone() != (1,):
                raise AuthorityRefused("foreign keys disabled")
            yield db
        finally:
            db.close()

    def _verify_domain(self, db: sqlite3.Connection) -> None:
        if db.execute("SELECT epoch FROM qrf_domain").fetchall() != [(self._sink._epoch,)]:
            raise AuthorityRefused("sink incarnation mismatch")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._verify_domain(db)
                yield db
                db.execute("COMMIT")
            except BaseException:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def reserve(
        self, month: str, destination: str, intent: str, generation: str, pins: Iterable[Pin]
    ) -> None:
        _validate_month(month, destination)
        if not _hex_id(intent) or not _hex_id(generation):
            raise AuthorityRefused("invalid intent or generation")
        values = _validate_pins(pins)
        with self._sink._lock, self._transaction() as db:
            db.execute(
                "INSERT INTO qrf_intent VALUES (?,?,?,?)", (month, destination, intent, generation)
            )
            db.executemany(
                "INSERT INTO qrf_pin VALUES (?,?,?,?)",
                [(month, p.name, p.sha256, p.size) for p in values],
            )

    @staticmethod
    def _identity(db: sqlite3.Connection, month: str, name: str, token: str) -> Claim:
        row = db.execute(
            """SELECT i.destination,i.intent,i.generation,p.digest,p.size
               FROM qrf_intent i JOIN qrf_pin p ON i.month=p.month
               WHERE i.month=? AND p.name=?""",
            (month, name),
        ).fetchone()
        if row is None:
            raise AuthorityRefused("unknown pin")
        destination, intent, generation, digest, size = row
        return Claim(month, destination, intent, generation, Pin(name, digest, size), token)

    def admit(self, month: str, name: str) -> Grant:
        _validate_month(month, f"ficha-{month}")
        if type(name) is not str:
            raise AuthorityRefused("invalid pin name")
        with self._sink._lock:
            with self._transaction() as db:
                claim = self._identity(db, month, name, uuid4().hex)
                try:
                    db.execute(
                        "INSERT INTO qrf_claim VALUES (?,?,?,'UNKNOWN')",
                        (month, name, claim.token),
                    )
                except sqlite3.IntegrityError as exc:
                    raise AuthorityRefused("already admitted or cohort UNKNOWN") from exc
            # Durable UNKNOWN precedes the grant; a crash here fails closed.
            grant = Grant(claim)
            self._sink._grants[claim.token] = (grant, "active")
            return grant

    def lookup_unknown(self, month: str) -> Claim | None:
        _validate_month(month, f"ficha-{month}")
        with self._sink._lock, self._transaction() as db:
            row = db.execute(
                "SELECT name,token FROM qrf_claim WHERE month=? AND state='UNKNOWN'", (month,)
            ).fetchone()
            return None if row is None else self._identity(db, month, *row)

    def reconcile(self, claim: Claim) -> Receipt:
        if type(claim) is not Claim or type(claim.pin) is not Pin:
            raise AuthorityRefused("invalid claim")
        with self._sink._lock:
            # Never accept a caller-supplied observation or quiescence boolean.
            with self._transaction() as db:
                row = db.execute(
                    "SELECT token FROM qrf_claim WHERE month=? AND name=? AND state='UNKNOWN'",
                    (claim.month, claim.pin.name),
                ).fetchone()
                if row != (claim.token,) or self._identity(
                    db, claim.month, claim.pin.name, claim.token
                ) != claim:
                    raise AuthorityRefused("claim not current")
            cut = self._sink._cut(claim)
            observation = self._sink._read(claim)
            observed = self._sink._tick()
            obj = observation.object
            receipt = Receipt(
                claim.token, self._sink._epoch, cut, observed, obj.sha256, obj.size, obj.final_url
            )
            with self._transaction() as db:
                cursor = db.execute(
                    """UPDATE qrf_claim SET state='TEST_MATCHED'
                       WHERE month=? AND name=? AND token=? AND state='UNKNOWN'""",
                    (claim.month, claim.pin.name, claim.token),
                )
                if cursor.rowcount != 1:
                    raise AuthorityRefused("stale CAS")
                db.execute(
                    "INSERT INTO qrf_receipt VALUES (?,?,?,?,?,?,?)",
                    (receipt.token, receipt.epoch, receipt.cut, receipt.observed,
                     receipt.digest, receipt.size, receipt.url),
                )
            return receipt


def historical_receipts(path: Path) -> tuple[Receipt, ...]:
    """Read-only synthetic history; no authority or continuity is conferred."""
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        try:
            return tuple(Receipt(*row) for row in db.execute(
                "SELECT token,epoch,cut,observed,digest,size,url FROM qrf_receipt ORDER BY observed"
            ))
        finally:
            db.close()
