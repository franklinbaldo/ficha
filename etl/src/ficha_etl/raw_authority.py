"""U-local: isolated SQLite authority for synthetic raw custody tests.

No transport integration. A local database is not a remote fence or proof of
freshness; the caller must establish exclusive, current authority separately.
Never restore a backup and assume that its absence of UNKNOWN is evidence.
"""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from .sources import canonical_inventory


class AuthorityRefused(RuntimeError):
    """No permission to initiate an effect or change claim state."""


@dataclass(frozen=True)
class Pin:
    name: str
    sha256: str
    size: int


@dataclass(frozen=True)
class Claim:
    month: str
    destination: str
    intent: str
    generation: str
    pin: Pin
    token: str


_SCHEMA = """
CREATE TABLE intent (
    month TEXT NOT NULL PRIMARY KEY,
    destination TEXT NOT NULL UNIQUE,
    intent TEXT NOT NULL UNIQUE,
    generation TEXT NOT NULL,
    CHECK(destination = 'ficha-' || month)
) STRICT;
CREATE TABLE pin (
    month TEXT NOT NULL REFERENCES intent(month),
    name TEXT NOT NULL,
    digest TEXT NOT NULL CHECK(length(digest)=64 AND digest NOT GLOB '*[^0-9a-f]*'),
    size INTEGER NOT NULL CHECK(size > 0),
    PRIMARY KEY(month,name)
) STRICT;
CREATE TABLE claim (
    month TEXT NOT NULL,
    name TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('UNKNOWN','TEST_MATCHED')),
    PRIMARY KEY(month,name),
    FOREIGN KEY(month,name) REFERENCES pin(month,name)
) STRICT;
CREATE UNIQUE INDEX one_unknown ON claim(month) WHERE state='UNKNOWN';
"""


def _hex_id(value: str) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{32}", value) is not None


def _validate_month(month: str, destination: str) -> None:
    if (
        type(month) is not str
        or re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month) is None
        or type(destination) is not str
        or destination != f"ficha-{month}"
    ):
        raise AuthorityRefused("noncanonical cohort")


def _validate_pins(pins: Iterable[Pin]) -> tuple[Pin, ...]:
    values = tuple(pins)
    names = {spec.name for spec in canonical_inventory()}
    if len(values) != 37 or any(
        type(p) is not Pin
        or type(p.name) is not str
        or type(p.sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", p.sha256) is None
        or type(p.size) is not int
        or not 0 < p.size < 2**63
        for p in values
    ):
        raise AuthorityRefused("invalid pins")
    if {p.name for p in values} != names:
        raise AuthorityRefused("noncanonical inventory")
    return tuple(sorted(values, key=lambda p: p.name))


class LocalAuthority:
    """One explicit disk database; no automatic recreation or retry.

    Only the API is within the trust boundary, not arbitrary SQL writers,
    replacement of the file, or multiple independent database copies.
    """

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise AuthorityRefused("absolute database path required")
        self._path = path

    @classmethod
    def create(cls, path: Path) -> LocalAuthority:
        """Exclusive creation. Existing or partially created files are refused."""
        authority = cls(path)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except OSError as exc:
            raise AuthorityRefused("cannot exclusively create database") from exc
        os.close(fd)
        with authority._connection() as db:
            db.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA + "\nCOMMIT;")
        return authority

    @classmethod
    def reopen_for_test(cls, path: Path, *, current_unique_authority: bool = False) -> LocalAuthority:
        """Synthetic precondition only; no implementation of antirollback or F."""
        if current_unique_authority is not True:
            raise AuthorityRefused("freshness and unique authority not established")
        authority = cls(path)
        with authority._connection() as db:
            db.execute("SELECT month FROM intent LIMIT 1").fetchall()
        return authority

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self._path.as_uri() + "?mode=rw", uri=True, isolation_level=None)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            if db.execute("PRAGMA foreign_keys").fetchone() != (1,):
                raise AuthorityRefused("foreign keys disabled")
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise

    def reserve(
        self, month: str, destination: str, intent: str, generation: str, pins: Iterable[Pin]
    ) -> None:
        """Write-once intent; recovery verifies, never replaces."""
        _validate_month(month, destination)
        if not _hex_id(intent) or not _hex_id(generation):
            raise AuthorityRefused("invalid intent or generation")
        values = _validate_pins(pins)
        with self._transaction() as db:
            try:
                db.execute(
                    "INSERT INTO intent VALUES (?,?,?,?)",
                    (month, destination, intent, generation),
                )
                db.executemany(
                    "INSERT INTO pin VALUES (?,?,?,?)",
                    [(month, p.name, p.sha256, p.size) for p in values],
                )
            except sqlite3.IntegrityError as exc:
                raise AuthorityRefused("intent already reserved or invalid") from exc

    @staticmethod
    def _verify(
        db: sqlite3.Connection,
        month: str,
        destination: str,
        intent: str,
        generation: str,
        pins: tuple[Pin, ...],
    ) -> None:
        row = db.execute(
            "SELECT destination,intent,generation FROM intent WHERE month=?", (month,)
        ).fetchone()
        stored = db.execute(
            "SELECT name,digest,size FROM pin WHERE month=? ORDER BY name", (month,)
        ).fetchall()
        if row != (destination, intent, generation) or stored != [
            (p.name, p.sha256, p.size) for p in pins
        ]:
            raise AuthorityRefused("intent identity mismatch")

    def perform_for_test(
        self,
        month: str,
        destination: str,
        intent: str,
        generation: str,
        pins: Iterable[Pin],
        name: str,
        effect: Callable[[Claim], None],
    ) -> Claim:
        """Commit UNKNOWN, then call once. Never retry or issue an existing grant."""
        _validate_month(month, destination)
        values = _validate_pins(pins)
        if not _hex_id(intent) or not _hex_id(generation):
            raise AuthorityRefused("invalid intent or generation")
        pin = next((p for p in values if p.name == name), None)
        if pin is None:
            raise AuthorityRefused("unknown pin")
        claim = Claim(month, destination, intent, generation, pin, uuid4().hex)
        with self._transaction() as db:
            self._verify(db, month, destination, intent, generation, values)
            try:
                db.execute("INSERT INTO claim VALUES (?,?,?,'UNKNOWN')", (month, name, claim.token))
            except sqlite3.IntegrityError as exc:
                raise AuthorityRefused("claim already exists or cohort has UNKNOWN") from exc
        # No caller-held grant can be fed back into this path. An exception or
        # process death from here onward leaves UNKNOWN, even with zero effects.
        effect(claim)
        return claim

    def record_test_match(self, claim: Claim) -> None:
        """Synthetic receipt only; never evidence of remote content or promotion."""
        with self._transaction() as db:
            cursor = db.execute(
                """UPDATE claim SET state='TEST_MATCHED'
                   WHERE month=? AND name=? AND token=? AND state='UNKNOWN'
                   AND EXISTS (
                     SELECT 1 FROM intent i JOIN pin p ON p.month=i.month
                     WHERE i.month=claim.month AND p.name=claim.name
                       AND i.destination=? AND i.intent=? AND i.generation=?
                       AND p.digest=? AND p.size=?
                   )""",
                (
                    claim.month,
                    claim.pin.name,
                    claim.token,
                    claim.destination,
                    claim.intent,
                    claim.generation,
                    claim.pin.sha256,
                    claim.pin.size,
                ),
            )
            if cursor.rowcount != 1:
                raise AuthorityRefused("receipt does not match active claim")
