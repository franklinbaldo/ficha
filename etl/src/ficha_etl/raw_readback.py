"""Offline-callable, full-body IA raw readback from independently pinned bytes.

This is a *read-only primitive*, not a publication gate. A matching response
establishes R at the time of the GET only. In particular it does not prove the
source cohort (S), durable intent resolution, or exclusion of later writes (F).

The caller supplies an unauthenticated httpx.Client with trust_env=False,
without event hooks, and pins from the *sealed* source, never IA metadata.
The client is not accepted as proof of transport or version identity. This module
is intentionally not wired to any upload/manifest/cron path (Ficha #433).
"""

from __future__ import annotations

import hashlib
import hmac
import re
from contextlib import closing
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from .sources import canonical_inventory, is_valid_month

_CHUNK = 64 * 1024
_MAX_REDIRECTS = 5
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_CDN_HOST = re.compile(r"ia[0-9]+\.us\.archive\.org\Z")


class RawReadbackError(RuntimeError):
    """No positive custody observation may be inferred from this failure."""


@dataclass(frozen=True)
class RawPin:
    name: str  # e.g., Empresas0.zip, without raw/ prefix
    size: int
    sha256: str  # independent digest of source bytes, not IA metadata


@dataclass(frozen=True)
class RawObjectObservation:
    name: str
    size: int
    sha256: str
    request_url: str
    final_url: str
    redirects: int
    status: str = "R_MATCH_AT_T_ONLY"

    @property
    def publication_authorized(self) -> bool:
        return False

    @property
    def future_writes_fenced(self) -> bool:
        return False


@dataclass(frozen=True)
class RawBatchObservation:
    month: str
    objects: tuple[RawObjectObservation, ...]
    total_size: int
    status: str = "R_MATCH_AT_T_ONLY"

    @property
    def publication_authorized(self) -> bool:
        return False

    @property
    def source_cohort_proven(self) -> bool:
        return False

    @property
    def future_writes_fenced(self) -> bool:
        return False


def _validated_pins(pins: tuple[RawPin, ...]) -> tuple[RawPin, ...]:
    expected = [spec.name for spec in canonical_inventory()]
    if len(expected) != 37 or len(set(expected)) != 37 or len(pins) != 37:
        raise RawReadbackError("raw readback requires exactly 37 canonical ZIP pins")
    names = [pin.name for pin in pins if isinstance(pin, RawPin)]
    if len(names) != 37 or len(set(names)) != 37 or set(names) != set(expected):
        raise RawReadbackError("raw readback requires exactly the canonical inventory")
    for pin in pins:
        if type(pin.size) is not int or pin.size <= 0:
            raise RawReadbackError("raw readback requires a positive independently pinned size")
        if not isinstance(pin.sha256, str) or _SHA256.fullmatch(pin.sha256) is None:
            raise RawReadbackError("raw readback requires lowercase SHA-256 pins")
    return tuple(sorted(pins, key=lambda p: p.name))


def _validate_archive_url(url: httpx.URL, identifier: str, name: str) -> None:
    """Accept only the canonical item/file URL, including a narrow CDN spelling.

    For IA storage nodes, the mapping /<drive>/items/<item>/<filename>
    is a URL-shape constraint, NOT proof that IA has served that object.
    Any unrecognised host/path, including a percent-encoded alias, fails closed.
    """
    host = url.host
    if (
        url.scheme != "https"
        or not host
        or url.port not in (None, 443)
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise RawReadbackError("raw readback refuses unsafe redirect")

    encoded_name = quote(name, safe="").encode("ascii")
    encoded_id = identifier.encode("ascii")
    if host == "archive.org":
        expected = b"/download/" + encoded_id + b"/raw/" + encoded_name
        matched = url.raw_path == expected
    elif _CDN_HOST.fullmatch(host):
        # The drive number varies across IA storage nodes. Match *every*
        # remaining byte to the independently requested item and filename.
        expected = rb"/[0-9]+/items/" + re.escape(encoded_id) + rb"/raw/" + re.escape(encoded_name)
        matched = re.fullmatch(expected, url.raw_path) is not None
    else:
        matched = False
    if not matched:
        raise RawReadbackError("raw readback redirect changes item or file key")


def _check_client(client: httpx.Client) -> None:
    if (
        # HTTPX 0.28 keeps trust_env private; fail closed on upgrades.
        # No ambient proxy/netrc/SSL overrides are admissible here.
        getattr(client, "_trust_env", True) is not False
        or client.auth is not None
        or bool(client.cookies)
        or bool(client.params)
        or "authorization" in client.headers
        or "cookie" in client.headers
        or "proxy-authorization" in client.headers
        or bool(client.event_hooks.get("request"))
        or bool(client.event_hooks.get("response"))
    ):
        raise RawReadbackError("raw readback requires an unauthenticated client")


def _read_one(client: httpx.Client, identifier: str, pin: RawPin) -> RawObjectObservation:
    # Only canonical inventory names reach this point, but URL-encode defensively.
    request_url = f"https://archive.org/download/{identifier}/raw/{quote(pin.name, safe='')}"
    current = httpx.URL(request_url)
    seen: set[str] = set()
    redirects = 0
    while True:
        _validate_archive_url(current, identifier, pin.name)
        if str(current) in seen:
            raise RawReadbackError("raw readback redirect loop")
        seen.add(str(current))
        try:
            # Never inherit automatic redirects or attach cookies/credentials.
            # Inspect the fully composed HTTPX request before any I/O.
            request = client.build_request(
                "GET",
                current,
                headers={"Accept-Encoding": "identity", "Cache-Control": "no-cache"},
            )
            _validate_archive_url(request.url, identifier, pin.name)
            response = client.send(request, stream=True, follow_redirects=False)
            with closing(response):
                if response.status_code in (301, 302, 303, 307, 308):
                    locations = response.headers.get_list("location")
                    if redirects >= _MAX_REDIRECTS or len(locations) != 1 or not locations[0]:
                        raise RawReadbackError("raw readback missing/ambiguous redirect")
                    # Relative redirects are permitted, subject to the same host/scheme gate.
                    current = response.url.join(locations[0])
                    redirects += 1
                    continue
                if response.status_code != 200 or "content-range" in response.headers:
                    raise RawReadbackError("raw readback requires a complete HTTP 200 body")
                encodings = response.headers.get_list("content-encoding")
                if len(encodings) > 1 or (encodings and encodings[0].strip().lower() != "identity"):
                    raise RawReadbackError("raw readback refuses encoded HTTP bodies")
                lengths = response.headers.get_list("content-length")
                if lengths:
                    if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdecimal():
                        raise RawReadbackError("raw readback refuses ambiguous content length")
                    if int(lengths[0]) != pin.size:
                        raise RawReadbackError("raw readback content length differs from pin")
                digest = hashlib.sha256()
                observed_size = 0
                for chunk in response.iter_raw(chunk_size=_CHUNK):
                    observed_size += len(chunk)
                    if observed_size > pin.size:
                        raise RawReadbackError("raw readback exceeds independently pinned size")
                    digest.update(chunk)
                if observed_size != pin.size or not hmac.compare_digest(
                    digest.hexdigest(), pin.sha256
                ):
                    raise RawReadbackError("raw readback body differs from independent pin")
                return RawObjectObservation(
                    name=pin.name,
                    size=observed_size,
                    sha256=digest.hexdigest(),
                    request_url=request_url,
                    final_url=str(response.url),
                    redirects=redirects,
                )
        except (httpx.HTTPError, httpx.StreamError, ValueError) as exc:
            raise RawReadbackError("raw readback HTTP transport/URL failed") from exc


def readback_raw_batch(
    client: httpx.Client, *, month: str, pins: tuple[RawPin, ...]
) -> RawBatchObservation:
    """Read 37 physical bodies without storing them or authorizing publication.

    Validates *all* pins before the first request. A failure or partial set
    raises, never returns a positive partial batch; there are no retries or PUTs.
    This must only be called after separate proofs of S and sealed source pins.
    """
    if not isinstance(month, str) or not is_valid_month(month):
        raise RawReadbackError("raw readback invalid month")
    ordered = _validated_pins(pins)
    _check_client(client)
    identifier = f"ficha-{month}"
    observations = tuple(_read_one(client, identifier, pin) for pin in ordered)
    return RawBatchObservation(
        month=month,
        objects=observations,
        total_size=sum(observation.size for observation in observations),
    )
