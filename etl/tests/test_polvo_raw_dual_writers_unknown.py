"""Calibrate dual-writer oracles independently of the retired IAS3 route.

Historical two-writer probe (strict XFAIL, two bodies and two HTTP 503 outcomes):
https://github.com/franklinbaldo/ficha/blob/7f10899c3542b077eca69102eed503826f9eac7b/etl/tests/test_polvo_raw_dual_writers_unknown.py#L251-L283

Current offline one-shot control: test_raw_qrf.py::test_qrf02_one_shot_two_threads.
It is not a multi-process or remotely enforced fence.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from ficha_etl import upload


_DUAL_PREFIX = "POLVO_OFFLINE_DUAL_RESULT="


class ConcurrentRawWrite(AssertionError):
    """Both invocations reached the fake PUT for the same remote object."""


def _is_authority_refusal(exc: BaseException | None) -> bool:
    # Proposed typed U boundary; absent today. Generic errors are not evidence.
    refusal = getattr(upload, "RawAuthorityRefusal", None)
    return isinstance(refusal, type) and isinstance(exc, refusal)


def _check_outcomes(
    attempts: list[str],
    observed: list[tuple[str, bytes]],
    outcomes: dict[str, BaseException | None],
    expected: dict[str, bytes],
) -> None:
    writers = [generation for generation, _ in observed]
    if sorted(attempts) != sorted(writers) or len(set(writers)) != len(writers):
        raise AssertionError("attempts and complete bodies must correspond one-to-one")
    if set(outcomes) != set(expected):
        raise AssertionError("missing writer outcome")
    for generation, body in observed:
        if generation not in expected or body != expected[generation]:
            raise AssertionError("wire body must match its writer's generation")
        outcome = outcomes[generation]
        if not isinstance(outcome, upload._IAS3Error) or outcome.status != 503:
            raise AssertionError("observed writer must receive its own HTTP 503")
    if len(writers) == 2:
        raise ConcurrentRawWrite("two writers sent A and B to the same raw object without a fence")
    if len(writers) != 1:
        raise AssertionError(f"expected one fenced write, got {len(writers)}")
    other = next(generation for generation in expected if generation not in writers)
    if not _is_authority_refusal(outcomes[other]):
        raise AssertionError("second writer must report an explicit raw authority refusal")


@pytest.mark.parametrize(
    ("observed", "status_a", "status_b", "generic_refusal", "attempts"),
    [
        ([("A", b"A")], 503, None, False, ["A"]),
        ([("A", b"B")], 503, None, False, ["A"]),
        ([("A", b"A")], None, 503, False, ["A"]),
        ([("A", b"A")], 401, None, False, ["A"]),
        ([("A", b"A")], 503, None, True, ["A"]),
        ([("A", b"A")], 503, None, False, ["A", "B"]),
        ([("A", b"A"), ("B", b"B")], 503, 401, False, ["A", "B"]),
    ],
)
def test_dual_writer_oracle_mutations(
    monkeypatch, observed, status_a, status_b, generic_refusal, attempts
):
    class RawAuthorityRefusal(RuntimeError):
        pass

    monkeypatch.setattr(upload, "RawAuthorityRefusal", RawAuthorityRefusal, raising=False)

    def outcome(status):
        if status is not None:
            return upload._IAS3Error(status, "https://s3.us.archive.org/offline")
        return RuntimeError("database locked") if generic_refusal else RawAuthorityRefusal()

    args = (
        attempts,
        observed,
        {"A": outcome(status_a), "B": outcome(status_b)},
        {"A": b"A", "B": b"B"},
    )
    if observed == [("A", b"A")] and status_a == 503 and not generic_refusal and attempts == ["A"]:
        _check_outcomes(*args)
    else:
        with pytest.raises(AssertionError) as error:
            _check_outcomes(*args)
        if isinstance(error.value, ConcurrentRawWrite):
            raise AssertionError("mutation must not be mistaken for the real counterexample")


def _check_dual_child(child: subprocess.CompletedProcess[str]) -> None:
    markers = [line for line in child.stdout.splitlines() if line.startswith(_DUAL_PREFIX)]
    if child.returncode == 42 and markers == [_DUAL_PREFIX + "CONCURRENT"]:
        raise ConcurrentRawWrite("offline worker observed two exact writes and HTTP 503 outcomes")
    if child.returncode != 0 or markers != [_DUAL_PREFIX + "PASS"]:
        raise AssertionError(child.stdout + "\n" + child.stderr)


@pytest.mark.parametrize("code", [0, 42])
def test_dual_worker_early_exit_is_not_a_result(code):
    child = subprocess.run(
        [sys.executable, "-c", f"raise SystemExit({code})"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    with pytest.raises(AssertionError) as error:
        _check_dual_child(child)
    if isinstance(error.value, ConcurrentRawWrite):
        raise AssertionError("early exit must not be classified as a counterexample")


@pytest.mark.parametrize(
    ("code", "markers"),
    [
        (0, ["PASS"]),
        (42, ["CONCURRENT"]),
        (0, ["CONCURRENT"]),
        (42, ["PASS"]),
        (42, ["CONCURRENT", "CONCURRENT"]),
        (0, ["PASS", "CONCURRENT"]),
    ],
)
def test_dual_terminal_marker_oracle(code, markers):
    stdout = "\n".join(_DUAL_PREFIX + marker for marker in markers)
    child = subprocess.CompletedProcess([], code, stdout, "")
    if code == 0 and markers == ["PASS"]:
        _check_dual_child(child)
    elif code == 42 and markers == ["CONCURRENT"]:
        with pytest.raises(ConcurrentRawWrite):
            _check_dual_child(child)
    else:
        with pytest.raises(AssertionError) as error:
            _check_dual_child(child)
        if isinstance(error.value, ConcurrentRawWrite):
            raise AssertionError("invalid marker must not be classified as a counterexample")
