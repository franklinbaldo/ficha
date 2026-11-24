"""Calibrate process-kill oracles independently of the retired IAS3 route.

Historical real-entrypoint http503/timeout/hard-kill probes (3 strict XFAIL):
https://github.com/franklinbaldo/ficha/blob/7f10899c3542b077eca69102eed503826f9eac7b/etl/tests/test_polvo_raw_process_restart_unknown.py#L227-L302

The admission refusal does not establish durable recovery after a remote PUT.
The independent negative controls below remain active.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest


_READY = "partial-body-awaiting-kill"
_WINDOWS_KILL_CODE = 197


def _kill_exit_code() -> int:
    return _WINDOWS_KILL_CODE if sys.platform == "win32" else -signal.SIGKILL


def _kill_parked_child(child: subprocess.Popen[str], marker: Path) -> None:
    if marker.read_text() != _READY or child.poll() is not None:
        raise AssertionError("worker is not parked at the exact partial PUT handshake")
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        terminate = ctypes.WinDLL("kernel32", use_last_error=True).TerminateProcess
        terminate.argtypes = [wintypes.HANDLE, wintypes.UINT]
        terminate.restype = wintypes.BOOL
        # Popen owns this handle until its context is closed.
        if not terminate(int(child._handle), _WINDOWS_KILL_CODE):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        os.kill(child.pid, signal.SIGKILL)
    if child.wait(timeout=15) != _kill_exit_code():
        raise AssertionError("worker did not terminate with the reserved hard-kill status")


@pytest.mark.parametrize("code", [0, 1, 42, 197])
def test_spontaneous_exit_after_handshake_is_not_parent_kill(tmp_path, code):
    marker = tmp_path / "ready.flag"
    program = "from pathlib import Path; import sys; Path(sys.argv[1]).write_text(sys.argv[2])"
    with subprocess.Popen(
        [sys.executable, "-c", program + f"; raise SystemExit({code})", str(marker), _READY]
    ) as child:
        if child.wait(timeout=15) != code or marker.read_text() != _READY:
            raise AssertionError("negative control failed to exit after handshake")
        with pytest.raises(AssertionError, match="not parked"):
            _kill_parked_child(child, marker)
