"""Tests for the systemd watchdog notifier and the fork-worker signal
reset.

Background: on 2026-05-11 the collector wedged for 22h with 4 forked
workers stuck in multiprocessing.connection.recv. The parent SIGTERM
handler was inherited by every worker; the Python-level handler logged
nicely but didn't interrupt their blocking C-level recv(), so the cgroup
SIGTERM was effectively a no-op. Two fixes:

1. Forked workers reset SIGTERM/SIGINT to SIG_DFL so the kernel can
   terminate them cleanly.
2. The collector pings systemd's NOTIFY_SOCKET with WATCHDOG=1 after
   every phase. If a phase hangs longer than WatchdogSec (set in the
   unit file), systemd kills and restarts the service.
"""

import os
import signal
import socket
import threading

from gwmsmon.collector import _notify_watchdog


def test_notify_watchdog_noop_without_socket(monkeypatch):
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    # Must not raise.
    _notify_watchdog()
    _notify_watchdog("status text")


def test_notify_watchdog_sends_to_socket(tmp_path, monkeypatch):
    sock_path = tmp_path / "notify.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server.bind(str(sock_path))
    server.settimeout(2.0)
    try:
        monkeypatch.setenv("NOTIFY_SOCKET", str(sock_path))
        _notify_watchdog()
        data, _ = server.recvfrom(4096)
        assert data == b"WATCHDOG=1"
    finally:
        server.close()


def test_notify_watchdog_status_appended(tmp_path, monkeypatch):
    sock_path = tmp_path / "notify.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server.bind(str(sock_path))
    server.settimeout(2.0)
    try:
        monkeypatch.setenv("NOTIFY_SOCKET", str(sock_path))
        _notify_watchdog("cycle 7: query_all done")
        data, _ = server.recvfrom(4096)
        assert data == b"WATCHDOG=1\nSTATUS=cycle 7: query_all done"
    finally:
        server.close()


def test_notify_watchdog_unreachable_socket_does_not_raise(monkeypatch):
    """If NOTIFY_SOCKET points to a path that doesn't exist (e.g., the
    service is running outside systemd), the notifier must swallow the
    error rather than break the cycle."""
    monkeypatch.setenv("NOTIFY_SOCKET", "/tmp/nonexistent-watchdog.sock")
    _notify_watchdog()
    _notify_watchdog("ignored")


def test_notify_watchdog_abstract_socket(monkeypatch):
    """systemd commonly uses abstract Unix sockets (path starts with
    '@' which maps to a leading NUL byte). The notifier must handle
    both forms."""
    addr = "\0test-gwmsmon-watchdog-abstract"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server.bind(addr)
    server.settimeout(2.0)
    try:
        monkeypatch.setenv("NOTIFY_SOCKET", "@" + addr[1:])
        _notify_watchdog()
        data, _ = server.recvfrom(4096)
        assert data == b"WATCHDOG=1"
    finally:
        server.close()


# --- Fork-worker signal-reset smoke test ---


def test_fork_worker_signals_reset_to_default():
    """After fork, the worker function calls _reset_worker_signals().
    A child that inherits a SIG_IGN-style handler from the parent
    should end up with SIG_DFL again, so a SIGTERM kills the worker
    rather than being silently logged and ignored."""
    from gwmsmon.state import _reset_worker_signals

    # Install a no-op handler in the test process (simulates the
    # parent's _handle_signal).
    def _noop(_signum, _frame):
        pass

    old_term = signal.signal(signal.SIGTERM, _noop)
    old_int = signal.signal(signal.SIGINT, _noop)
    try:
        # Capture the inherited handler is our no-op.
        assert signal.getsignal(signal.SIGTERM) is _noop
        # After reset, it must be back to SIG_DFL.
        _reset_worker_signals()
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
        assert signal.getsignal(signal.SIGINT) == signal.SIG_DFL
    finally:
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)


def test_query_worker_signals_reset():
    """Same as above for the query.py reset helper. Two copies because
    state and query are separately imported in fork children — verify
    both behave identically."""
    from gwmsmon.query import _reset_worker_signals

    def _noop(_signum, _frame):
        pass

    old_term = signal.signal(signal.SIGTERM, _noop)
    try:
        _reset_worker_signals()
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
    finally:
        signal.signal(signal.SIGTERM, old_term)


def test_fork_child_dies_on_sigterm():
    """End-to-end: parent installs a no-op SIGTERM handler, forks a
    child that calls _reset_worker_signals() then sleeps. Parent sends
    SIGTERM to the child. Child must exit promptly (default SIGTERM
    action). If the inherited handler had remained, the child would
    swallow SIGTERM and sleep to completion — the exact failure mode
    of the 2026-05-11 incident."""
    from gwmsmon.state import _reset_worker_signals

    # Pipe so child can signal "I'm ready".
    r, w = os.pipe()

    def _parent_noop(_signum, _frame):
        pass

    old_term = signal.signal(signal.SIGTERM, _parent_noop)
    try:
        pid = os.fork()
        if pid == 0:
            # Child
            os.close(r)
            _reset_worker_signals()
            os.write(w, b"ready")
            os.close(w)
            # Long sleep that would block forever without SIG_DFL.
            try:
                threading.Event().wait(30.0)
            finally:
                os._exit(0)
        else:
            # Parent
            os.close(w)
            # Wait for child to be ready.
            assert os.read(r, 16) == b"ready"
            os.close(r)
            os.kill(pid, signal.SIGTERM)
            # Child must exit within a few seconds; SIGTERM default is
            # termination, so reaping returns its exit status.
            for _ in range(30):  # up to 3s
                done_pid, status = os.waitpid(pid, os.WNOHANG)
                if done_pid == pid:
                    break
                threading.Event().wait(0.1)
            else:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                raise AssertionError(
                    "child did not exit on SIGTERM within 3s — signal "
                    "reset is not effective")
            # SIGTERM termination: WIFSIGNALED true, signal == SIGTERM.
            assert os.WIFSIGNALED(status)
            assert os.WTERMSIG(status) == signal.SIGTERM
    finally:
        signal.signal(signal.SIGTERM, old_term)
