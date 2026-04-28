"""Tests for gwmsmon.exitcodes.describe()."""

import pytest

from gwmsmon.exitcodes import SIGNALS, WM_JOB_ERROR_CODES, describe


def test_known_code():
    assert describe("0") == "Success"
    assert "segmentation" in describe("11").lower()


def test_signal_known():
    assert describe("SIG:9") == "Killed by SIGKILL"
    assert describe("SIG:15") == "Killed by SIGTERM"


def test_signal_unknown():
    assert describe("SIG:99") == "Killed by signal 99"


def test_unknown_code_returns_empty():
    assert describe("123456") == ""


def test_negative_code():
    assert describe("-1") == WM_JOB_ERROR_CODES[-1]


def test_garbage_input_returns_empty():
    assert describe("not-a-code") == ""
    assert describe("") == ""


def test_signal_garbage_raises():
    """SIG: prefix with non-int payload should not crash silently —
    raise so we notice rather than blandly returning empty."""
    with pytest.raises(ValueError):
        describe("SIG:abc")


def test_all_signal_codes_have_names():
    for sig, name in SIGNALS.items():
        assert name.startswith("SIG"), name
        assert isinstance(sig, int)
