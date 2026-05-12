"""Shared test fixtures and stubs.

The htcondor/classad bindings are only available on hosts with the
HTCondor packages installed. Tests run on dev machines and CI where
they aren't, so we install a minimal in-process stub for `classad`
that mimics the surface our code touches: ExprTree (with eval) and
Value (with Undefined/Error sentinels).
"""

import sys
import types


def _install_classad_stub():
    if "classad" in sys.modules:
        return  # real binding available

    classad = types.ModuleType("classad")

    class ExprTree:
        """Stub ExprTree. Construct with the value eval() should
        return, or with an Exception instance to raise."""

        def __init__(self, eval_value=None):
            self._eval_value = eval_value

        def eval(self):
            v = self._eval_value
            if isinstance(v, BaseException):
                raise v
            return v

    class Value:
        """Marker class. Value.Undefined and Value.Error are
        instances; isinstance(x, Value) works."""
        pass

    Value.Undefined = Value()
    Value.Error = Value()

    class ClassAd(dict):
        """Stub ClassAd: behaves like a dict for our purposes."""

    def parseOne(_text):
        return ClassAd()

    classad.ExprTree = ExprTree
    classad.Value = Value
    classad.ClassAd = ClassAd
    classad.parseOne = parseOne
    sys.modules["classad"] = classad


def _install_htcondor_stub():
    """Stub `htcondor` for tests that import modules touching the
    binding (e.g., collector). No test exercises a real query — the
    stubs only need to satisfy module-import-time attribute access."""
    if "htcondor" in sys.modules:
        return  # real binding available

    htcondor = types.ModuleType("htcondor")

    class _Schedd:
        def __init__(self, *_a, **_kw):
            pass

        def query(self, *_a, **_kw):
            return []

        def history(self, *_a, **_kw):
            return []

    class _Collector:
        def __init__(self, *_a, **_kw):
            pass

        def query(self, *_a, **_kw):
            return []

    class _AdTypes:
        Schedd = "Schedd"
        Submitter = "Submitter"
        Startd = "Startd"
        Negotiator = "Negotiator"
        Accounting = "Accounting"

    htcondor.Schedd = _Schedd
    htcondor.Collector = _Collector
    htcondor.AdTypes = _AdTypes
    sys.modules["htcondor"] = htcondor


_install_classad_stub()
_install_htcondor_stub()
