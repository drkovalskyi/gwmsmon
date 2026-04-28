"""Tests for gwmsmon.convert.

Covers the type-dispatch fast path added on 2026-04-28: scalar values
must bypass the recursive walker and pass through unchanged; ExprTrees
and lists must still be walked.
"""

import classad

from gwmsmon.convert import classad_to_python, convert_ad

# --- classad_to_python ---

def test_scalar_passthrough():
    assert classad_to_python(7) == 7
    assert classad_to_python(7.5) == 7.5
    assert classad_to_python("foo") == "foo"
    assert classad_to_python(True) is True
    assert classad_to_python(False) is False
    assert classad_to_python(None) is None


def test_exprtree_eval():
    expr = classad.ExprTree(eval_value=42)
    assert classad_to_python(expr) == 42


def test_exprtree_eval_returns_string():
    expr = classad.ExprTree(eval_value="evaluated")
    assert classad_to_python(expr) == "evaluated"


def test_exprtree_eval_raises():
    expr = classad.ExprTree(eval_value=RuntimeError("boom"))
    assert classad_to_python(expr) is None


def test_exprtree_chained():
    inner = classad.ExprTree(eval_value=99)
    outer = classad.ExprTree(eval_value=inner)
    assert classad_to_python(outer) == 99


def test_value_undefined_to_none():
    assert classad_to_python(classad.Value.Undefined) is None
    assert classad_to_python(classad.Value.Error) is None


def test_list_recurses():
    expr = classad.ExprTree(eval_value=5)
    assert classad_to_python([1, "x", expr]) == [1, "x", 5]


def test_dict_recurses():
    expr = classad.ExprTree(eval_value=10)
    assert classad_to_python({"a": 1, "b": expr}) == {"a": 1, "b": 10}


def test_unknown_falls_back_to_str():
    class Weird:
        def __str__(self):
            return "weird"
    assert classad_to_python(Weird()) == "weird"


# --- convert_ad ---

def test_convert_ad_with_projection():
    ad = {"JobStatus": 2, "Owner": "alice", "Extra": "ignored"}
    result = convert_ad(ad, projection=["JobStatus", "Owner"])
    assert result == {"JobStatus": 2, "Owner": "alice"}


def test_convert_ad_missing_keys_skipped():
    ad = {"JobStatus": 2}
    result = convert_ad(ad, projection=["JobStatus", "MissingField"])
    assert result == {"JobStatus": 2}
    assert "MissingField" not in result


def test_convert_ad_no_projection_uses_all_keys():
    ad = {"a": 1, "b": "two"}
    result = convert_ad(ad)
    assert result == {"a": 1, "b": "two"}


def test_convert_ad_evaluates_exprtree():
    ad = {
        "JobStatus": 2,
        "RequestDisk": classad.ExprTree(eval_value=1024),
    }
    result = convert_ad(ad, projection=["JobStatus", "RequestDisk"])
    assert result == {"JobStatus": 2, "RequestDisk": 1024}


def test_convert_ad_list_of_strings():
    """DESIRED_Sites in production is a comma-separated string,
    but some classads expose it as a list. Both must work."""
    ad = {"DESIRED_Sites": ["T1_US", "T2_CH"]}
    result = convert_ad(ad, projection=["DESIRED_Sites"])
    assert result == {"DESIRED_Sites": ["T1_US", "T2_CH"]}


def test_convert_ad_empty_projection():
    ad = {"JobStatus": 2}
    assert convert_ad(ad, projection=[]) == {}


def test_fast_path_handles_large_scalar_dict():
    """Sanity: convert_ad can chew through 10k scalar values quickly.
    If the fast path regressed to per-element recursion, we'd notice
    via the existing test runtime budget."""
    big = {f"k{i}": i for i in range(10_000)}
    out = convert_ad(big, projection=list(big.keys()))
    assert out == big
    big_s = {f"k{i}": str(i) for i in range(10_000)}
    out_s = convert_ad(big_s, projection=list(big_s.keys()))
    assert out_s == big_s
