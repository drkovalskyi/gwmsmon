"""Static check: every field in JOB_PROJECTION must be read in state.py.

Catches the failure mode "we fetch a field but nobody uses it" (waste)
and "we use a field but forgot to fetch it" (silent NULL/zero).

Implemented as a text scan to avoid pulling htcondor into tests.
"""

import ast
import pathlib

REPO = pathlib.Path(__file__).resolve().parents[1]
QUERY_PY = REPO / "src" / "gwmsmon" / "query.py"
STATE_PY = REPO / "src" / "gwmsmon" / "state.py"

# Fields that the *query layer* itself injects into job dicts (not
# fetched from classads). Excluded from the "must be read" check.
INJECTED_BY_QUERY = {"_schedd", "_schedd_type"}

# Fields that are always required for a complete job ad regardless of
# downstream readers (e.g. used as sentinels/keys).
ALWAYS_KEEP = {"JobStatus", "RequestCpus"}


def _extract_projection(path):
    """Return the list of strings in JOB_PROJECTION as defined in
    src/gwmsmon/query.py — by parsing the AST so we don't have to
    import htcondor."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "JOB_PROJECTION"
                and isinstance(node.value, ast.List)):
            return [
                elt.value
                for elt in node.value.elts
                if isinstance(elt, ast.Constant)
                and isinstance(elt.value, str)
            ]
    raise AssertionError("JOB_PROJECTION not found in query.py")


def _state_text():
    return STATE_PY.read_text()


def test_every_projection_field_is_read():
    proj = _extract_projection(QUERY_PY)
    text = _state_text()
    unread = []
    for field in proj:
        if field in INJECTED_BY_QUERY or field in ALWAYS_KEEP:
            continue
        if f'"{field}"' not in text and f"'{field}'" not in text:
            unread.append(field)
    assert not unread, (
        "JOB_PROJECTION contains fields with no read in state.py "
        f"(safe to drop): {unread}")


def test_projection_has_no_duplicates():
    proj = _extract_projection(QUERY_PY)
    seen = set()
    dupes = [f for f in proj if f in seen or seen.add(f)]
    assert not dupes, f"duplicate fields in JOB_PROJECTION: {dupes}"


def test_projection_field_names_are_valid_classad_attrs():
    """ClassAd attribute names start with a letter, contain alnum/underscore."""
    import re
    valid = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
    proj = _extract_projection(QUERY_PY)
    bad = [f for f in proj if not valid.match(f)]
    assert not bad, f"invalid classad attribute names: {bad}"
