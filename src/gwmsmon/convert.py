"""Classad-to-Python conversion boundary.

All classad values must be converted to plain Python types at the
query boundary. No classad objects are ever stored in application
data structures or serialized to JSON.
"""

try:
    import classad
    _HAS_CLASSAD = True
except ImportError:
    _HAS_CLASSAD = False


_SCALAR_TYPES = (int, str, float, bool, type(None))


def classad_to_python(value):
    """Recursively convert a classad value to a plain Python type.

    Classad types must be checked BEFORE the scalar fast path:
    classad.ExprTree and classad.Value can inherit from str/int via
    Boost.Python, so isinstance(v, _SCALAR_TYPES) returns True and the
    fast path would store the unconverted classad object — which then
    fails JSON serialization downstream.
    """
    if _HAS_CLASSAD:
        if isinstance(value, classad.ExprTree):
            try:
                evaluated = value.eval()
            except Exception:
                return None
            return classad_to_python(evaluated)
        if isinstance(value, classad.Value):
            return None

    if isinstance(value, _SCALAR_TYPES):
        return value

    if isinstance(value, list):
        return [classad_to_python(item) for item in value]

    if isinstance(value, dict):
        return {k: classad_to_python(v) for k, v in value.items()}

    return str(value)


def convert_ad(ad, projection=None):
    """Convert a classad to a plain Python dict.

    Most fields are scalars (already native Python) — pass through.
    Classad types (ExprTree, Value) inherit from scalars via Boost.Python,
    so we check those first to keep them off the fast path.

    If projection is given, only include those keys.
    """
    result = {}
    keys = projection if projection is not None else ad.keys()
    for key in keys:
        try:
            v = ad[key]
        except KeyError:
            continue
        if _HAS_CLASSAD and isinstance(v, (classad.ExprTree, classad.Value)):
            result[key] = classad_to_python(v)
        elif isinstance(v, _SCALAR_TYPES):
            result[key] = v
        else:
            result[key] = classad_to_python(v)
    return result
