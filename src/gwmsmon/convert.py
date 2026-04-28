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

    Hot path first: scalar (str/int/float/bool/None) — pass through.
    Slow path: ExprTree / Value / list / dict / fallback.
    """
    if isinstance(value, _SCALAR_TYPES):
        return value

    if _HAS_CLASSAD:
        if isinstance(value, classad.ExprTree):
            try:
                evaluated = value.eval()
            except Exception:
                return None
            return classad_to_python(evaluated)

        if isinstance(value, classad.Value):
            return None

    if isinstance(value, list):
        return [classad_to_python(item) for item in value]

    if isinstance(value, dict):
        return {k: classad_to_python(v) for k, v in value.items()}

    return str(value)


def convert_ad(ad, projection=None):
    """Convert a classad to a plain Python dict.

    Fast path: most fields are scalars (already native Python). Skip
    the recursive walker for those — only call classad_to_python for
    ExprTree/list/dict.

    If projection is given, only include those keys.
    """
    result = {}
    keys = projection if projection else ad.keys()
    for key in keys:
        try:
            v = ad[key]
        except KeyError:
            continue
        if isinstance(v, _SCALAR_TYPES):
            result[key] = v
        else:
            result[key] = classad_to_python(v)
    return result
