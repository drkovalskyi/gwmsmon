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


def classad_to_python(value):
    """Recursively convert a classad value to a plain Python type.

    - ExprTree: evaluate via .eval(), then convert the result
    - Value.Undefined / Value.Error: None
    - list: recursively convert elements
    - int, float, bool, str, None: pass through
    - everything else: str()
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

    if isinstance(value, list):
        return [classad_to_python(item) for item in value]

    if isinstance(value, dict):
        return {k: classad_to_python(v) for k, v in value.items()}

    if isinstance(value, (bool, int, float, str, type(None))):
        return value

    return str(value)


def convert_ad(ad, projection=None):
    """Convert a classad to a plain Python dict.

    If projection is given, only include those keys.
    """
    result = {}
    keys = projection if projection else ad.keys()
    for key in keys:
        try:
            result[key] = classad_to_python(ad[key])
        except KeyError:
            pass
    return result
