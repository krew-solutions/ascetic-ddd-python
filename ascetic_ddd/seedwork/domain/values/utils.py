
__all__ = ("hashable", "freeze", )


def hashable(o):
    if isinstance(o, dict):
        return tuple((k, hashable(v)) for k, v in o.items())
    if isinstance(o, list):
        return tuple([hashable(v) for v in o])
    return o


def freeze(o):
    if isinstance(o, dict):
        return frozenset({k: freeze(v) for k, v in o.items()}.items())
    if isinstance(o, list):
        return frozenset([freeze(v) for v in o])
    return o
