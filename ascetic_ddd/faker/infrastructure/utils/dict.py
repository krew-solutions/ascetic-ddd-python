from collections.abc import MutableMapping

__all__ = ('flatten_dict', 'flatten_dict_gen',)


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str | None = '.') -> MutableMapping:
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key and sep is not None else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _flatten_dict_gen(d: MutableMapping, parent_key: str, sep: str | None):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key and sep is not None else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict_gen(d: MutableMapping, parent_key: str = '', sep: str | None = '.'):
    return dict(_flatten_dict_gen(d, parent_key, sep))
