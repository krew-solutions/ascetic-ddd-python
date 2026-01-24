__all__ = ('Escape', 'escape',)


class Escape:

    _translation_mapping = (
        ("\\", "\\\\"),
        ("\000", "\\0"),
        ('\b', '\\b'),
        ('\n', '\\n'),
        ('\r', '\\r'),
        ('\t', '\\t'),
        ("%", "%%")
    )
    _delimiter = '"'
    _escape_delimiter = '"'
    _max_length = 63

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_{}'.format(k), v)

    def __call__(self, name) -> str:
        name = name.replace(self._delimiter, self._escape_delimiter + self._delimiter)
        for k, v in self._translation_mapping:
            name = name.replace(k, v)
        if len(name) > self._max_length:
            raise ValueError("The length of name {0!r} is more than {1}".format(name, self._max_length))
        return '"%s"' % name


escape = Escape()
