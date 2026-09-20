"""What a text that is not trusted may cost: the time to read it.

The Go port has the same tests, ``jsonpath/bounds_test.go``, and the Rust port
``tests/jsonpath.rs``.
"""
import time
import unittest
from typing import Any

from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    JSONPathSyntaxError, parse,
)


def balanced(depth: int) -> str:
    """Comparisons with a parameter each, two to the power of ``depth`` of
    them, grouped in halves: a wide tree that is neither tall nor deep."""
    if depth == 0:
        return "@.a == %d"
    return "(%s && %s)" % (balanced(depth - 1), balanced(depth - 1))


class TestATemplateIsReadInTheTimeItTakesToReadIt(unittest.TestCase):
    """The builder of a placeholder's value counted the placeholders among
    the tokens before it, for each placeholder, so a template was parsed in a
    time that grew as the square of their number: two thousand in 0.2 s,
    eight thousand in 3.4 s, thirty-two thousand in 58 s.
    """

    def test_a_template_of_many_parameters(self):
        # 16384 of them; with the square it took a minute
        template = "$[?%s]" % balanced(14)
        started = time.monotonic()
        specification = parse(template)
        self.assertLess(time.monotonic() - started, 10)
        self.assertIs(specification.match(Record(a=1), (1,) * 2 ** 14), True)

    def test_a_long_text_that_is_refused(self):
        long = "$[?@.a == %d" + " @.a %d" * 60000 + "]"
        started = time.monotonic()
        with self.assertRaises(JSONPathSyntaxError) as raised:
            parse(long)
        self.assertEqual(raised.exception.message, "Expected ']'")
        self.assertLess(time.monotonic() - started, 10)


class Record:
    """A candidate with members given by name."""

    def __init__(self, **members: Any):
        self._members = members

    def get(self, key: str) -> Any:
        return self._members[key]




if __name__ == "__main__":
    unittest.main()
