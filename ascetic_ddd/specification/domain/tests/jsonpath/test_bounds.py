"""What a text that is not trusted may cost: the time to read it, and a tree
within its bounds.

The Go port has the same tests, ``jsonpath/bounds_test.go``, and the Rust port
``tests/jsonpath.rs``: a template is one in every port, or in none.
"""
import sys
import time
import unittest
from typing import Any, Callable

from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    JSONPathSyntaxError, parse,
)
from ascetic_ddd.specification.domain.nodes import (
    EmptiableObject, Field, GlobalScope, Object, Value, Visitable,
)
from ascetic_ddd.specification.domain.tests.describing import describe
from ascetic_ddd.specification.infrastructure.postgresql_visitor import compile_to_sql
from ascetic_ddd.specification.infrastructure.mapping_visitor import (
    IMapping, transform,
)

TOO_DEEP = "Expression is nested too deep"


def height(description: Any) -> int:
    """The levels of the longest way down a tree, by its description: a field
    and a value are a level, an operator or a collection is one above what is
    under it."""
    if description[0] in ("field", "value"):
        return 1
    if description[0] == "any":
        return 1 + height(description[2])
    return 1 + max(height(operand) for operand in description[1:])


def height_of(template: str) -> int:
    return height(describe(parse(template).bind()))


def groups_on_the_left(levels: int, links: Callable[[int], int]) -> str:
    """Groups nested on the left, each the first operand of a chain of `&&`
    that is the first operand of a chain of `||`. How deep the parser is grows
    by one with a group; how tall the tree is, by two chains."""
    inner = "@.a"
    for level in range(levels, 0, -1):
        n = links(level)
        inner = "(%s%s%s)" % (inner, " && @.a" * n, " || @.a" * n)
    return "$[?%s]" % inner


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
        long = "$[?@.a == %d" + " @.a %d" * 30000 + "]"
        started = time.monotonic()
        with self.assertRaises(JSONPathSyntaxError) as raised:
            parse(long)
        self.assertEqual(raised.exception.message, "Expected ']'")
        self.assertLess(time.monotonic() - started, 10)

    def test_a_template_longer_than_the_bound_is_refused_before_it_is_read(self):
        # The bounds on height and nesting bound the shape of a tree and not
        # the size of a text: a text of megabytes was lexed whole before the
        # parser could refuse it, or accepted with a literal of megabytes. The
        # length is the first thing looked at.
        room = "$[?@.a == 1]"
        at_the_bound = "$[?@.a == 1" + " " * (262_144 - len(room)) + "]"
        self.assertEqual(len(at_the_bound.encode()), 262_144)
        self.assertIs(parse(at_the_bound).match(Record(a=1)), True)
        with self.assertRaises(JSONPathSyntaxError) as raised:
            parse(at_the_bound + " ")
        self.assertEqual(raised.exception.message, "Template too long")
        self.assertEqual(raised.exception.context, "at most 262144 bytes of UTF-8, this has 262145")
        # Bytes, not characters: a template is one in every port, or in none.
        with self.assertRaises(JSONPathSyntaxError):
            parse("$[?@.a == '" + "\u00e9" * 131_072 + "']")
        # And it is refused in the time it takes to look at its length.
        started = time.monotonic()
        with self.assertRaises(JSONPathSyntaxError):
            parse("$[?" + " && ".join(["@.a == 1"] * 400_000) + "]")
        self.assertLess(time.monotonic() - started, 1)


class Record:
    """A candidate with members given by name."""

    def __init__(self, **members: Any):
        self._members = members

    def get(self, key: str) -> Any:
        return self._members[key]


class TestTheBoundsAreHeldToTheLevel(unittest.TestCase):
    """There was no bound at all. A tree from a text that is not trusted was
    as tall as the text made it, and every reader of a tree recurses: the
    parser or a reader ended in a RecursionError, which is not the error of a
    template and is not caught as one.
    """

    def assert_too_deep(self, template: str) -> None:
        with self.assertRaises(JSONPathSyntaxError) as raised:
            parse(template)
        self.assertEqual(raised.exception.message, TOO_DEEP)

    def test_a_tree_of_128_levels_and_no_taller(self):
        def links(operands: int) -> str:
            return "@.a" + " && @.a" * (operands - 1)

        self.assertEqual(height_of("$[?%s]" % links(128)), 128)
        self.assert_too_deep("$[?%s]" % links(129))
        # The chain is the LEFT operand of the comparison: it was read before
        # anything knew of an operator over it.
        self.assertEqual(height_of("$[?(%s) == true]" % links(127)), 128)
        self.assert_too_deep("$[?(%s) == true]" % links(128))

    def test_a_parser_32_deep_and_no_deeper(self):
        shapes = {"groups": ("(", ")"), "nots": ("!", ""), "filters": ("@.items[*][?", "]")}
        for name, (opening, closing) in shapes.items():
            with self.subTest(name):
                parse("$[?%s@.a%s]" % (opening * 32, closing * 32))
                self.assert_too_deep("$[?%s@.a%s]" % (opening * 33, closing * 33))

    def test_a_group_on_the_left_counts_towards_the_height(self):
        # As long a chain after each group as a count of how deep the parser
        # is would let through, had it been passed to the right operands alone
        # - which is how the Rust port counted: sixteen thousand levels passed
        # for 128.
        self.assert_too_deep(groups_on_the_left(127, lambda level: 128 - level))

    def test_no_tree_of_a_template_is_taller_than_the_bound(self):
        shapes: list[Callable[[str, str], str]] = [
            lambda inner, links: "(%s%s)" % (inner, links),
            lambda inner, links: "(@.a%s && %s)" % (links, inner),
            lambda inner, links: "!(%s%s)" % (inner, links),
            lambda inner, links: "@.items[*][?%s%s]" % (inner, links),
        ]
        accepted = 0
        for wrap in shapes:
            for levels in (1, 2, 3, 7, 20, 60, 127):
                for length in (0, 1, 5, 40, 63, 64, 126, 127, 128):
                    links = " && @.a" * length + " || @.a" * length
                    inner = "@.a"
                    for _ in range(levels):
                        inner = wrap(inner, links)
                    try:
                        specification = parse("$[?%s]" % inner)
                    except JSONPathSyntaxError as error:
                        self.assertEqual(error.message, TOO_DEEP)
                        continue
                    accepted += 1
                    self.assertLessEqual(height(describe(specification.bind())), 128)
        # Not all refused: the property is of trees that were made.
        self.assertGreater(accepted, 50)


class SameNames(IMapping):
    """A mapping that leaves a member under its name."""

    def attr_node(self, path: list[str]) -> Visitable:
        owner: EmptiableObject = GlobalScope()
        for name in path[:-1]:
            owner = Object(owner, name)
        return Field(owner, path[-1])

    def value_node(self, val: Any) -> Visitable:
        return Value(val)


def frames() -> int:
    """How deep the stack is where this is called."""
    frame, count = sys._getframe(), 0
    while frame is not None:
        frame, count = frame.f_back, count + 1
    return count


class TestTheDeepestTemplateFitsHalfOfTheRecursionLimit(unittest.TestCase):
    """What the bounds are for. Python stops a recursion at a thousand frames
    unless told otherwise, and whoever calls the library has used some of
    them. The deepest templates there can be are parsed, and their trees go
    through everything that reads a tree, in five hundred frames: measured,
    the worst of them takes 418 to evaluate, 293 to compile and 237 to parse.
    """
    BUDGET = 500

    def test_every_reader(self):
        both = "@.a == %d"
        for level in range(32):
            # A comparison is two levels; a filter over a chain of three, four.
            both = "@.items[*][?%s%s]" % (both, " && @.a == %d" * (2 if level >= 30 else 3))
        templates = {
            "$[?@.a == %d" + " && @.a == %d" * 126 + "]": 128,
            "$[?%s@.a == %%d%s]" % ("(" * 32, ")" * 32): 2,
            # A `!` and a group are a level of the parser each
            "$[?%s@.a == %%d%s]" % ("!(" * 16, ")" * 16): 18,
            "$[?%s@.a == %%d%s]" % ("@.items[*][?" * 32, "]" * 32): 34,
            # As deep as the parser goes and as tall as a tree gets, at once
            "$[?%s]" % both: 128,
        }
        record = Record(a=1)
        for _ in range(33):
            record = Record(a=1, items=CollectionContext([record]))
        limit = sys.getrecursionlimit()
        sys.setrecursionlimit(frames() + self.BUDGET)
        try:
            for template, tall in templates.items():
                params = (1,) * template.count("%d")
                specification = parse(template)
                specification.match(record, params)
                tree = specification.bind(params)
                sql, values = compile_to_sql(transform(SameNames(), tree))
                self.assertEqual(len(values), len(params))
                self.assertEqual(height(describe(tree)), tall)
        finally:
            sys.setrecursionlimit(limit)


if __name__ == "__main__":
    unittest.main()
