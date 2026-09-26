"""
Native JSONPath parser for Specification Pattern without external dependencies.

Parses RFC 9535 compliant JSONPath expressions with C-style placeholders
(%s, %d, %f, %(name)s) and converts them directly to Specification AST nodes.

RFC 9535 Compliance:
- Uses == for equality (double equals)
- Uses && for logical AND (double ampersand)
- Uses || for logical OR (double pipe)
- Uses ! for logical NOT (exclamation mark)
"""
import decimal
import math
import re
import string
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple, Union

from ascetic_ddd.specification.domain.nodes import (
    And,
    EmptiableObject,
    Equal,
    Field,
    GlobalScope,
    GreaterThan,
    GreaterThanEqual,
    Item,
    LessThan,
    LessThanEqual,
    Not,
    NotEqual,
    Object,
    Or,
    Value,
    Visitable,
    Wildcard,
    equality_or_null_test,
)
from ascetic_ddd.specification.domain.arithmetic import BIGINT_MAX, BIGINT_MIN
from ascetic_ddd.specification.domain.evaluate_visitor import Context, EvaluateVisitor


class JSONPathError(Exception):
    """Base exception for JSONPath parsing and evaluation errors."""
    pass


class JSONPathSyntaxError(JSONPathError):
    """
    Raised when JSONPath expression has invalid syntax.

    Attributes:
        message: Human-readable error description
        position: Character position where error occurred (0-indexed)
        expression: The JSONPath expression being parsed
        context: Additional context about what was expected
    """

    def __init__(
        self,
        message: str,
        position: Optional[int] = None,
        expression: Optional[str] = None,
        context: Optional[str] = None,
    ):
        self.message = message
        self.position = position
        self.expression = expression
        self.context = context
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = [self.message]

        if self.position is not None:
            parts.append(f" at position {self.position}")

        if self.context:
            parts.append(f" ({self.context})")

        if self.expression and self.position is not None:
            # Show the expression with a pointer to the error position; a
            # control character by its escape, so that the message has none.
            shown = "".join(_shown(c) for c in self.expression)
            pointer = len("".join(_shown(c) for c in self.expression[:self.position]))
            parts.append(f"\n  {shown}")
            parts.append(f"\n  {' ' * pointer}^")

        return "".join(parts)


class JSONPathTypeError(JSONPathError):
    """Raised when data doesn't conform to expected type/protocol."""

    def __init__(self, message: str, expected: Optional[str] = None, got: Optional[str] = None):
        self.message = message
        self.expected = expected
        self.got = got
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = [self.message]
        if self.expected and self.got:
            parts.append(f": expected {self.expected}, got {self.got}")
        return "".join(parts)


# What the letter of a placeholder asks of its parameter. Not isinstance: bool
# is a subclass of int, and True is not what `%d` asks for.
_PARAMETER_TYPES: dict[str, tuple[type, ...]] = {
    "d": (int,),
    "f": (int, float, decimal.Decimal),
}

_PARAMETER_KINDS: dict[str, str] = {
    "d": "an integer",
    "f": "a number",
}


def require_parameter_of_kind(format_type: str, name: str, value: Any) -> Any:
    """
    Require a parameter to be what the letter of its placeholder asks for.

    `%d` takes an integer, `%f` a number, `%s` a value of any type, as
    Python's `%s` does - a Value Object goes there. A None fits any. The
    letter used to be read and never used.

    Args:
        format_type: The letter of the placeholder: "s", "d" or "f"
        name: The name of the placeholder, or its position
        value: The parameter bound to it

    Returns:
        The parameter

    Raises:
        JSONPathTypeError: If the parameter is of another type
    """
    if format_type in _PARAMETER_TYPES and value is not None:
        if type(value) not in _PARAMETER_TYPES[format_type]:
            raise JSONPathTypeError(
                "Placeholder %s does not take this parameter" % name,
                expected=_PARAMETER_KINDS[format_type],
                got=type(value).__name__,
            )
    return value


_BACKSLASH = "\\"


# What the character after a backslash stands for in a string (RFC 9535,
# 2.3.5.1). The escape of a code point, a "u" and four hexadecimal digits, is
# read apart: it is not one character long.
_ESCAPES: dict[str, str] = {
    _BACKSLASH: _BACKSLASH,
    "'": "'",
    '"': '"',
    "/": "/",
    "b": chr(0x08),
    "f": chr(0x0C),
    "n": chr(0x0A),
    "r": chr(0x0D),
    "t": chr(0x09),
}


def _hex4(text: str, at: int) -> Optional[int]:
    """
    Read four hexadecimal digits.

    Not ``int(digits, 16)`` alone, which takes a sign, blanks and underscores.

    Args:
        text: The text the digits stand in
        at: Where the first of them is

    Returns:
        The number they spell, None if they are not four hexadecimal digits
    """
    digits = text[at:at + 4]
    if len(digits) == 4 and all(digit in string.hexdigits for digit in digits):
        return int(digits, 16)
    return None


def _read_escape(text: str, at: int) -> Optional[Tuple[str, int]]:
    """
    Read the escape that starts with the backslash at ``at``.

    Args:
        text: The text the escape stands in
        at: Where its backslash is

    Returns:
        (the character it stands for, where the text goes on), None if it is
        not an escape of RFC 9535
    """
    letter = text[at + 1:at + 2]
    if letter in _ESCAPES:
        return _ESCAPES[letter], at + 2
    if letter != "u":
        return None
    high = _hex4(text, at + 2)
    if high is None:
        return None
    if 0xD800 <= high < 0xDC00:
        # A code point beyond the basic plane is a pair of escapes.
        low = _hex4(text, at + 8) if text[at + 6:at + 8] == _BACKSLASH + "u" else None
        if low is None or not 0xDC00 <= low < 0xE000:
            return None
        return chr(0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)), at + 12
    if 0xDC00 <= high < 0xE000:
        # The second half of a pair, without the first.
        return None
    return chr(high), at + 6


def read_string(spelling: str, position: int, expression: str) -> str:
    """
    Read the string a STRING token spells: its quotes off, its escapes read.

    The token used to be read as ``spelling[1:-1]``: a backslash was a
    backslash, so a quote of the kind the string is written in had no spelling.

    Args:
        spelling: The token as it stands in the template, quotes included
        position: Where the token starts in the template
        expression: The template, for an error to show

    Returns:
        The string

    Raises:
        JSONPathSyntaxError: If a backslash is followed by what is not an
            escape, or a control character stands in the string unescaped
    """
    characters: list[str] = []
    at = 1
    end = len(spelling) - 1
    while at < end:
        if spelling[at] != _BACKSLASH:
            # RFC 9535, 2.3.5.1: unescaped, a character of a string is %x20
            # and up. A control character is written as its escape, and a
            # NUL that arrives raw does not get as far as a query.
            if ord(spelling[at]) < 0x20:
                raise JSONPathSyntaxError(
                    "Control character in a string",
                    position=position + at,
                    expression=expression,
                    context="escape it, %s" % _shown(spelling[at]),
                )
            characters.append(spelling[at])
            at += 1
            continue
        escape = _read_escape(spelling, at)
        if escape is None:
            raise JSONPathSyntaxError(
                "Invalid escape",
                position=position + at,
                expression=expression,
                context="expected %s" % ", ".join(
                    [_BACKSLASH + letter for letter in _ESCAPES] + [_BACKSLASH + "uXXXX"]
                ),
            )
        character, at = escape
        characters.append(character)
    return "".join(characters)


def _shown(character: str) -> str:
    """Return a character as an error can show it: a control character by its escape."""
    return character if character.isprintable() else character.encode("unicode_escape").decode("ascii")


def read_number(spelling: str, position: int, expression: str) -> Union[int, float]:
    """
    Read the number a NUMBER token spells.

    An integer, or - with a fraction or an exponent - a float: ``1e3`` is a
    float, as ``1000.0`` is. The evaluator computes an ``int`` as PostgreSQL
    does a ``bigint`` and a ``float`` as a ``double precision``; a literal that
    is neither is refused here and not computed with as if it were.

    Args:
        spelling: The token as it stands in the template
        position: Where the token starts in the template
        expression: The template, for an error to show

    Returns:
        The number

    Raises:
        JSONPathSyntaxError: If the number does not fit
    """
    out_of_range = JSONPathSyntaxError(
        "Number out of range",
        position=position,
        expression=expression,
        context="expected a number that fits",
    )
    if any(mark in spelling for mark in ".eE"):
        # `1e999` parses, to infinity.
        value = float(spelling)
        if math.isinf(value):
            raise out_of_range
        return value
    integer = int(spelling)
    if not BIGINT_MIN <= integer <= BIGINT_MAX:
        raise out_of_range
    return integer


@dataclass(frozen=True)
class _ParseContext:
    """
    Parsing context passed through parser methods.

    Using a context object instead of instance variables makes the parser
    thread-safe and enables concurrent parsing of different templates.

    Immutable: a filter on a collection is parsed with a context of its own,
    made by ``replace``, so there is nothing to restore after it. Which
    placeholder comes next is not kept here either: a placeholder's place is
    a fact of its token (see ``_create_placeholder_value``).
    """
    is_wildcard_context: bool = field(default=False)
    # How deep the parser is: it grows where the parser recurses - at a group,
    # a `!`, the filter of a collection - and nowhere else.
    depth: int = field(default=0)


# How tall the tree of a template may be - the levels of the longest way down
# it - and how deep the parser may go to read one.
#
# Every reader of a tree recurses, so a tree from a text that is not trusted
# was as tall as the text made it, and the parser or a reader ended in a
# RecursionError: not the error of a template, and not caught as one. There
# was no bound at all.
#
# The two are counted apart, for they are not one number. How deep the parser
# is comes down to a rule from the rule that called it. How tall a tree is
# comes up from the trees below it, and grows wherever a node is made - in the
# loop of a chain as well, where the parser does not recurse, and above a left
# operand, which was read before anything knew it would have an operator over
# it. The numbers are those of the Rust port, where they are measured against
# a stack that does not grow: a template is one in every port, or in none.
_MAX_HEIGHT = 128
_MAX_NESTING = 32


# What a template is bound to: a tuple for positional placeholders, a mapping
# for named ones, as Python's `%` takes them.
Params = Union[Tuple[Any, ...], Dict[str, Any]]

# What a rule of the parser returns: not a node, but the function that builds
# the node once the parameters are there.
#
# A template is a translation that waits for its parameters. Kept as a tree,
# it needs a word for "a value comes here later", and a specification has no
# such word: a lambda and a tree built by hand have no placeholders. That word
# was a marker inside a Value, which every reader of the tree took for a
# value, and then a node of its own, which every reader, a user's own
# included, had to have a method to refuse. Kept as a function, it needs
# neither, and a tree comes of it with values in it or does not come at all.
#
# With the function comes how tall the tree it builds is: known when the
# template is parsed, which is when a tree too tall is refused.
class _Builder(NamedTuple):
    build: Callable[[Params], Visitable]
    height: int


def _constant(node: Visitable) -> _Builder:
    """
    Build the same node whatever the parameters: there is no placeholder in it.

    For nodes such as Field, Item, GlobalScope, Object and a literal Value,
    return as-is.

    Args:
        node: The node

    Returns:
        The builder of the node
    """
    return _Builder(lambda params: node, 1)


def _negation(operand: _Builder) -> _Builder:
    """
    Build the negation of what ``operand`` builds.

    Args:
        operand: The builder of the operand

    Returns:
        The builder of the Not node
    """
    return _Builder(lambda params: Not(operand.build(params)), operand.height + 1)


def _comparison(node_class: type, left: _Builder, right: _Builder) -> _Builder:
    """
    Build the comparison of what ``left`` and ``right`` build.

    `@.a == %s` bound to None is the null test, as `@.a == null` is, and so is
    `%s == null`. The rule used to be applied twice, to a literal when the
    template was parsed and to a parameter when it was bound, by a second
    walk of the tree that had to know every kind of node and returned as it
    was a kind it did not. Here both operands are values by the time the
    comparison is made, and the rule is applied once.

    Args:
        node_class: The class of the comparison node
        left: The builder of the left operand
        right: The builder of the right operand

    Returns:
        The builder of the comparison, or of the null test
    """
    return _Builder(
        lambda params: equality_or_null_test(node_class, left.build(params), right.build(params)),
        max(left.height, right.height) + 1,
    )


def _connective(
    node_class: Callable[[Visitable, Visitable], Visitable], left: _Builder, right: _Builder
) -> _Builder:
    """
    Build the conjunction or the disjunction of what ``left`` and ``right`` build.

    Args:
        node_class: And or Or
        left: The builder of the left operand
        right: The builder of the right operand

    Returns:
        The builder of the node
    """
    return _Builder(
        lambda params: node_class(left.build(params), right.build(params)),
        max(left.height, right.height) + 1,
    )


def _some_item(collection: Object, predicate: _Builder) -> _Builder:
    """
    Build "some item of ``collection`` satisfies what ``predicate`` builds".

    Args:
        collection: The collection
        predicate: The builder of the predicate, bound with the rest

    Returns:
        The builder of the Wildcard node
    """
    return _Builder(
        lambda params: Wildcard(collection, predicate.build(params)), predicate.height + 1,
    )


class Token:
    """Represents a token in the JSONPath expression."""

    def __init__(self, type_: str, value: Any, position: int = 0):
        self.type = type_
        self.value = value
        self.position = position

    def __repr__(self):
        return f"Token({self.type}, {self.value!r})"


class Lexer:
    """Tokenizes JSONPath expressions."""

    # Pre-compiled token patterns for performance.
    # Patterns are compiled once at class definition time, not on each tokenize() call.
    TOKEN_PATTERNS = [
        ("LBRACKET", re.compile(r"\[")),
        ("RBRACKET", re.compile(r"\]")),
        ("LPAREN", re.compile(r"\(")),
        ("RPAREN", re.compile(r"\)")),
        ("DOT", re.compile(r"\.")),
        ("DOLLAR", re.compile(r"\$")),
        ("AT", re.compile(r"@")),
        ("QUESTION", re.compile(r"\?")),
        ("WILDCARD", re.compile(r"\*")),
        ("AND", re.compile(r"&&")),  # RFC 9535: double ampersand
        ("OR", re.compile(r"\|\|")),  # RFC 9535: double pipe
        ("EQ", re.compile(r"==")),  # RFC 9535: double equals (must be before single =)
        ("NE", re.compile(r"!=")),  # Must be before NOT to match != as one token
        ("GTE", re.compile(r">=")),
        ("LTE", re.compile(r"<=")),
        ("GT", re.compile(r">")),
        ("LT", re.compile(r"<")),
        ("NOT", re.compile(r"!")),  # RFC 9535: exclamation mark (after !=)
        # RFC 9535: a fraction has digits, so `1.` is a number and a dot;
        # an exponent makes a float, `1e3`. Not `\d`, which in a pattern of
        # `str` is any decimal digit of Unicode.
        ("NUMBER", re.compile(r"-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")),
        # RFC 9535: a backslash takes the character after it along, so a quote
        # of the kind the string is written in can stand inside it. Which
        # escapes there are is `read_string`'s to say, with a position.
        ("STRING", re.compile(
            r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", re.DOTALL
        )),
        # Not `\w`, which in a pattern of `str` is any letter or digit of Unicode.
        ("PLACEHOLDER", re.compile(r"%\([A-Za-z0-9_]+\)[sdf]|%[sdf]")),
        ("IDENTIFIER", re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")),
        ("WHITESPACE", re.compile(r"\s+")),
    ]

    tokens: list[Token]

    def __init__(self, text: str):
        self.text = text
        self.position = 0
        self.tokens = []

    def tokenize(self) -> list[Token]:
        """Tokenize the input text."""
        while self.position < len(self.text):
            matched = False

            for token_type, regex in self.TOKEN_PATTERNS:
                match = regex.match(self.text, self.position)

                if match:
                    value = match.group(0)
                    if token_type != "WHITESPACE":  # Skip whitespace
                        self.tokens.append(Token(token_type, value, self.position))
                    self.position = match.end()
                    matched = True
                    break

            if not matched and self.text[self.position] in "'\"":
                # A quote starts a string or nothing: the pattern of a string
                # did not match, so its closing quote is not there.
                raise JSONPathSyntaxError(
                    "Unterminated string",
                    position=self.position,
                    expression=self.text,
                    context="expected closing quote",
                )

            if not matched:
                raise JSONPathSyntaxError(
                    f"Unexpected character '{_shown(self.text[self.position])}'",
                    position=self.position,
                    expression=self.text,
                    context="expected valid token",
                )

        return self.tokens


class NativeParametrizedSpecification:
    """
    Native JSONPath specification parser without external dependencies.

    Parses template once, binds different values at execution time.

    What is kept of the template is not a tree but a function of the
    parameters (see ``_Builder``): ``bind()`` calls it and returns the
    specification, ``match()`` evaluates that.
    """

    def __init__(self, template: str):
        """
        Parse JSONPath template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders
        """
        self.template = template

        # Parse AST once at initialization (cached for all match() calls),
        # into the function that builds it of the parameters
        # Context is created locally - no mutable instance state
        lexer = Lexer(template)
        tokens = lexer.tokenize()

        # Placeholders are those of the tokens, so one inside a string literal
        # is text, and they are listed in the order they stand, which is the
        # order they are bound in.
        self._placeholder_info: list[dict] = self._extract_placeholders(tokens)

        ctx = _ParseContext()
        self._builder, self._is_wildcard = self._parse_path(tokens, ctx)

    def _extract_placeholders(self, tokens: list[Token]) -> list[dict]:
        """
        Extract placeholder information from the tokens of the template.

        Args:
            tokens: List of tokens

        Returns:
            One entry per placeholder, in the order they stand. A positional
            one is named by its place among the positional ones.

        Raises:
            JSONPathSyntaxError: If positional and named placeholders are
                mixed. ``match()`` takes a tuple or a mapping, as Python's
                ``%`` does, and neither can bind such a template.
        """
        # A template is of one style, so its placeholders are listed in the
        # order they stand: the place of a token among them is its entry.
        # Counted here, once - it used to be counted over the tokens before
        # each placeholder, and the time grew as the square of their number.
        at = [i for i, token in enumerate(tokens) if token.type == "PLACEHOLDER"]
        self._placeholder_at: Dict[int, int] = {i: place for place, i in enumerate(at)}
        placeholders = [tokens[i] for i in at]
        named = [token for token in placeholders if token.value.startswith("%(")]
        positional = [token for token in placeholders if not token.value.startswith("%(")]

        if named and positional:
            raise JSONPathSyntaxError(
                "Positional and named placeholders in one template",
                position=max(named[0].position, positional[0].position),
                expression=self.template,
                context="expected placeholders of one style",
            )

        # Named placeholders: %(name)s, %(age)d, %(price)f
        # Positional placeholders: %s, %d, %f
        return [
            {
                "name": token.value[2:-2],
                "format_type": token.value[-1],
                "positional": False,
            }
            if token.value.startswith("%(") else
            {
                "name": str(position),
                "format_type": token.value[-1],
                "positional": True,
            }
            for position, token in enumerate(placeholders)
        ]

    def _position(self, tokens: list[Token], i: int) -> int:
        """Return the position in the template of the token at ``i``, or of its end."""
        return tokens[i].position if i < len(tokens) else len(self.template)

    def _expect(
        self, tokens: list[Token], i: int, type_: str, message: str, context: str
    ) -> int:
        """
        Require the token at ``i`` to be of ``type_``.

        The grammar has no token that "may be present": a bracket the parser
        stepped over where there was one, and did not miss where there was
        none, let a template with a bracket left open, or one too many, be
        read as if it were well formed.

        Args:
            tokens: List of tokens
            i: Position of the token
            type_: The type the token must be of
            message: What to say if it is not
            context: What was expected

        Returns:
            Next position

        Raises:
            JSONPathSyntaxError: If the token is of another type, or there is none
        """
        if i < len(tokens) and tokens[i].type == type_:
            return i + 1
        raise JSONPathSyntaxError(
            message,
            position=self._position(tokens, i),
            expression=self.template,
            context=context,
        )

    def _too_deep(self, tokens: list[Token], at: int) -> JSONPathSyntaxError:
        """
        Return the refusal of a template beyond either bound.

        Args:
            tokens: List of tokens
            at: Position of the token that asks for one level more
        """
        return JSONPathSyntaxError(
            "Expression is nested too deep",
            position=self._position(tokens, at),
            expression=self.template,
            context="expected a simpler expression",
        )

    def _deeper(self, tokens: list[Token], at: int, ctx: _ParseContext) -> _ParseContext:
        """
        Return the context a level below, if the parser may go that deep.

        Args:
            tokens: List of tokens
            at: Position of the token where the parser recurses
            ctx: Parse context

        Raises:
            JSONPathSyntaxError: If the parser is as deep as it may go
        """
        if ctx.depth >= _MAX_NESTING:
            raise self._too_deep(tokens, at)
        return replace(ctx, depth=ctx.depth + 1)

    def _bounded(self, tokens: list[Token], at: int, node: _Builder) -> _Builder:
        """
        Return the builder of a node, if the tree it builds may be that tall.

        Every node a rule makes goes through it.

        Args:
            tokens: List of tokens
            at: Position of the token that asks for the node
            node: The builder of the node

        Raises:
            JSONPathSyntaxError: If the tree is taller than it may be
        """
        if node.height > _MAX_HEIGHT:
            raise self._too_deep(tokens, at)
        return node

    def _parse_filter(
        self, tokens: list[Token], ctx: _ParseContext, start: int
    ) -> tuple[_Builder, int]:
        """
        Parse a filter: "[" "?" expression "]".

        Args:
            tokens: List of tokens
            ctx: Parse context; says what "@" is inside the filter
            start: Starting position

        Returns:
            (builder of the node, next position)
        """
        message = "Expected filter expression '[?...]'"
        i = self._expect(tokens, start, "LBRACKET", message, "expected '['")
        i = self._expect(tokens, i, "QUESTION", message, "expected '?'")
        node, i = self._parse_expression(tokens, ctx, i)
        i = self._expect(tokens, i, "RBRACKET", "Expected ']'", "expected end of filter expression")
        return node, i

    _COMPARISONS = {
        "EQ": Equal,
        "NE": NotEqual,
        "GT": GreaterThan,
        "LT": LessThan,
        "GTE": GreaterThanEqual,
        "LTE": LessThanEqual,
    }

    def _parse_primary(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[_Builder, int]:
        """
        Parse a primary expression (comparison, NOT, or parenthesized expression).

        Does NOT handle AND/OR operators - those are handled by _parse_expression
        to ensure left-associativity.

        Grammar:
            primary    = "!" primary | comparison
            comparison = operand ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) operand )?

        A comparison has at most one operator: it does not associate.

        Args:
            tokens: List of tokens
            ctx: Parse context
            start: Starting position

        Returns:
            (builder of the node, next position)
        """
        i = start

        # Check for NOT operator (RFC 9535: !)
        if i < len(tokens) and tokens[i].type == "NOT":
            node, after = self._parse_primary(tokens, self._deeper(tokens, i, ctx), i + 1)
            return self._bounded(tokens, i, _negation(node)), after

        # Parse left side (field access, nested wildcard, value or parentheses)
        left_node, i = self._parse_operand(tokens, ctx, i)

        # Parse operator
        if i >= len(tokens) or tokens[i].type not in self._COMPARISONS:
            return left_node, i
        node_class = self._COMPARISONS[tokens[i].type]

        # Parse right side
        right_node, after = self._parse_operand(tokens, ctx, i + 1)

        # Create comparison node; `@.a == null` is the null test
        return self._bounded(tokens, i, _comparison(node_class, left_node, right_node)), after

    def _parse_operand(
        self, tokens: list[Token], ctx: _ParseContext, start: int
    ) -> tuple[_Builder, int]:
        """
        Parse an operand: either side of a comparison, or a test by itself.

        Grammar:
            operand = "(" expression ")" | value | query

        Args:
            tokens: List of tokens
            ctx: Parse context
            start: Starting position

        Returns:
            (builder of the node, next position)
        """
        i = start

        if i < len(tokens) and tokens[i].type == "LPAREN":
            # Recursively parse FULL expression inside parentheses (can have && and ||)
            node, i = self._parse_expression(tokens, self._deeper(tokens, i, ctx), i + 1)
            # The parenthesis that closes this group: an inner primary used to
            # take it for its own, and `(a || b) && c` was read `a || (b && c)`.
            i = self._expect(tokens, i, "RPAREN", "Expected ')'", "expected closing parenthesis")
            return node, i

        if i < len(tokens) and tokens[i].type in ("AT", "DOLLAR"):
            return self._parse_field_access(tokens, ctx, i)

        return self._parse_value(tokens, ctx, i)

    def _parse_and_expression(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[_Builder, int]:
        """
        Parse AND expressions with left-associativity.

        AND (&&) has higher precedence than OR (||), so it binds tighter.
        `a && b && c` becomes `And(And(a, b), c)`.

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (builder of the node, next position)
        """
        # Parse first primary expression
        node, i = self._parse_primary(tokens, ctx, start)

        # Handle && with left associativity
        while i < len(tokens) and tokens[i].type == "AND":
            separator = i
            right_node, i = self._parse_primary(tokens, ctx, i + 1)
            node = self._bounded(tokens, separator, _connective(And, node, right_node))

        return node, i

    def _parse_expression(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[_Builder, int]:
        """
        Parse OR expressions with left-associativity (lowest precedence).

        Operator precedence (highest to lowest):
        1. Comparisons (==, !=, <, >, <=, >=)
        2. NOT (!)
        3. AND (&&)
        4. OR (||)

        This ensures `a || b && c` is parsed as `Or(a, And(b, c))`.

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (builder of the node, next position)
        """
        # Parse first AND expression (higher precedence)
        node, i = self._parse_and_expression(tokens, ctx, start)

        # Handle || with left associativity
        while i < len(tokens) and tokens[i].type == "OR":
            separator = i
            right_node, i = self._parse_and_expression(tokens, ctx, i + 1)
            node = self._bounded(tokens, separator, _connective(Or, node, right_node))

        return node, i

    def _parse_identifier_chain(
        self, tokens: list[Token], start: int
    ) -> tuple[list[str], int]:
        """
        Parse a chain of dot-separated identifiers.

        Examples: "a", "a.b", "a.b.c"

        Args:
            tokens: List of tokens
            start: Starting position

        Returns:
            (list of identifier names, next position)
        """
        i = start
        chain: list[str] = []

        while i < len(tokens) and tokens[i].type == "IDENTIFIER":
            chain.append(tokens[i].value)
            i += 1

            # Check for dot followed by identifier
            if (
                i < len(tokens)
                and tokens[i].type == "DOT"
                and i + 1 < len(tokens)
                and tokens[i + 1].type == "IDENTIFIER"
            ):
                i += 1  # Skip dot, continue to next identifier
            else:
                break

        return chain, i

    def _build_object_chain(self, parent: EmptiableObject, names: list[str]) -> EmptiableObject:
        """
        Build a chain of Object nodes from a list of field names.

        Example: ["a", "b", "c"] with GlobalScope() parent becomes:
            Object(Object(Object(GlobalScope(), "a"), "b"), "c")

        Args:
            parent: Starting parent node
            names: List of field names

        Returns:
            Nested Object node
        """
        result = parent
        for name in names:
            result = Object(result, name)
        return result

    def _is_wildcard_pattern(self, tokens: list[Token], start: int) -> bool:
        """
        Check if tokens at position form a wildcard pattern [*].

        Args:
            tokens: List of tokens
            start: Starting position

        Returns:
            True if [*] pattern found
        """
        return (
            start + 2 < len(tokens)
            and tokens[start].type == "LBRACKET"
            and tokens[start + 1].type == "WILDCARD"
            and tokens[start + 2].type == "RBRACKET"
        )

    def _parse_field_access(
        self, tokens: list[Token], ctx: _ParseContext, start: int
    ) -> tuple[_Builder, int]:
        """
        Parse field access expression (including nested paths and wildcards).

        Supports:
        - Simple: @.field
        - Nested: @.a.b.c
        - Nested wildcard: @.items[*][?@.price > 100]
        - From the candidate, inside a filter as well: $.limit

        Grammar:
            query = ( "@" | "$" ) ( "." name )+ ( "[*]" filter )?

        Args:
            tokens: List of tokens
            ctx: Parse context
            start: Starting position

        Returns:
            (builder of the Field node or of the Wildcard node, next position)
        """
        i = start

        # Check for @ (current item)
        if i < len(tokens) and tokens[i].type == "AT":
            i += 1
            # Use Item() only in wildcard context, otherwise GlobalScope()
            parent: EmptiableObject = Item() if ctx.is_wildcard_context else GlobalScope()
        else:
            # $ (the candidate), whatever filter it stands in
            i = self._expect(tokens, i, "DOLLAR", "Expected '@' or '$'", "expected field access")
            parent = GlobalScope()

        # Parse field path chain (e.g., a.b.c)
        field_chain: list[str] = []
        if i < len(tokens) and tokens[i].type == "DOT":
            field_chain, i = self._parse_identifier_chain(tokens, i + 1)

        if not field_chain:
            raise JSONPathSyntaxError(
                "Expected field name",
                position=self._position(tokens, i),
                expression=self.template,
                context="after '@.' or '$.'",
            )

        # Check for nested wildcard on last field: field[*][?...]
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            # Build parent chain for all fields except the last
            parent = self._build_object_chain(parent, field_chain[:-1])
            collection_name = field_chain[-1]
            return self._parse_nested_wildcard(tokens, ctx, i, parent, collection_name)

        # Build nested Field structure: a.b.c -> Field(Object(Object(parent, "a"), "b"), "c")
        parent = self._build_object_chain(parent, field_chain[:-1])
        return _constant(Field(parent, field_chain[-1])), i

    def _parse_nested_wildcard(
        self, tokens: list[Token], ctx: _ParseContext, start: int,
        parent: EmptiableObject, collection_name: str
    ) -> tuple[_Builder, int]:
        """
        Parse nested wildcard pattern: collection[*][?predicate]

        A filter applies to the items of a collection, so the wildcard is
        required: `collection[?predicate]` used to lose its path and have the
        predicate applied to the candidate.

        Args:
            tokens: List of tokens
            ctx: Parse context
            start: Position after collection name
            parent: Parent node (Item or GlobalScope)
            collection_name: Name of the collection field

        Returns:
            (builder of the Wildcard node, next position)
        """
        i = start

        # Skip [*]
        if self._is_wildcard_pattern(tokens, i):
            i += 3
        else:
            pos = tokens[i + 1].position if i + 1 < len(tokens) else len(self.template)
            raise JSONPathSyntaxError(
                "Expected wildcard '[*]'",
                position=pos,
                expression=self.template,
                context="a filter applies to the items of a collection",
            )

        # Parse filter expression [?...]
        # Set wildcard context to True for nested predicate
        # The predicate is read a level below where the collection stands
        inner = replace(self._deeper(tokens, start, ctx), is_wildcard_context=True)
        predicate, i = self._parse_filter(tokens, inner, i)

        # Create Wildcard node
        collection_obj = Object(parent, collection_name)
        return self._bounded(tokens, start, _some_item(collection_obj, predicate)), i

    def _parse_value(
        self, tokens: list[Token], ctx: _ParseContext, start: int
    ) -> tuple[_Builder, int]:
        """
        Parse a value (literal or placeholder).

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (builder of the Value node, next position)
        """
        i = start

        if i >= len(tokens):
            raise JSONPathSyntaxError(
                "Unexpected end of expression",
                position=len(self.template),
                expression=self.template,
                context="expected value (number, string, boolean, or placeholder)",
            )

        token = tokens[i]

        if token.type == "NUMBER":
            # Parse number
            return _constant(Value(read_number(token.value, token.position, self.template))), i + 1

        elif token.type == "STRING":
            # Parse string (remove quotes, read escapes)
            return _constant(Value(read_string(token.value, token.position, self.template))), i + 1

        elif token.type == "PLACEHOLDER":
            # This is a placeholder - will be bound later
            # Return the builder of the Value it is bound to
            value_node = self._create_placeholder_value(tokens, i)
            return value_node, i + 1

        elif token.type == "IDENTIFIER":
            # Could be a boolean literal
            if token.value.lower() == "true":
                return _constant(Value(True)), i + 1
            elif token.value.lower() == "false":
                return _constant(Value(False)), i + 1
            elif token.value.lower() == "null":
                return _constant(Value(None)), i + 1

        raise JSONPathSyntaxError(
            f"Unexpected token '{token.value}'",
            position=token.position,
            expression=self.template,
            context="expected value (number, string, boolean, or placeholder)",
        )

    def _create_placeholder_value(self, tokens: list[Token], i: int) -> _Builder:
        """
        Create the builder of a value that will be bound later.

        Args:
            tokens: List of tokens
            i: Position of the placeholder token

        Returns:
            Builder of the Value node the placeholder is bound to: by what
            its entry of ``_placeholder_info`` says, which is found by the
            place of the token among the placeholders of the template.
        """
        info = self._placeholder_info[self._placeholder_at[i]]
        # Bind the placeholder: a Value stands where it stood
        return _Builder(lambda params: Value(self._bind_placeholder(info, params)), 1)

    def _parse_path(
        self, tokens: list[Token], ctx: _ParseContext
    ) -> tuple[_Builder, bool]:
        """
        Parse the full JSONPath expression (supports nested paths).

        Supports:
        - Simple: $[?@.age > 25]
        - Collection: $.items[*][?@.price > 100]
        - Nested: $.store.items[*][?@.price > 100]
        - Deep nested: $.a.b.c.items[*][?@.x > 1]

        Grammar:
            template = "$" ( filter | ( "." name )+ "[*]" filter )

        and nothing after it.

        Args:
            tokens: List of tokens
            ctx: Parse context

        Returns:
            (builder of the node, is_wildcard)
        """
        i = self._expect(tokens, 0, "DOLLAR", "Expected '$'", "a template starts at the root")

        # Parse path chain (e.g., a.b.c)
        path_chain: list[str] = []
        if i < len(tokens) and tokens[i].type == "DOT":
            path_chain, i = self._parse_identifier_chain(tokens, i + 1)
            if not path_chain:
                raise JSONPathSyntaxError(
                    "Expected field name",
                    position=self._position(tokens, i),
                    expression=self.template,
                    context="after '$.'",
                )

        node: _Builder
        if not path_chain:
            # No path found, it's just a filter without path
            # e.g., $[?@.age > 25]
            if i >= len(tokens) or tokens[i].type != "LBRACKET":
                raise JSONPathSyntaxError(
                    "Expected path or filter expression",
                    position=self._position(tokens, i),
                    expression=self.template,
                    context="after '$'",
                )
            # Simple filter without path
            node, i = self._parse_filter(tokens, replace(ctx, is_wildcard_context=False), i)
            is_wildcard = False
        else:
            if i >= len(tokens) or tokens[i].type != "LBRACKET":
                raise JSONPathSyntaxError(
                    "Expected filter expression '[?...]'",
                    position=self._position(tokens, i),
                    expression=self.template,
                    context="after path",
                )
            # Build parent chain and get collection name
            parent = self._build_object_chain(GlobalScope(), path_chain[:-1])
            collection_name = path_chain[-1]
            # Wildcard with filter
            node, i = self._parse_nested_wildcard(tokens, ctx, i, parent, collection_name)
            is_wildcard = True

        if i < len(tokens):
            raise JSONPathSyntaxError(
                "Unexpected token '%s'" % tokens[i].value,
                position=tokens[i].position,
                expression=self.template,
                context="expected end of expression",
            )
        return node, is_wildcard

    def _bind_placeholder(self, info: dict, params: Params) -> Any:
        """
        Bind a placeholder to its actual value.

        Args:
            info: The placeholder's entry of ``_placeholder_info``
            params: Parameter values

        Returns:
            Actual value
        """
        # Get actual value from params.
        # A parameter that is not found is an error: it used to
        # leave the marker in the tree, where it compared unequal
        # to everything, and the match was a silent False.
        if info["positional"]:
            param_idx = int(info["name"])
            if isinstance(params, (list, tuple)) and param_idx < len(params):
                return require_parameter_of_kind(
                    info["format_type"], info["name"], params[param_idx],
                )
            raise JSONPathSyntaxError(
                "Missing positional parameter at index %d" % param_idx,
                expression=self.template,
                context="expected %d parameters" % len(self._placeholder_info),
            )
        else:
            if isinstance(params, dict) and info["name"] in params:
                return require_parameter_of_kind(
                    info["format_type"], info["name"], params[info["name"]],
                )
            raise JSONPathSyntaxError(
                "Missing named parameter: %s" % info["name"],
                expression=self.template,
            )

    def bind(self, params: Params = ()) -> Visitable:
        """
        Build the specification the template is of these parameters.

        The tree to evaluate, to transform, to compile to SQL. It has values
        where the template has placeholders; what a parameter is may decide
        what the tree is - a None makes a null test of an equality - so a
        query is compiled of a bound template, and not once for all.

        Args:
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            Specification AST

        Raises:
            JSONPathSyntaxError: If a parameter is missing
            JSONPathTypeError: If a parameter is not what its placeholder takes

        Examples:
            >>> spec = parse("$[?@.age > %d]")
            >>> spec.bind((25,))  # GreaterThan(Field(GlobalScope(), "age"), Value(25))
            >>> spec.bind((None,))  # raises nothing: a None fits any placeholder
        """
        return self._builder.build(params)

    def match(self, data: Any, params: Params = ()) -> bool:
        """
        Check if data matches the specification with given parameters.

        Args:
            data: The data object to check (must implement Context protocol)
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            True if data matches the specification, False otherwise

        Examples:
            >>> spec = parse("$[?(@.age > %d)]")
            >>> user = DictContext({"age": 30})
            >>> spec.match(user, (25,))
            True
        """
        # Check if data implements Context protocol (has 'get' method)
        if not hasattr(data, "get") or not callable(getattr(data, "get")):
            raise JSONPathTypeError(
                "Data must implement Context protocol",
                expected="object with 'get' method",
                got=type(data).__name__,
            )

        # Bind placeholder values to cached AST
        bound_ast = self.bind(params)

        # Evaluate using EvaluateVisitor
        visitor = EvaluateVisitor(data)
        result = bound_ast.accept(visitor)

        # A null is "not satisfied", as a row with a null condition is not
        # selected.
        if result is None:
            return False

        if not isinstance(result, bool):
            raise JSONPathTypeError(
                "Specification did not yield a boolean",
                expected="bool",
                got=type(result).__name__,
            )
        return result


def parse(template: str) -> NativeParametrizedSpecification:
    """
    Parse RFC 9535 compliant JSONPath expression with C-style placeholders (native implementation).

    Args:
        template: JSONPath with %s, %d, %f or %(name)s placeholders

    Returns:
        NativeParametrizedSpecification that can be executed with different parameter values

    Examples:
        >>> spec = parse("$[?@.age > %d]")
        >>> user = DictContext({"age": 30})
        >>> spec.match(user, (25,))
        True

        >>> spec = parse("$[?@.name == %(name)s]")
        >>> user = DictContext({"name": "Alice"})
        >>> spec.match(user, {"name": "Alice"})
        True

        >>> spec = parse("$[?@.age > %d && @.active == %s]")
        >>> user = DictContext({"age": 30, "active": True})
        >>> spec.match(user, (25, True))
        True

        >>> spec = parse("$[?!(@.active == %s)]")
        >>> user = DictContext({"active": False})
        >>> spec.match(user, (True,))
        True
    """
    return NativeParametrizedSpecification(template)
