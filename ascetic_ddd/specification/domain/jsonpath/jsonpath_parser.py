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
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Union

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
)
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
            # Show the expression with a pointer to the error position
            parts.append(f"\n  {self.expression}")
            parts.append(f"\n  {' ' * self.position}^")

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


@dataclass
class _ParseContext:
    """
    Mutable parsing context passed through parser methods.

    Using a context object instead of instance variables makes the parser
    thread-safe and enables concurrent parsing of different templates.
    """
    placeholder_bind_index: int = field(default=0)
    is_wildcard_context: bool = field(default=False)


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
        ("NUMBER", re.compile(r"-?\d+\.?\d*")),
        ("STRING", re.compile(r"'[^']*'|\"[^\"]*\"")),
        ("PLACEHOLDER", re.compile(r"%\(\w+\)[sdf]|%[sdf]")),
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

            if not matched:
                raise JSONPathSyntaxError(
                    f"Unexpected character '{self.text[self.position]}'",
                    position=self.position,
                    expression=self.text,
                    context="expected valid token",
                )

        return self.tokens


class NativeParametrizedSpecification:
    """
    Native JSONPath specification parser without external dependencies.

    Parses template once, binds different values at execution time.
    """

    def __init__(self, template: str):
        """
        Parse JSONPath template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders
        """
        self.template = template
        self._placeholder_info: list[dict] = []

        # Extract placeholders before tokenization
        self._extract_placeholders()

        # Parse AST once at initialization (cached for all match() calls)
        # Context is created locally - no mutable instance state
        lexer = Lexer(template)
        tokens = lexer.tokenize()
        ctx = _ParseContext()
        self._ast, self._is_wildcard = self._parse_path(tokens, ctx)

    def _extract_placeholders(self):
        """Extract placeholder information from template."""
        # Find named placeholders: %(name)s, %(age)d, %(price)f
        named_pattern = r"%\((\w+)\)([sdf])"
        for match in re.finditer(named_pattern, self.template):
            name = match.group(1)
            format_type = match.group(2)
            self._placeholder_info.append(
                {
                    "name": name,
                    "format_type": format_type,
                    "positional": False,
                }
            )

        # Find positional placeholders: %s, %d, %f
        # Create a temp string without named placeholders
        temp = re.sub(named_pattern, "", self.template)
        positional_pattern = r"%([sdf])"
        position = 0
        for match in re.finditer(positional_pattern, temp):
            format_type = match.group(1)
            self._placeholder_info.append(
                {
                    "name": str(position),
                    "format_type": format_type,
                    "positional": True,
                }
            )
            position += 1

    def _parse_primary(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[Visitable, int]:
        """
        Parse a primary expression (comparison, NOT, or parenthesized expression).

        Does NOT handle AND/OR operators - those are handled by _parse_expression
        to ensure left-associativity.

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (Visitable node, next position)
        """
        i = start

        # Skip opening bracket if present
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            i += 1

        # Skip question mark if present
        if i < len(tokens) and tokens[i].type == "QUESTION":
            i += 1

        # Check for NOT operator (RFC 9535: !)
        has_not = False
        if i < len(tokens) and tokens[i].type == "NOT":
            has_not = True
            i += 1

        # Skip opening parenthesis if present
        if i < len(tokens) and tokens[i].type == "LPAREN":
            i += 1
            # Recursively parse FULL expression inside parentheses (can have && and ||)
            node, i = self._parse_expression(tokens, ctx, i)
            # Skip closing parenthesis
            if i < len(tokens) and tokens[i].type == "RPAREN":
                i += 1
        else:
            # Parse left side (field access or nested wildcard)
            left_node, i = self._parse_field_access(tokens, ctx, i)

            # Check if left_node is a Wildcard (nested wildcard case)
            if isinstance(left_node, Wildcard):
                # This is a nested wildcard - return it directly
                node = left_node
            else:
                # Parse operator
                if i >= len(tokens):
                    if has_not:
                        return Not(left_node), i
                    return left_node, i

                op_token = tokens[i]
                i += 1

                # Parse right side (value)
                right_node, i = self._parse_value(tokens, ctx, i)

                # Create comparison node
                if op_token.type == "EQ":
                    node = Equal(left_node, right_node)
                elif op_token.type == "NE":
                    node = NotEqual(left_node, right_node)
                elif op_token.type == "GT":
                    node = GreaterThan(left_node, right_node)
                elif op_token.type == "LT":
                    node = LessThan(left_node, right_node)
                elif op_token.type == "GTE":
                    node = GreaterThanEqual(left_node, right_node)
                elif op_token.type == "LTE":
                    node = LessThanEqual(left_node, right_node)
                else:
                    raise JSONPathSyntaxError(
                        f"Unexpected operator '{op_token.value}'",
                        position=op_token.position,
                        expression=self.template,
                        context="expected comparison operator (==, !=, <, >, <=, >=)",
                    )

            # Skip closing parenthesis if present (from earlier opening)
            if i < len(tokens) and tokens[i].type == "RPAREN":
                i += 1

        # Apply NOT if present
        if has_not:
            node = Not(node)

        return node, i

    def _parse_and_expression(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[Visitable, int]:
        """
        Parse AND expressions with left-associativity.

        AND (&&) has higher precedence than OR (||), so it binds tighter.
        `a && b && c` becomes `And(And(a, b), c)`.

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (Visitable node, next position)
        """
        # Parse first primary expression
        node, i = self._parse_primary(tokens, ctx, start)

        # Handle && with left associativity
        while i < len(tokens) and tokens[i].type == "AND":
            i += 1
            right_node, i = self._parse_primary(tokens, ctx, i)
            node = And(node, right_node)

        return node, i

    def _parse_expression(
        self, tokens: list[Token], ctx: _ParseContext, start: int = 0
    ) -> tuple[Visitable, int]:
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
            (Visitable node, next position)
        """
        # Parse first AND expression (higher precedence)
        node, i = self._parse_and_expression(tokens, ctx, start)

        # Handle || with left associativity
        while i < len(tokens) and tokens[i].type == "OR":
            i += 1
            right_node, i = self._parse_and_expression(tokens, ctx, i)
            node = Or(node, right_node)

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
    ) -> tuple[Visitable, int]:
        """
        Parse field access expression (including nested paths and wildcards).

        Supports:
        - Simple: @.field
        - Nested: @.a.b.c
        - Nested wildcard: @.items[*][?@.price > 100]

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (Field node or Wildcard node, next position)
        """
        i = start

        # Check for @ (current item)
        if i < len(tokens) and tokens[i].type == "AT":
            i += 1
            # Use Item() only in wildcard context, otherwise GlobalScope()
            parent: EmptiableObject = Item() if ctx.is_wildcard_context else GlobalScope()
        else:
            parent = GlobalScope()

        # Skip dot
        if i < len(tokens) and tokens[i].type == "DOT":
            i += 1

        # Parse field path chain (e.g., a.b.c)
        field_chain, i = self._parse_identifier_chain(tokens, i)

        if not field_chain:
            pos = tokens[i].position if i < len(tokens) else len(self.template)
            raise JSONPathSyntaxError(
                "Expected field name",
                position=pos,
                expression=self.template,
                context="after '@.' or '.'",
            )

        # Check for nested wildcard on last field: field[*][?...]
        if self._check_nested_wildcard(tokens, i):
            # Build parent chain for all fields except the last
            parent = self._build_object_chain(parent, field_chain[:-1])
            collection_name = field_chain[-1]
            return self._parse_nested_wildcard(tokens, ctx, i, parent, collection_name)

        # Build nested Field structure: a.b.c -> Field(Object(Object(parent, "a"), "b"), "c")
        parent = self._build_object_chain(parent, field_chain[:-1])
        return Field(parent, field_chain[-1]), i

    def _check_nested_wildcard(self, tokens: list[Token], start: int) -> bool:
        """
        Check if tokens starting at position indicate a nested wildcard pattern.

        Pattern: [*][?...]

        Args:
            tokens: List of tokens
            start: Starting position

        Returns:
            True if nested wildcard pattern detected
        """
        # Check for [*] followed by [?...]
        return (
            self._is_wildcard_pattern(tokens, start)
            and start + 3 < len(tokens)
            and tokens[start + 3].type == "LBRACKET"
        )

    def _parse_nested_wildcard(
        self, tokens: list[Token], ctx: _ParseContext, start: int,
        parent: EmptiableObject, collection_name: str
    ) -> tuple[Wildcard, int]:
        """
        Parse nested wildcard pattern: collection[*][?predicate]

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Position after collection name
            parent: Parent node (Item or GlobalScope)
            collection_name: Name of the collection field

        Returns:
            (Wildcard node, next position)
        """
        i = start

        # Skip [*]
        if self._is_wildcard_pattern(tokens, i):
            i += 3
        else:
            pos = tokens[i].position if i < len(tokens) else len(self.template)
            raise JSONPathSyntaxError(
                "Expected wildcard '[*]'",
                position=pos,
                expression=self.template,
                context="in nested wildcard pattern",
            )

        # Parse filter expression [?...]
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            # Save current wildcard context
            old_context = ctx.is_wildcard_context

            # Set wildcard context to True for nested predicate
            ctx.is_wildcard_context = True
            predicate, i = self._parse_expression(tokens, ctx, i)

            # Restore previous context
            ctx.is_wildcard_context = old_context

            # Create Wildcard node
            collection_obj = Object(parent, collection_name)
            return Wildcard(collection_obj, predicate), i

        pos = tokens[i].position if i < len(tokens) else len(self.template)
        raise JSONPathSyntaxError(
            "Expected filter expression '[?...]'",
            position=pos,
            expression=self.template,
            context="after wildcard '[*]'",
        )

    def _parse_value(
        self, tokens: list[Token], ctx: _ParseContext, start: int
    ) -> tuple[Value, int]:
        """
        Parse a value (literal or placeholder).

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)
            start: Starting position

        Returns:
            (Value node, next position)
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
            value = float(token.value) if "." in token.value else int(token.value)
            return Value(value), i + 1

        elif token.type == "STRING":
            # Parse string (remove quotes)
            value = token.value[1:-1]
            return Value(value), i + 1

        elif token.type == "PLACEHOLDER":
            # This is a placeholder - will be bound later
            # Return a special marker value
            value_node = self._create_placeholder_value(ctx)
            return value_node, i + 1

        elif token.type == "IDENTIFIER":
            # Could be a boolean literal
            if token.value.lower() == "true":
                return Value(True), i + 1
            elif token.value.lower() == "false":
                return Value(False), i + 1
            elif token.value.lower() == "null":
                return Value(None), i + 1

        raise JSONPathSyntaxError(
            f"Unexpected token '{token.value}'",
            position=token.position,
            expression=self.template,
            context="expected value (number, string, boolean, or placeholder)",
        )

    def _create_placeholder_value(self, ctx: _ParseContext) -> Value:
        """
        Create a placeholder value that will be bound later.

        Args:
            ctx: Parse context (mutable state)

        Returns:
            Value node with placeholder marker
        """
        # We'll store a special marker that we'll replace during match()
        value = Value(("__PLACEHOLDER__", ctx.placeholder_bind_index))
        ctx.placeholder_bind_index += 1
        return value

    def _parse_path(
        self, tokens: list[Token], ctx: _ParseContext
    ) -> tuple[Visitable, bool]:
        """
        Parse the full JSONPath expression (supports nested paths).

        Supports:
        - Simple: $.items[?@.price > 100]
        - Nested: $.store.items[?@.price > 100]
        - Deep nested: $.a.b.c.items[?@.x > 1]

        Args:
            tokens: List of tokens
            ctx: Parse context (mutable state)

        Returns:
            (Visitable node, is_wildcard)
        """
        i = 0

        # Skip $
        if i < len(tokens) and tokens[i].type == "DOLLAR":
            i += 1

        # Skip .
        if i < len(tokens) and tokens[i].type == "DOT":
            i += 1

        # Parse path chain (e.g., a.b.c)
        path_chain, i = self._parse_identifier_chain(tokens, i)

        if not path_chain:
            # No path found, check if it's just a filter without path
            # e.g., $[?@.age > 25]
            if i < len(tokens) and tokens[i].type == "LBRACKET":
                # Simple filter without path
                ctx.is_wildcard_context = False
                predicate, _ = self._parse_expression(tokens, ctx, i)
                return predicate, False
            pos = tokens[i].position if i < len(tokens) else len(self.template)
            raise JSONPathSyntaxError(
                "Expected path or filter expression",
                position=pos,
                expression=self.template,
                context="after '$'",
            )

        # Build parent chain and get collection name
        parent = self._build_object_chain(GlobalScope(), path_chain[:-1])
        collection_name = path_chain[-1]

        # Check for wildcard [*]
        is_wildcard = self._is_wildcard_pattern(tokens, i)
        if is_wildcard:
            i += 3

        # Parse filter expression
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            if is_wildcard:
                # Wildcard with filter
                ctx.is_wildcard_context = True
                predicate, _ = self._parse_expression(tokens, ctx, i)
                ctx.is_wildcard_context = False

                # Create Wildcard node
                collection_obj = Object(parent, collection_name)
                return Wildcard(collection_obj, predicate), True
            else:
                # Simple filter without wildcard
                ctx.is_wildcard_context = False
                predicate, _ = self._parse_expression(tokens, ctx, i)
                return predicate, False

        pos = tokens[i].position if i < len(tokens) else len(self.template)
        raise JSONPathSyntaxError(
            "Expected filter expression '[?...]'",
            position=pos,
            expression=self.template,
            context="after path",
        )

    def _bind_placeholder(
        self, value: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]
    ) -> Any:
        """
        Bind a placeholder to its actual value.

        Args:
            value: Value (may contain placeholder marker)
            params: Parameter values

        Returns:
            Actual value
        """
        if isinstance(value, tuple) and len(value) == 2:
            marker, idx = value
            if marker == "__PLACEHOLDER__":
                if idx < len(self._placeholder_info):
                    ph_info = self._placeholder_info[idx]

                    # Get actual value from params
                    if ph_info["positional"]:
                        param_idx = int(ph_info["name"])
                        if param_idx < len(params):
                            return params[param_idx]  # type: ignore[index]
                    else:
                        if ph_info["name"] in params:
                            return params[ph_info["name"]]

                    # If not found, return marker as-is
                    return value

        return value

    def _bind_values_in_ast(
        self, node: Visitable, params: Union[Tuple[Any, ...], Dict[str, Any]]
    ) -> Visitable:
        """
        Recursively bind placeholder values in the AST.

        Args:
            node: AST node
            params: Parameter values

        Returns:
            AST node with bound values
        """
        if isinstance(node, Value):
            # Bind the value if it's a placeholder
            bound_value = self._bind_placeholder(node.value(), params)
            return Value(bound_value)

        elif isinstance(node, (Equal, NotEqual, GreaterThan, LessThan, GreaterThanEqual, LessThanEqual)):
            # Recursively bind left and right
            left = self._bind_values_in_ast(node.left(), params)
            right = self._bind_values_in_ast(node.right(), params)
            return type(node)(left, right)

        elif isinstance(node, (And, Or)):
            left = self._bind_values_in_ast(node.left(), params)
            right = self._bind_values_in_ast(node.right(), params)
            return type(node)(left, right)

        elif isinstance(node, Not):
            operand = self._bind_values_in_ast(node.operand(), params)
            return Not(operand)

        elif isinstance(node, Wildcard):
            # Recursively bind predicate
            predicate = self._bind_values_in_ast(node.predicate(), params)
            return Wildcard(node.parent(), predicate)

        # For other nodes (Field, Item, GlobalScope, Object), return as-is
        return node

    def match(
        self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]] = ()
    ) -> bool:
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
        bound_ast = self._bind_values_in_ast(self._ast, params)

        # Evaluate using EvaluateVisitor
        visitor = EvaluateVisitor(data)
        result = bound_ast.accept(visitor)

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
