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
from typing import Any, Dict, Tuple, Union
import re

from ..nodes import (
    And,
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
from ..evaluate_visitor import Context, EvaluateVisitor


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

    TOKEN_PATTERNS = [
        ("LBRACKET", r"\["),
        ("RBRACKET", r"\]"),
        ("LPAREN", r"\("),
        ("RPAREN", r"\)"),
        ("DOT", r"\."),
        ("DOLLAR", r"\$"),
        ("AT", r"@"),
        ("QUESTION", r"\?"),
        ("WILDCARD", r"\*"),
        ("AND", r"&&"),  # RFC 9535: double ampersand
        ("OR", r"\|\|"),  # RFC 9535: double pipe
        ("EQ", r"=="),  # RFC 9535: double equals (must be before single =)
        ("NE", r"!="),  # Must be before NOT to match != as one token
        ("GTE", r">="),
        ("LTE", r"<="),
        ("GT", r">"),
        ("LT", r"<"),
        ("NOT", r"!"),  # RFC 9535: exclamation mark (after !=)
        ("NUMBER", r"-?\d+\.?\d*"),
        ("STRING", r"'[^']*'|\"[^\"]*\""),
        ("PLACEHOLDER", r"%\(\w+\)[sdf]|%[sdf]"),
        ("IDENTIFIER", r"[a-zA-Z_][a-zA-Z0-9_]*"),
        ("WHITESPACE", r"\s+"),
    ]

    def __init__(self, text: str):
        self.text = text
        self.position = 0
        self.tokens = []

    def tokenize(self) -> list[Token]:
        """Tokenize the input text."""
        while self.position < len(self.text):
            matched = False

            for token_type, pattern in self.TOKEN_PATTERNS:
                regex = re.compile(pattern)
                match = regex.match(self.text, self.position)

                if match:
                    value = match.group(0)
                    if token_type != "WHITESPACE":  # Skip whitespace
                        self.tokens.append(Token(token_type, value, self.position))
                    self.position = match.end()
                    matched = True
                    break

            if not matched:
                raise SyntaxError(
                    f"Unexpected character at position {self.position}: "
                    f"{self.text[self.position]}"
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
        self._placeholder_info = []
        self._placeholder_refs = []
        self._placeholder_bind_index = 0
        self._is_wildcard_context = False  # Track if we're in wildcard context

        # Extract placeholders before tokenization
        self._extract_placeholders()

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

    def _parse_expression(
        self, tokens: list[Token], start: int = 0
    ) -> tuple[Visitable, int]:
        """
        Parse a filter expression from tokens.

        Args:
            tokens: List of tokens
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
            # Recursively parse expression inside parentheses
            node, i = self._parse_expression(tokens, i)
            # Skip closing parenthesis
            if i < len(tokens) and tokens[i].type == "RPAREN":
                i += 1
        else:
            # Parse left side (field access or nested wildcard)
            left_node, i = self._parse_field_access(tokens, i)

            # Check if left_node is a Wildcard (nested wildcard case)
            if isinstance(left_node, Wildcard):
                # This is a nested wildcard - return it directly
                node = left_node
            else:
                # Parse operator
                if i >= len(tokens):
                    return left_node, i

                op_token = tokens[i]
                i += 1

                # Parse right side (value)
                right_node, i = self._parse_value(tokens, i)

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
                    raise SyntaxError(f"Unexpected operator: {op_token.type}")

            # Skip closing parenthesis if present (from earlier opening)
            if i < len(tokens) and tokens[i].type == "RPAREN":
                i += 1

        # Apply NOT if present
        if has_not:
            node = Not(node)

        # Check for AND/OR (RFC 9535: && and ||)
        if i < len(tokens) and tokens[i].type in ("AND", "OR"):
            op = tokens[i].type
            i += 1
            right_expr, i = self._parse_expression(tokens, i)
            if op == "AND":
                node = And(node, right_expr)
            else:
                node = Or(node, right_expr)

        return node, i

    def _parse_field_access(
        self, tokens: list[Token], start: int
    ) -> tuple[Visitable, int]:
        """
        Parse field access expression (including nested paths and wildcards).

        Supports:
        - Simple: @.field
        - Nested: @.a.b.c
        - Nested wildcard: @.items[*][?@.price > 100]

        Args:
            tokens: List of tokens
            start: Starting position

        Returns:
            (Field node or Wildcard node, next position)
        """
        i = start

        # Check for @ (current item)
        if i < len(tokens) and tokens[i].type == "AT":
            i += 1
            # Use Item() only in wildcard context, otherwise GlobalScope()
            parent = Item() if self._is_wildcard_context else GlobalScope()
        else:
            parent = GlobalScope()

        # Skip dot
        if i < len(tokens) and tokens[i].type == "DOT":
            i += 1

        # Parse field path chain (e.g., a.b.c)
        field_chain = []
        while i < len(tokens) and tokens[i].type == "IDENTIFIER":
            field_chain.append(tokens[i].value)
            i += 1

            # Check for dot (continues path)
            if i < len(tokens) and tokens[i].type == "DOT":
                # Check if next token is also an identifier
                if i + 1 < len(tokens) and tokens[i + 1].type == "IDENTIFIER":
                    i += 1  # Skip dot
                    continue
                else:
                    # Dot but no identifier after - break
                    break
            else:
                # No dot, break
                break

        if not field_chain:
            raise SyntaxError(f"Expected field name at position {i}")

        # Check for nested wildcard on last field: field[*][?...]
        if len(field_chain) > 0 and self._check_nested_wildcard(tokens, i):
            # Build parent chain for all fields except the last
            for field in field_chain[:-1]:
                parent = Object(parent, field)

            # Last field is the collection for the wildcard
            collection_name = field_chain[-1]
            return self._parse_nested_wildcard(tokens, i, parent, collection_name)

        # Build nested Field structure
        # e.g., ["a", "b", "c"] -> Field(Object(Object(parent, "a"), "b"), "c")
        for field in field_chain[:-1]:
            parent = Object(parent, field)

        # Last field
        field_name = field_chain[-1]
        return Field(parent, field_name), i

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
        i = start

        # Check for [*]
        if (
            i + 2 < len(tokens)
            and tokens[i].type == "LBRACKET"
            and tokens[i + 1].type == "WILDCARD"
            and tokens[i + 2].type == "RBRACKET"
        ):
            # Check if followed by [?...]
            if (
                i + 3 < len(tokens)
                and tokens[i + 3].type == "LBRACKET"
            ):
                return True

        return False

    def _parse_nested_wildcard(
        self, tokens: list[Token], start: int, parent: Visitable, collection_name: str
    ) -> tuple[Wildcard, int]:
        """
        Parse nested wildcard pattern: collection[*][?predicate]

        Args:
            tokens: List of tokens
            start: Position after collection name
            parent: Parent node (Item or GlobalScope)
            collection_name: Name of the collection field

        Returns:
            (Wildcard node, next position)
        """
        i = start

        # Skip [*]
        if (
            i + 2 < len(tokens)
            and tokens[i].type == "LBRACKET"
            and tokens[i + 1].type == "WILDCARD"
            and tokens[i + 2].type == "RBRACKET"
        ):
            i += 3
        else:
            raise SyntaxError(f"Expected [*] at position {i}")

        # Parse filter expression [?...]
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            # Save current wildcard context
            old_context = self._is_wildcard_context

            # Set wildcard context to True for nested predicate
            self._is_wildcard_context = True
            predicate, i = self._parse_expression(tokens, i)

            # Restore previous context
            self._is_wildcard_context = old_context

            # Create Wildcard node
            collection_obj = Object(parent, collection_name)
            return Wildcard(collection_obj, predicate), i

        raise SyntaxError(f"Expected filter expression at position {i}")

    def _parse_value(self, tokens: list[Token], start: int) -> tuple[Value, int]:
        """
        Parse a value (literal or placeholder).

        Args:
            tokens: List of tokens
            start: Starting position

        Returns:
            (Value node, next position)
        """
        i = start

        if i >= len(tokens):
            raise SyntaxError("Expected value but reached end of tokens")

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
            value_node = self._create_placeholder_value(token.value)
            return value_node, i + 1

        elif token.type == "IDENTIFIER":
            # Could be a boolean literal
            if token.value.lower() == "true":
                return Value(True), i + 1
            elif token.value.lower() == "false":
                return Value(False), i + 1
            elif token.value.lower() == "null":
                return Value(None), i + 1

        raise SyntaxError(f"Unexpected token in value position: {token}")

    def _create_placeholder_value(self, placeholder_str: str) -> Value:
        """
        Create a placeholder value that will be bound later.

        Args:
            placeholder_str: Placeholder string (e.g., %d, %(name)s)

        Returns:
            Value node with placeholder marker
        """
        # We'll store a special marker that we'll replace during match()
        value = Value(("__PLACEHOLDER__", self._placeholder_bind_index))
        self._placeholder_bind_index += 1
        return value

    def _parse_path(self, tokens: list[Token]) -> tuple[Visitable, bool]:
        """
        Parse the full JSONPath expression (supports nested paths).

        Supports:
        - Simple: $.items[?@.price > 100]
        - Nested: $.store.items[?@.price > 100]
        - Deep nested: $.a.b.c.items[?@.x > 1]

        Args:
            tokens: List of tokens

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
        path_chain = []
        while i < len(tokens) and tokens[i].type == "IDENTIFIER":
            path_chain.append(tokens[i].value)
            i += 1

            # Check for dot (continues path)
            if i < len(tokens) and tokens[i].type == "DOT":
                i += 1
                # Continue to next identifier
            else:
                # No more dots, break
                break

        if not path_chain:
            # No path found, check if it's just a filter without path
            # e.g., $[?@.age > 25]
            if i < len(tokens) and tokens[i].type == "LBRACKET":
                # Simple filter without path
                self._is_wildcard_context = False
                predicate, _ = self._parse_expression(tokens, i)
                return predicate, False
            raise SyntaxError("Expected path or filter expression")

        # Build nested Object structure from path chain
        # e.g., ["a", "b", "c"] -> Object(Object(Object(GlobalScope(), "a"), "b"), "c")
        parent = GlobalScope()
        for path_element in path_chain[:-1]:
            parent = Object(parent, path_element)

        # Last element in path is the collection name
        collection_name = path_chain[-1]

        # Check for wildcard [*]
        is_wildcard = False
        if (
            i + 2 < len(tokens)
            and tokens[i].type == "LBRACKET"
            and tokens[i + 1].type == "WILDCARD"
            and tokens[i + 2].type == "RBRACKET"
        ):
            is_wildcard = True
            i += 3

        # Parse filter expression
        if i < len(tokens) and tokens[i].type == "LBRACKET":
            if is_wildcard:
                # Wildcard with filter
                self._is_wildcard_context = True
                predicate, _ = self._parse_expression(tokens, i)
                self._is_wildcard_context = False

                # Create Wildcard node
                collection_obj = Object(parent, collection_name)
                return Wildcard(collection_obj, predicate), True
            else:
                # Simple filter without wildcard
                self._is_wildcard_context = False
                predicate, _ = self._parse_expression(tokens, i)
                return predicate, False

        raise SyntaxError("Expected filter expression")

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
                            return params[param_idx]
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
            raise TypeError(
                f"Data must implement Context protocol (have a 'get' method), "
                f"got {type(data).__name__}"
            )

        # Reset placeholder binding index
        self._placeholder_bind_index = 0

        # Tokenize
        lexer = Lexer(self.template)
        tokens = lexer.tokenize()

        # Parse to AST
        spec_ast, is_wildcard = self._parse_path(tokens)

        # Bind placeholder values
        bound_ast = self._bind_values_in_ast(spec_ast, params)

        # Evaluate using EvaluateVisitor
        visitor = EvaluateVisitor(data)
        bound_ast.accept(visitor)

        return visitor.result()


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
