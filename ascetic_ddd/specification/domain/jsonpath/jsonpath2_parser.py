"""
JSONPath parser for Specification Pattern using jsonpath2 library.

Parses JSONPath expressions with C-style placeholders (%s, %d, %f, %(name)s)
and converts them to Specification AST nodes using jsonpath2 library.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Tuple, Union
import re

from jsonpath2.path import Path
from jsonpath2.nodes.subscript import SubscriptNode
from jsonpath2.subscripts.filter import FilterSubscript
from jsonpath2.subscripts.wildcard import WildcardSubscript
from jsonpath2.subscripts.objectindex import ObjectIndexSubscript
from jsonpath2.expressions.operator import (
    BinaryOperatorExpression,
    EqualBinaryOperatorExpression,
    NotEqualBinaryOperatorExpression,
    GreaterThanBinaryOperatorExpression,
    LessThanBinaryOperatorExpression,
    GreaterThanOrEqualToBinaryOperatorExpression,
    LessThanOrEqualToBinaryOperatorExpression,
    AndVariadicOperatorExpression,
    OrVariadicOperatorExpression,
    NotUnaryOperatorExpression,
)
from jsonpath2.expressions.some import SomeExpression
from jsonpath2.nodes.current import CurrentNode

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
from ascetic_ddd.specification.domain.evaluate_visitor import EvaluateVisitor
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    JSONPathTypeError,
)


# Unique placeholder markers (chosen to avoid collision with real data)
PLACEHOLDER_MARKER_INT = -8765432109876
PLACEHOLDER_MARKER_FLOAT = -8765432109876.5
PLACEHOLDER_MARKER_STR = "__JSONPATH_PLACEHOLDER_a1b2c3d4__"


@dataclass
class _PlaceholderInfo:
    """Information about a placeholder in the template."""
    name: str
    format_type: str
    positional: bool


@dataclass
class _ConvertContext:
    """
    Immutable context for AST conversion.

    Using a dataclass instead of instance variables ensures thread-safety
    by avoiding shared mutable state during concurrent match() calls.
    """
    params: Union[Tuple[Any, ...], Dict[str, Any]]
    placeholder_info: List[_PlaceholderInfo]
    in_item_context: bool = False
    placeholder_bind_index: int = 0


# Pre-compiled regex patterns for better performance
_NAMED_PLACEHOLDER_PATTERN = re.compile(r"%\((\w+)\)([sdf])")
_POSITIONAL_PLACEHOLDER_PATTERN = re.compile(r"%([sdf])")


def _iterate_with_string_awareness(template: str) -> Iterator[Tuple[int, str, bool]]:
    """
    Iterate over template characters with string literal awareness.

    Yields:
        Tuple of (index, character, is_inside_string)
    """
    in_string = False
    string_char = None

    for i, char in enumerate(template):
        # Track if we're inside a string literal
        if char in ('"', "'") and (i == 0 or template[i - 1] != '\\'):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None

        yield i, char, in_string


def _get_placeholder_marker(format_type: str) -> str:
    """
    Get the appropriate placeholder marker for a format type.

    Args:
        format_type: 's', 'd', or 'f'

    Returns:
        String representation of the marker for use in JSONPath
    """
    if format_type == "s":
        return f'"{PLACEHOLDER_MARKER_STR}"'
    elif format_type == "d":
        return str(PLACEHOLDER_MARKER_INT)
    elif format_type == "f":
        return str(PLACEHOLDER_MARKER_FLOAT)
    else:
        raise ValueError(f"Unknown format type: {format_type}")


def _is_placeholder_marker(value: Any, format_type: str) -> bool:
    """
    Check if a value is a placeholder marker.

    Args:
        value: The value to check
        format_type: Expected format type ('s', 'd', 'f')

    Returns:
        True if value is the placeholder marker for the given type
    """
    if format_type == "s":
        return value == PLACEHOLDER_MARKER_STR
    elif format_type in ("d", "f"):
        return value == PLACEHOLDER_MARKER_INT or value == PLACEHOLDER_MARKER_FLOAT
    return False


class ParametrizedSpecificationJsonPath2:
    """
    JSONPath specification parser using jsonpath2 library.

    Parses template once in __init__, binds different values at execution time.
    Thread-safe: uses immutable context during match().
    """

    def __init__(self, template: str):
        """
        Parse JSONPath template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders
        """
        self.template = template
        self._placeholder_info: List[_PlaceholderInfo] = []

        # Extract placeholders before parsing
        self._extract_placeholders()

        # Preprocess and cache the template (done once, not on every match)
        self._processed_template = self._preprocess_template()

    def _normalize_equality_operator(self, template: str) -> str:
        """
        Normalize == to = for jsonpath2 library compatibility.

        RFC 9535 standard defines == for equality, but jsonpath2 library
        deviates from the standard and uses single =.
        This method provides better UX by accepting both syntaxes.

        Args:
            template: JSONPath template string

        Returns:
            Normalized template with == replaced by =
        """
        result = []
        i = 0

        for idx, char, in_string in _iterate_with_string_awareness(template):
            if idx < i:
                continue  # Skip already processed characters

            # Replace == with = only outside strings
            if not in_string and char == '=' and idx + 1 < len(template) and template[idx + 1] == '=':
                result.append('=')
                i = idx + 2  # Skip both = characters
                continue

            result.append(char)
            i = idx + 1

        return ''.join(result)

    def _normalize_logical_operators(self, template: str) -> str:
        """
        Normalize RFC 9535 logical operators to jsonpath2 text operators.

        RFC 9535 standard defines: &&, ||, !
        jsonpath2 library uses text operators: and, or, not
        This method normalizes symbol operators to text operators.

        Args:
            template: JSONPath template string

        Returns:
            Normalized template with text logical operators
        """
        result = []
        i = 0

        for idx, char, in_string in _iterate_with_string_awareness(template):
            if idx < i:
                continue  # Skip already processed characters

            if not in_string:
                # Replace && with and
                if char == '&' and idx + 1 < len(template) and template[idx + 1] == '&':
                    result.append(' and ')
                    i = idx + 2
                    continue

                # Replace || with or
                elif char == '|' and idx + 1 < len(template) and template[idx + 1] == '|':
                    result.append(' or ')
                    i = idx + 2
                    continue

                # Replace ! with not (but not in !=)
                elif char == '!' and idx + 1 < len(template) and template[idx + 1] != '=':
                    result.append('not ')
                    i = idx + 1
                    continue

            result.append(char)
            i = idx + 1

        return ''.join(result)

    def _extract_placeholders(self) -> None:
        """Extract placeholder information from template."""
        # Find named placeholders: %(name)s, %(age)d, %(price)f
        for match in _NAMED_PLACEHOLDER_PATTERN.finditer(self.template):
            name = match.group(1)
            format_type = match.group(2)
            self._placeholder_info.append(_PlaceholderInfo(
                name=name,
                format_type=format_type,
                positional=False,
            ))

        # Find positional placeholders: %s, %d, %f
        # Create a temp string without named placeholders
        temp = _NAMED_PLACEHOLDER_PATTERN.sub("", self.template)
        position = 0
        for match in _POSITIONAL_PLACEHOLDER_PATTERN.finditer(temp):
            format_type = match.group(1)
            self._placeholder_info.append(_PlaceholderInfo(
                name=str(position),
                format_type=format_type,
                positional=True,
            ))
            position += 1

    def _add_parentheses_to_filter(self, template: str) -> str:
        """
        Add parentheses around filter expressions if not present.

        jsonpath2 library requires parentheses: $[?(@.age > 25)] not $[?@.age > 25]

        Args:
            template: JSONPath template string

        Returns:
            Template with parentheses added
        """
        result = template

        # Pattern: [? not followed by (
        pattern = re.compile(r'\[\?(?!\()')
        positions = []

        for match in pattern.finditer(result):
            # Check if next char is @ or space then @
            pos = match.end()
            if pos < len(result) and (
                result[pos] == '@' or
                (result[pos] == ' ' and pos + 1 < len(result) and result[pos + 1] == '@')
            ):
                positions.append(match.start())

        # Process from right to left to maintain positions
        for pos in reversed(positions):
            # Find the matching ]
            depth = 1
            i = pos + 2  # After [?
            closing_pos = None

            while i < len(result) and depth > 0:
                if result[i] == '[':
                    depth += 1
                elif result[i] == ']':
                    depth -= 1
                    if depth == 0:
                        closing_pos = i
                        break
                i += 1

            if closing_pos is not None:
                # Insert ) before ]
                result = result[:closing_pos] + ')' + result[closing_pos:]
                # Insert ( after ?
                insert_pos = pos + 2  # After [?
                result = result[:insert_pos] + '(' + result[insert_pos:]

        return result

    def _replace_placeholders(self, template: str) -> str:
        """
        Replace placeholders with unique marker values.

        Args:
            template: Template with placeholders

        Returns:
            Template with placeholders replaced by markers
        """
        processed = template

        # Replace named placeholders
        for match in _NAMED_PLACEHOLDER_PATTERN.finditer(processed):
            placeholder_str = match.group(0)
            format_type = match.group(2)
            replacement = _get_placeholder_marker(format_type)
            processed = processed.replace(placeholder_str, replacement, 1)

        # Replace positional placeholders
        for match in _POSITIONAL_PLACEHOLDER_PATTERN.finditer(processed):
            placeholder_str = match.group(0)
            format_type = match.group(1)
            replacement = _get_placeholder_marker(format_type)
            processed = processed.replace(placeholder_str, replacement, 1)

        return processed

    def _preprocess_template(self) -> str:
        """
        Replace placeholders with temporary markers and normalize operators.

        Returns:
            Processed template string
        """
        processed = self.template

        # Add parentheses to filter expressions (required by jsonpath2)
        processed = self._add_parentheses_to_filter(processed)

        # Normalize == to = for jsonpath2 library compatibility
        processed = self._normalize_equality_operator(processed)

        # Normalize logical operators for jsonpath2 library compatibility
        processed = self._normalize_logical_operators(processed)

        # Replace placeholders with markers
        processed = self._replace_placeholders(processed)

        return processed

    def _contains_wildcard(self, path: Path) -> bool:
        """Check if JSONPath contains wildcard [*]."""
        current_node = path.root_node
        while current_node:
            if isinstance(current_node, SubscriptNode):
                for subscript in current_node.subscripts:
                    if isinstance(subscript, WildcardSubscript):
                        return True
            current_node = getattr(current_node, "next_node", None)
        return False

    def _extract_filter_expression(
        self, path: Path, ctx: _ConvertContext
    ) -> Visitable:
        """
        Extract and convert filter expression from JSONPath to Specification AST.

        Args:
            path: Parsed JSONPath
            ctx: Conversion context

        Returns:
            Specification AST node
        """
        # Check for wildcard
        has_wildcard = self._contains_wildcard(path)

        # Traverse nodes to find collection name and filter
        current_node = path.root_node.next_node  # Skip RootNode
        collection_name = None

        while current_node:
            if isinstance(current_node, SubscriptNode):
                for subscript in current_node.subscripts:
                    if isinstance(subscript, ObjectIndexSubscript):
                        collection_name = subscript.index
                    elif isinstance(subscript, FilterSubscript):
                        if has_wildcard and collection_name:
                            return self._create_wildcard_spec(
                                subscript.expression, collection_name, ctx
                            )
                        else:
                            result, _ = self._convert_expression_to_spec(
                                subscript.expression, ctx
                            )
                            return result

            current_node = getattr(current_node, "next_node", None)

        raise ValueError("No filter expression found in JSONPath")

    def _create_wildcard_spec(
        self,
        expression: Any,
        collection_name: str,
        ctx: _ConvertContext
    ) -> Wildcard:
        """
        Create a Wildcard specification for collection filtering.

        Args:
            expression: Filter expression
            collection_name: Name of the collection field
            ctx: Conversion context

        Returns:
            Wildcard specification node
        """
        # Convert filter with Item context
        item_ctx = _ConvertContext(
            params=ctx.params,
            placeholder_info=ctx.placeholder_info,
            in_item_context=True,
            placeholder_bind_index=ctx.placeholder_bind_index,
        )
        predicate, _ = self._convert_expression_to_spec(expression, item_ctx)

        # Create Wildcard node
        parent = Object(GlobalScope(), collection_name)
        return Wildcard(parent, predicate)

    def _convert_expression_to_spec(
        self,
        expression: Any,
        ctx: _ConvertContext
    ) -> Tuple[Visitable, _ConvertContext]:
        """
        Convert jsonpath2 expression to Specification AST.

        Args:
            expression: JSONPath expression node
            ctx: Conversion context

        Returns:
            Tuple of (Specification AST node, updated context)
        """
        # Handle unary NOT operator
        if isinstance(expression, NotUnaryOperatorExpression):
            operand, ctx = self._convert_expression_to_spec(expression.expression, ctx)
            return Not(operand), ctx

        # Handle variadic operators (AND, OR)
        if isinstance(expression, (AndVariadicOperatorExpression, OrVariadicOperatorExpression)):
            operands = []
            current_ctx = ctx
            for operand in expression.expressions:
                converted, current_ctx = self._convert_expression_to_spec(operand, current_ctx)
                operands.append(converted)

            # Combine with AND or OR (left-associative)
            if isinstance(expression, AndVariadicOperatorExpression):
                result = operands[0]
                for operand in operands[1:]:
                    result = And(result, operand)
                return result, current_ctx
            else:  # OR
                result = operands[0]
                for operand in operands[1:]:
                    result = Or(result, operand)
                return result, current_ctx

        # Handle SomeExpression (nested wildcards)
        if isinstance(expression, SomeExpression):
            return self._convert_some_expression(expression, ctx), ctx

        # Handle binary operators
        if isinstance(expression, BinaryOperatorExpression):
            left, ctx = self._convert_node_or_value(expression.left_node_or_value, ctx)
            right, ctx = self._convert_node_or_value(expression.right_node_or_value, ctx)

            # Map expression type to Specification node
            if isinstance(expression, EqualBinaryOperatorExpression):
                return Equal(left, right), ctx
            elif isinstance(expression, NotEqualBinaryOperatorExpression):
                return NotEqual(left, right), ctx
            elif isinstance(expression, GreaterThanBinaryOperatorExpression):
                return GreaterThan(left, right), ctx
            elif isinstance(expression, LessThanBinaryOperatorExpression):
                return LessThan(left, right), ctx
            elif isinstance(expression, GreaterThanOrEqualToBinaryOperatorExpression):
                return GreaterThanEqual(left, right), ctx
            elif isinstance(expression, LessThanOrEqualToBinaryOperatorExpression):
                return LessThanEqual(left, right), ctx
            else:
                raise ValueError(f"Unsupported binary operator: {type(expression)}")

        raise ValueError(f"Unsupported expression type: {type(expression)}")

    def _convert_some_expression(
        self,
        expression: SomeExpression,
        ctx: _ConvertContext
    ) -> Wildcard:
        """
        Convert SomeExpression (nested wildcard) to Wildcard Specification node.

        SomeExpression represents nested wildcard patterns like:
        @.items[*][?@.price > 500]

        Args:
            expression: SomeExpression from jsonpath2
            ctx: Conversion context

        Returns:
            Wildcard specification node
        """
        # Start with CurrentNode (@)
        current = expression.next_node_or_value

        if not isinstance(current, CurrentNode):
            raise ValueError(f"SomeExpression should start with CurrentNode, got {type(current)}")

        # Move to next node (should be SubscriptNode with collection field)
        current = current.next_node

        if not isinstance(current, SubscriptNode):
            raise ValueError(f"Expected SubscriptNode after CurrentNode, got {type(current)}")

        # Extract collection field name
        if not current.subscripts or not isinstance(current.subscripts[0], ObjectIndexSubscript):
            raise ValueError("Expected ObjectIndexSubscript for collection field")

        collection_name = current.subscripts[0].index

        # Move to next node (should be SubscriptNode with WildcardSubscript)
        current = current.next_node

        if not isinstance(current, SubscriptNode):
            raise ValueError(f"Expected SubscriptNode with wildcard, got {type(current)}")

        if not current.subscripts or not isinstance(current.subscripts[0], WildcardSubscript):
            subscript_type = type(current.subscripts[0]) if current.subscripts else 'no subscripts'
            raise ValueError(f"Expected WildcardSubscript, got {subscript_type}")

        # Move to next node (should be SubscriptNode with FilterSubscript)
        current = current.next_node

        if not isinstance(current, SubscriptNode):
            raise ValueError(f"Expected SubscriptNode with filter, got {type(current)}")

        if not current.subscripts or not isinstance(current.subscripts[0], FilterSubscript):
            subscript_type = type(current.subscripts[0]) if current.subscripts else 'no subscripts'
            raise ValueError(f"Expected FilterSubscript, got {subscript_type}")

        # Extract filter expression (predicate)
        filter_expression = current.subscripts[0].expression

        # Convert filter expression to predicate (in item context)
        item_ctx = _ConvertContext(
            params=ctx.params,
            placeholder_info=ctx.placeholder_info,
            in_item_context=True,
            placeholder_bind_index=ctx.placeholder_bind_index,
        )
        predicate, _ = self._convert_expression_to_spec(filter_expression, item_ctx)

        # Build parent: Item() or GlobalScope() + Object(collection_name)
        parent: EmptiableObject = Item() if ctx.in_item_context else GlobalScope()
        parent = Object(parent, collection_name)

        return Wildcard(parent, predicate)

    def _convert_node_or_value(
        self,
        node_or_value: Any,
        ctx: _ConvertContext
    ) -> Tuple[Visitable, _ConvertContext]:
        """
        Convert jsonpath2 node or value to Specification AST.

        Args:
            node_or_value: JSONPath node or literal value
            ctx: Conversion context

        Returns:
            Tuple of (Specification AST node, updated context)
        """
        # Check if it's a literal value
        if isinstance(node_or_value, (int, float, str, bool, type(None))):
            # Check if it's a placeholder marker
            if ctx.placeholder_bind_index < len(ctx.placeholder_info):
                ph = ctx.placeholder_info[ctx.placeholder_bind_index]

                if _is_placeholder_marker(node_or_value, ph.format_type):
                    # Get actual value from params
                    if ph.positional:
                        param_idx = int(ph.name)
                        if isinstance(ctx.params, (list, tuple)) and param_idx < len(ctx.params):
                            actual_value = ctx.params[param_idx]
                        else:
                            raise ValueError(f"Missing positional parameter at index {param_idx}")
                    else:
                        if isinstance(ctx.params, dict) and ph.name in ctx.params:
                            actual_value = ctx.params[ph.name]
                        else:
                            raise ValueError(f"Missing named parameter: {ph.name}")

                    new_ctx = _ConvertContext(
                        params=ctx.params,
                        placeholder_info=ctx.placeholder_info,
                        in_item_context=ctx.in_item_context,
                        placeholder_bind_index=ctx.placeholder_bind_index + 1,
                    )
                    return Value(actual_value), new_ctx

            return Value(node_or_value), ctx

        # Check if it's a CurrentNode (@)
        if isinstance(node_or_value, CurrentNode):
            field_chain = []
            current = node_or_value.next_node

            # Walk through the chain and collect all field names
            while current and current.__class__.__name__ != 'TerminalNode':
                if isinstance(current, SubscriptNode):
                    if current.subscripts and isinstance(current.subscripts[0], ObjectIndexSubscript):
                        field_chain.append(current.subscripts[0].index)
                        current = getattr(current, "next_node", None)
                    else:
                        break
                else:
                    break

            if field_chain:
                # Build nested Field structure
                parent: EmptiableObject = Item() if ctx.in_item_context else GlobalScope()

                # Build Object chain for all fields except the last
                for field_name in field_chain[:-1]:
                    parent = Object(parent, field_name)

                # Last field
                return Field(parent, field_chain[-1]), ctx

        # Check for nested expression
        if isinstance(node_or_value, (
            BinaryOperatorExpression,
            AndVariadicOperatorExpression,
            OrVariadicOperatorExpression,
            NotUnaryOperatorExpression
        )):
            return self._convert_expression_to_spec(node_or_value, ctx)

        raise ValueError(f"Unsupported node type: {type(node_or_value)}")

    def match(
        self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]] = ()
    ) -> bool:
        """
        Check if data matches the specification with given parameters.

        Thread-safe: uses immutable context during evaluation.

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
                f"Data must implement Context protocol (have a 'get' method)",
                expected="Context protocol",
                got=type(data).__name__
            )

        # Parse the cached preprocessed template
        path = Path.parse_str(self._processed_template)

        # Create immutable context for this match call
        ctx = _ConvertContext(
            params=params,
            placeholder_info=self._placeholder_info,
            in_item_context=False,
            placeholder_bind_index=0,
        )

        # Extract filter expression and convert to Specification AST
        spec_ast = self._extract_filter_expression(path, ctx)

        # Evaluate using EvaluateVisitor
        result = spec_ast.accept(EvaluateVisitor(data))
        if not isinstance(result, bool):
            raise TypeError(
                "Specification did not yield a boolean, got: %s"
                % type(result).__name__
            )
        return result


def parse(template: str) -> ParametrizedSpecificationJsonPath2:
    """
    Parse JSONPath expression with C-style placeholders (jsonpath2 implementation).

    Args:
        template: JSONPath with %s, %d, %f or %(name)s placeholders

    Returns:
        ParametrizedSpecificationJsonPath2 that can be executed with different parameter values

    Examples:
        >>> spec = parse("$[?(@.age > %d)]")
        >>> user = DictContext({"age": 30})
        >>> spec.match(user, (25,))
        True

        >>> spec = parse("$[?(@.name = %(name)s)]")
        >>> user = DictContext({"name": "Alice"})
        >>> spec.match(user, {"name": "Alice"})
        True
    """
    return ParametrizedSpecificationJsonPath2(template)
