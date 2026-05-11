"""
JSONPath parser for Specification Pattern using jsonpath-rfc9535 library.

Parses RFC 9535 compliant JSONPath expressions with C-style placeholders
(%s, %d, %f, %(name)s) and converts them to Specification AST nodes.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
import re

from jsonpath_rfc9535 import JSONPathEnvironment
from jsonpath_rfc9535.selectors import NameSelector, WildcardSelector, FilterSelector
from jsonpath_rfc9535.filter_expressions import (
    ComparisonExpression,
    LogicalExpression,
    PrefixExpression,
    RelativeFilterQuery,
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
    BooleanLiteral,
    NullLiteral,
)

from ascetic_ddd.specification.domain.evaluate_visitor import EvaluateVisitor
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

# Import shared exceptions from native parser
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    JSONPathError,
    JSONPathSyntaxError,
    JSONPathTypeError,
)


# Placeholder marker constants to avoid magic numbers
# Using unlikely values within safe integer range for JSON (2^53 - 1)
PLACEHOLDER_MARKER_INT = -8765432109876
PLACEHOLDER_MARKER_FLOAT = -8765432109876.5
PLACEHOLDER_MARKER_STR = "__JSONPATH_PLACEHOLDER_a1b2c3d4__"


@dataclass
class _ConvertContext:
    """
    Mutable context for AST conversion.

    Using a context object instead of instance variables makes the converter
    thread-safe and enables concurrent conversion of different expressions.
    """
    placeholder_bind_index: int = field(default=0)
    in_item_context: bool = field(default=False)


class ParametrizedSpecificationRFC9535:
    """
    JSONPath specification parser using jsonpath-rfc9535 library (RFC 9535 compliant).

    Parses template once at initialization, binds different values at execution time.
    Thread-safe: all mutable state is passed through context objects.
    """

    def __init__(self, template: str):
        """
        Parse JSONPath template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders

        Raises:
            JSONPathSyntaxError: If template has invalid syntax
        """
        self.template = template
        self._placeholder_info: List[dict] = []
        self._env = JSONPathEnvironment()

        # Extract placeholders before parsing
        self._extract_placeholders()

        # Parse and cache query at initialization (not on every match() call)
        processed_template = self._preprocess_template()
        try:
            self._query = self._env.compile(processed_template)
        except Exception as e:
            raise JSONPathSyntaxError(
                f"Failed to parse JSONPath template: {e}",
                expression=template,
            ) from e

        # Cache whether query contains wildcard
        self._has_wildcard = self._contains_wildcard(self._query)

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

    def _preprocess_template(self) -> str:
        """
        Replace placeholders with temporary markers.

        Uses unique marker values that are unlikely to appear in real data
        to avoid collisions.

        Returns:
            Processed template string
        """
        processed = self.template

        # Replacement values based on type
        replacements = {
            "s": f'"{PLACEHOLDER_MARKER_STR}"',
            "d": str(PLACEHOLDER_MARKER_INT),
            "f": str(PLACEHOLDER_MARKER_FLOAT),
        }

        # Replace named placeholders: %(name)s, %(age)d, %(price)f
        named_pattern = r"%\((\w+)\)([sdf])"
        for match in re.finditer(named_pattern, processed):
            placeholder_str = match.group(0)
            format_type = match.group(2)
            processed = processed.replace(placeholder_str, replacements[format_type], 1)

        # Replace positional placeholders: %s, %d, %f
        positional_pattern = r"%([sdf])"
        for match in re.finditer(positional_pattern, processed):
            placeholder_str = match.group(0)
            format_type = match.group(1)
            processed = processed.replace(placeholder_str, replacements[format_type], 1)

        return processed

    def _contains_wildcard(self, query) -> bool:
        """Check if JSONPath contains wildcard [*]."""
        for segment in query.segments:
            for selector in segment.selectors:
                if isinstance(selector, WildcardSelector):
                    return True
        return False

    def _extract_filter_expression(
        self,
        query,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
    ) -> Visitable:
        """
        Extract and convert filter expression from JSONPath to Specification AST.

        Args:
            query: Parsed JSONPath query
            ctx: Conversion context (mutable state)
            params: Parameter values

        Returns:
            Specification AST node

        Raises:
            JSONPathSyntaxError: If no filter expression found
        """
        # Traverse segments to find collection name and filter
        collection_name = None

        for segment in query.segments:
            for selector in segment.selectors:
                if isinstance(selector, NameSelector):
                    # This is the collection name (e.g., "items" in $.items[*])
                    collection_name = selector.name
                elif isinstance(selector, FilterSelector):
                    # Found filter expression
                    filter_expr = selector.expression.expression
                    if self._has_wildcard and collection_name:
                        return self._create_wildcard_spec(
                            filter_expr, collection_name, ctx, params
                        )
                    else:
                        return self._convert_expression_to_spec(
                            filter_expr, ctx, params, in_item_context=False
                        )

        raise JSONPathSyntaxError(
            "No filter expression found in JSONPath",
            expression=self.template,
            context="expected [?...] filter",
        )

    def _create_wildcard_spec(
        self,
        expression,
        collection_name: str,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
    ) -> Wildcard:
        """
        Create a Wildcard specification for collection filtering.

        Args:
            expression: Filter expression
            collection_name: Name of the collection field
            ctx: Conversion context (mutable state)
            params: Parameter values

        Returns:
            Wildcard specification node
        """
        # Convert filter with Item context
        predicate = self._convert_expression_to_spec(
            expression, ctx, params, in_item_context=True
        )

        # Create Wildcard node
        parent = Object(GlobalScope(), collection_name)
        return Wildcard(parent, predicate)

    def _convert_relative_query_to_wildcard(
        self,
        rel_query: RelativeFilterQuery,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
        in_item_context: bool,
    ) -> Visitable:
        """
        Convert RelativeFilterQuery to nested Wildcard.

        Handles expressions like: @.items[*][?@.price > 100]
        which represents a nested wildcard filtering.

        Args:
            rel_query: RelativeFilterQuery from jsonpath-rfc9535
            ctx: Conversion context (mutable state)
            params: Parameter values
            in_item_context: Whether we're already in a wildcard context

        Returns:
            Wildcard node representing the nested filter, or Field for simple access

        Raises:
            JSONPathSyntaxError: If query structure is invalid
        """
        query = rel_query.query

        # Find collection name and filter expression in segments
        collection_name = None
        has_wildcard = False
        filter_expression = None

        for segment in query.segments:
            for selector in segment.selectors:
                if isinstance(selector, NameSelector):
                    collection_name = selector.name
                elif isinstance(selector, WildcardSelector):
                    has_wildcard = True
                elif isinstance(selector, FilterSelector):
                    filter_expression = selector.expression.expression

        if not collection_name:
            raise JSONPathSyntaxError(
                "No collection name found in relative query",
                expression=self.template,
                context=f"in expression: {rel_query}",
            )

        if not has_wildcard:
            # No wildcard - just a simple field access
            parent = Item() if in_item_context else GlobalScope()
            return Field(parent, collection_name)

        # We have a wildcard - create nested Wildcard
        parent_obj = Item() if in_item_context else GlobalScope()
        collection_obj = Object(parent_obj, collection_name)

        # Convert the filter expression (if any)
        if filter_expression:
            predicate = self._convert_expression_to_spec(
                filter_expression, ctx, params, in_item_context=True
            )
        else:
            raise JSONPathSyntaxError(
                "Wildcard without filter expression is not supported",
                expression=self.template,
                context="expected [*][?...] pattern",
            )

        return Wildcard(collection_obj, predicate)

    def _convert_expression_to_spec(
        self,
        expression,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
        in_item_context: bool,
    ) -> Visitable:
        """
        Convert jsonpath-rfc9535 expression to Specification AST.

        Args:
            expression: JSONPath expression node
            ctx: Conversion context (mutable state)
            params: Parameter values
            in_item_context: Whether we're in a wildcard/item context

        Returns:
            Specification AST node

        Raises:
            JSONPathSyntaxError: If expression type is not supported
        """
        ctx.in_item_context = in_item_context

        # Handle unary NOT operator (prefix expression)
        if isinstance(expression, PrefixExpression):
            if expression.operator == '!':
                operand = self._convert_expression_to_spec(
                    expression.right, ctx, params, in_item_context
                )
                return Not(operand)
            else:
                raise JSONPathSyntaxError(
                    f"Unsupported prefix operator: {expression.operator}",
                    expression=self.template,
                )

        # Handle logical operators (AND, OR)
        if isinstance(expression, LogicalExpression):
            left = self._convert_expression_to_spec(
                expression.left, ctx, params, in_item_context
            )
            right = self._convert_expression_to_spec(
                expression.right, ctx, params, in_item_context
            )

            # Use operator attribute if available, fallback to string parsing
            operator = getattr(expression, 'operator', None)
            if operator is None:
                # Fallback: parse from string representation
                expr_str = str(expression)
                if '&&' in expr_str:
                    operator = '&&'
                elif '||' in expr_str:
                    operator = '||'

            if operator == '&&' or operator == 'and':
                return And(left, right)
            elif operator == '||' or operator == 'or':
                return Or(left, right)
            else:
                raise JSONPathSyntaxError(
                    f"Unknown logical operator in expression: {expression}",
                    expression=self.template,
                    context="expected && or ||",
                )

        # Handle comparison operators
        if isinstance(expression, ComparisonExpression):
            left = self._convert_operand_to_spec(expression.left, ctx, params)
            right = self._convert_operand_to_spec(expression.right, ctx, params)

            operator_map = {
                '==': Equal,
                '!=': NotEqual,
                '>': GreaterThan,
                '<': LessThan,
                '>=': GreaterThanEqual,
                '<=': LessThanEqual,
            }

            node_class = operator_map.get(expression.operator)
            if node_class:
                return node_class(left, right)
            else:
                raise JSONPathSyntaxError(
                    f"Unsupported comparison operator: {expression.operator}",
                    expression=self.template,
                )

        # Handle nested wildcard (RelativeFilterQuery as expression)
        if isinstance(expression, RelativeFilterQuery):
            return self._convert_relative_query_to_wildcard(
                expression, ctx, params, in_item_context
            )

        raise JSONPathSyntaxError(
            f"Unsupported expression type: {type(expression).__name__}",
            expression=self.template,
        )

    def _convert_operand_to_spec(
        self,
        operand,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
    ) -> Visitable:
        """
        Convert jsonpath-rfc9535 operand to Specification AST.

        Args:
            operand: JSONPath operand (literal or query)
            ctx: Conversion context (mutable state)
            params: Parameter values

        Returns:
            Specification AST node

        Raises:
            JSONPathSyntaxError: If operand type is not supported
        """
        # Handle integer literals
        if isinstance(operand, IntegerLiteral):
            value: int | float | str = operand.value
            # Check if it's a placeholder marker
            if value == PLACEHOLDER_MARKER_INT:
                return self._try_bind_placeholder(ctx, params, ("d", "f"))
            return Value(value)

        # Handle float literals
        if isinstance(operand, FloatLiteral):
            value = operand.value
            # Check if it's a placeholder marker
            if value == PLACEHOLDER_MARKER_FLOAT:
                return self._try_bind_placeholder(ctx, params, ("f",))
            return Value(value)

        # Handle string literals
        if isinstance(operand, StringLiteral):
            value = operand.value
            # Check if it's a placeholder marker
            if value == PLACEHOLDER_MARKER_STR:
                return self._try_bind_placeholder(ctx, params, ("s",))
            return Value(value)

        # Handle boolean and null literals
        if isinstance(operand, BooleanLiteral):
            return Value(operand.value)

        if isinstance(operand, NullLiteral):
            return Value(None)

        # Handle relative filter query (@.field or @.items[*][?...])
        if isinstance(operand, RelativeFilterQuery):
            return self._convert_relative_query_operand(operand, ctx, params)

        # Handle nested expressions
        if isinstance(operand, (ComparisonExpression, LogicalExpression, PrefixExpression)):
            return self._convert_expression_to_spec(
                operand, ctx, params, ctx.in_item_context
            )

        raise JSONPathSyntaxError(
            f"Unsupported operand type: {type(operand).__name__}",
            expression=self.template,
        )

    def _try_bind_placeholder(
        self,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
        allowed_types: tuple,
    ) -> Value:
        """
        Try to bind a placeholder value from params.

        Args:
            ctx: Conversion context
            params: Parameter values
            allowed_types: Tuple of allowed format types (e.g., ("d", "f"))

        Returns:
            Value node with bound parameter
        """
        if ctx.placeholder_bind_index < len(self._placeholder_info):
            ph = self._placeholder_info[ctx.placeholder_bind_index]
            if ph["format_type"] in allowed_types:
                return self._get_placeholder_value(ctx, ph, params)
        # If not a placeholder, this shouldn't happen with marker values
        raise JSONPathSyntaxError(
            "Unexpected placeholder marker value",
            expression=self.template,
        )

    def _convert_relative_query_operand(
        self,
        operand: RelativeFilterQuery,
        ctx: _ConvertContext,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
    ) -> Visitable:
        """
        Convert RelativeFilterQuery operand to AST node.

        Handles both simple field access (@.field) and nested wildcards (@.items[*][?...]).

        Args:
            operand: RelativeFilterQuery from jsonpath-rfc9535
            ctx: Conversion context
            params: Parameter values

        Returns:
            Field or Wildcard node
        """
        query = operand.query

        # Check if this is a simple field access or a nested wildcard
        has_wildcard = False
        has_filter = False

        for segment in query.segments:
            for selector in segment.selectors:
                if isinstance(selector, WildcardSelector):
                    has_wildcard = True
                elif isinstance(selector, FilterSelector):
                    has_filter = True

        # If it has wildcard or filter, treat it as a nested wildcard
        if has_wildcard or has_filter:
            return self._convert_relative_query_to_wildcard(
                operand, ctx, params, ctx.in_item_context
            )

        # Simple field access: @.field or @.profile.age
        return self._convert_field_access(query, ctx.in_item_context)

    def _convert_field_access(self, query, in_item_context: bool) -> Field:
        """
        Convert query segments to Field node with proper nesting.

        Args:
            query: Parsed query with segments
            in_item_context: Whether we're in wildcard context

        Returns:
            Field node (possibly nested via Object chain)

        Raises:
            JSONPathSyntaxError: If field names cannot be extracted
        """
        if not query.segments:
            raise JSONPathSyntaxError(
                "Empty field access query",
                expression=self.template,
            )

        # Collect all field names from segments
        field_chain: List[str] = []
        for segment in query.segments:
            if segment.selectors:
                selector = segment.selectors[0]
                if isinstance(selector, NameSelector):
                    field_chain.append(selector.name)
                else:
                    raise JSONPathSyntaxError(
                        f"Unsupported selector in field path: {type(selector).__name__}",
                        expression=self.template,
                    )

        if not field_chain:
            raise JSONPathSyntaxError(
                "No field names found in query",
                expression=self.template,
            )

        # Build nested Field structure
        parent: EmptiableObject = Item() if in_item_context else GlobalScope()
        parent = self._build_object_chain(parent, field_chain[:-1])
        return Field(parent, field_chain[-1])

    def _build_object_chain(self, parent: EmptiableObject, names: List[str]) -> EmptiableObject:
        """
        Build a chain of Object nodes from a list of field names.

        Args:
            parent: Starting parent node
            names: List of field names

        Returns:
            Nested Object node (or parent if names is empty)
        """
        result = parent
        for name in names:
            result = Object(result, name)
        return result

    def _get_placeholder_value(
        self,
        ctx: _ConvertContext,
        ph: dict,
        params: Union[Tuple[Any, ...], Dict[str, Any]],
    ) -> Value:
        """
        Get actual value from parameters for a placeholder.

        Args:
            ctx: Conversion context (mutable state)
            ph: Placeholder info dict
            params: Parameter values

        Returns:
            Value node with bound parameter

        Raises:
            JSONPathSyntaxError: If required parameter is missing
        """
        if ph["positional"]:
            param_idx = int(ph["name"])
            if isinstance(params, (list, tuple)) and param_idx < len(params):
                actual_value = params[param_idx]
            else:
                raise JSONPathSyntaxError(
                    f"Missing positional parameter at index {param_idx}",
                    expression=self.template,
                    context=f"expected {len(self._placeholder_info)} parameters",
                )
        else:
            if isinstance(params, dict) and ph["name"] in params:
                actual_value = params[ph["name"]]
            else:
                raise JSONPathSyntaxError(
                    f"Missing named parameter: {ph['name']}",
                    expression=self.template,
                )

        ctx.placeholder_bind_index += 1
        return Value(actual_value)

    def match(
        self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]] = ()
    ) -> bool:
        """
        Check if data matches the specification with given parameters.

        This method is thread-safe: it uses a local context object for all
        mutable state during conversion.

        Args:
            data: The data object to check (must implement Context protocol)
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            True if data matches the specification, False otherwise

        Raises:
            JSONPathTypeError: If data doesn't implement Context protocol

        Examples:
            >>> spec = parse("$[?@.age > %d]")
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

        # Create local context for thread-safety
        ctx = _ConvertContext()

        # Convert cached query to Specification AST (binds params)
        spec_ast = self._extract_filter_expression(self._query, ctx, params)

        # Evaluate using EvaluateVisitor
        result = spec_ast.accept(EvaluateVisitor(data))
        if not isinstance(result, bool):
            raise TypeError(
                "Specification did not yield a boolean, got: %s"
                % type(result).__name__
            )
        return result


def parse(template: str) -> ParametrizedSpecificationRFC9535:
    """
    Parse RFC 9535 compliant JSONPath expression with C-style placeholders.

    Args:
        template: JSONPath with %s, %d, %f or %(name)s placeholders

    Returns:
        ParametrizedSpecificationRFC9535 that can be executed with different parameter values

    Examples:
        >>> spec = parse("$[?@.age > %d]")
        >>> user = DictContext({"age": 30})
        >>> spec.match(user, (25,))
        True

        >>> spec = parse("$[?@.name == %(name)s]")
        >>> user = DictContext({"name": "Alice"})
        >>> spec.match(user, {"name": "Alice"})
        True
    """
    return ParametrizedSpecificationRFC9535(template)
