"""
JSONPath parser for Specification Pattern using jsonpath-rfc9535 library.

Parses RFC 9535 compliant JSONPath expressions with C-style placeholders
(%s, %d, %f, %(name)s) and converts them to Specification AST nodes.
"""
from typing import Any, Dict, Tuple, Union
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
from ..evaluate_visitor import EvaluateVisitor


class ParametrizedSpecificationRFC9535:
    """
    JSONPath specification parser using jsonpath-rfc9535 library (RFC 9535 compliant).

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
        self._placeholder_bind_index = 0
        self._in_item_context = False
        self.env = JSONPathEnvironment()

        # Extract placeholders before parsing
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

    def _preprocess_template(self) -> str:
        """
        Replace placeholders with temporary markers.

        Returns:
            Processed template string
        """
        processed = self.template

        # Replace named placeholders
        named_pattern = r"%\((\w+)\)([sdf])"
        for match in re.finditer(named_pattern, processed):
            placeholder_str = match.group(0)
            format_type = match.group(2)

            # Replace with valid literal based on type
            if format_type == "s":
                replacement = '"__PLACEHOLDER__"'
            elif format_type == "d":
                replacement = "999999"
            elif format_type == "f":
                replacement = "999999.0"

            processed = processed.replace(placeholder_str, replacement, 1)

        # Replace positional placeholders
        positional_pattern = r"%([sdf])"
        for match in re.finditer(positional_pattern, processed):
            placeholder_str = match.group(0)
            format_type = match.group(1)

            # Replace with valid literal
            if format_type == "s":
                replacement = '"__PLACEHOLDER__"'
            elif format_type == "d":
                replacement = "999999"
            elif format_type == "f":
                replacement = "999999.0"

            processed = processed.replace(placeholder_str, replacement, 1)

        return processed

    def _contains_wildcard(self, query) -> bool:
        """Check if JSONPath contains wildcard [*]."""
        for segment in query.segments:
            for selector in segment.selectors:
                if isinstance(selector, WildcardSelector):
                    return True
        return False

    def _extract_filter_expression(
        self, query, params: Union[Tuple[Any, ...], Dict[str, Any]]
    ) -> Visitable:
        """
        Extract and convert filter expression from JSONPath to Specification AST.

        Args:
            query: Parsed JSONPath query
            params: Parameter values

        Returns:
            Specification AST node
        """
        # Reset placeholder binding index
        self._placeholder_bind_index = 0

        # Check for wildcard
        has_wildcard = self._contains_wildcard(query)

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
                    if has_wildcard and collection_name:
                        return self._create_wildcard_spec(
                            filter_expr, collection_name, params
                        )
                    else:
                        return self._convert_expression_to_spec(
                            filter_expr, params, False
                        )

        raise ValueError("No filter expression found in JSONPath")

    def _create_wildcard_spec(
        self, expression, collection_name: str, params
    ) -> Wildcard:
        """
        Create a Wildcard specification for collection filtering.

        Args:
            expression: Filter expression
            collection_name: Name of the collection field
            params: Parameter values

        Returns:
            Wildcard specification node
        """
        # Convert filter with Item context
        predicate = self._convert_expression_to_spec(expression, params, True)

        # Create Wildcard node
        parent = Object(GlobalScope(), collection_name)
        return Wildcard(parent, predicate)

    def _convert_relative_query_to_wildcard(
        self, rel_query: RelativeFilterQuery, params, in_item_context: bool
    ) -> Wildcard:
        """
        Convert RelativeFilterQuery to nested Wildcard.

        Handles expressions like: @.items[*][?@.price > 100]
        which represents a nested wildcard filtering.

        Args:
            rel_query: RelativeFilterQuery from jsonpath-rfc9535
            params: Parameter values
            in_item_context: Whether we're already in a wildcard context

        Returns:
            Wildcard node representing the nested filter
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
            raise ValueError(f"No collection name found in RelativeFilterQuery: {rel_query}")

        if not has_wildcard:
            # No wildcard - just a simple field access
            # This shouldn't happen in a filter expression context, but handle it
            parent = Item() if in_item_context else GlobalScope()
            return Field(parent, collection_name)

        # We have a wildcard - create nested Wildcard
        # Parent should be Item() if we're in nested context
        parent_obj = Item() if in_item_context else GlobalScope()
        collection_obj = Object(parent_obj, collection_name)

        # Convert the filter expression (if any)
        if filter_expression:
            # Set item context to True for nested wildcard predicate
            predicate = self._convert_expression_to_spec(filter_expression, params, True)
        else:
            # No filter - matches all items (this is unusual but possible)
            # We could use AlwaysTrue specification if we had one
            raise ValueError("Wildcard without filter expression is not supported")

        return Wildcard(collection_obj, predicate)

    def _convert_expression_to_spec(
        self, expression, params, in_item_context: bool
    ) -> Visitable:
        """
        Convert jsonpath-rfc9535 expression to Specification AST.

        Args:
            expression: JSONPath expression node
            params: Parameter values
            in_item_context: Whether we're in a wildcard/item context

        Returns:
            Specification AST node
        """
        self._in_item_context = in_item_context

        # Handle unary NOT operator (prefix expression)
        if isinstance(expression, PrefixExpression):
            if expression.operator == '!':
                # Get the operand expression
                operand = self._convert_expression_to_spec(
                    expression.right, params, in_item_context
                )
                return Not(operand)
            else:
                raise ValueError(f"Unsupported prefix operator: {expression.operator}")

        # Handle logical operators (AND, OR)
        if isinstance(expression, LogicalExpression):
            # Get left and right operands
            left = self._convert_expression_to_spec(
                expression.left, params, in_item_context
            )
            right = self._convert_expression_to_spec(
                expression.right, params, in_item_context
            )

            # Determine operator type
            expr_str = str(expression)
            if '&&' in expr_str or ' and ' in expr_str.lower():
                return And(left, right)
            elif '||' in expr_str or ' or ' in expr_str.lower():
                return Or(left, right)
            else:
                # Fallback: assume AND
                return And(left, right)

        # Handle comparison operators
        if isinstance(expression, ComparisonExpression):
            # Get left and right operands
            left = self._convert_operand_to_spec(expression.left, params)
            right = self._convert_operand_to_spec(expression.right, params)

            # Map operator to Specification node
            if expression.operator == '==':
                return Equal(left, right)
            elif expression.operator == '!=':
                return NotEqual(left, right)
            elif expression.operator == '>':
                return GreaterThan(left, right)
            elif expression.operator == '<':
                return LessThan(left, right)
            elif expression.operator == '>=':
                return GreaterThanEqual(left, right)
            elif expression.operator == '<=':
                return LessThanEqual(left, right)
            else:
                raise ValueError(f"Unsupported comparison operator: {expression.operator}")

        # Handle nested wildcard (RelativeFilterQuery as expression)
        if isinstance(expression, RelativeFilterQuery):
            return self._convert_relative_query_to_wildcard(expression, params, in_item_context)

        raise ValueError(f"Unsupported expression type: {type(expression)}")

    def _convert_operand_to_spec(self, operand, params) -> Visitable:
        """
        Convert jsonpath-rfc9535 operand to Specification AST.

        Args:
            operand: JSONPath operand (literal or query)
            params: Parameter values

        Returns:
            Specification AST node
        """
        # Handle literals
        if isinstance(operand, IntegerLiteral):
            value = operand.value
            # Check if it's a placeholder marker
            if value == 999999 and self._placeholder_bind_index < len(self._placeholder_info):
                ph = self._placeholder_info[self._placeholder_bind_index]
                if ph["format_type"] in ("d", "f"):
                    return self._get_placeholder_value(ph, params)
            return Value(value)

        elif isinstance(operand, FloatLiteral):
            value = operand.value
            # Check if it's a placeholder marker
            if value == 999999.0 and self._placeholder_bind_index < len(self._placeholder_info):
                ph = self._placeholder_info[self._placeholder_bind_index]
                if ph["format_type"] == "f":
                    return self._get_placeholder_value(ph, params)
            return Value(value)

        elif isinstance(operand, StringLiteral):
            value = operand.value
            # Check if it's a placeholder marker
            if value == "__PLACEHOLDER__" and self._placeholder_bind_index < len(self._placeholder_info):
                ph = self._placeholder_info[self._placeholder_bind_index]
                if ph["format_type"] == "s":
                    return self._get_placeholder_value(ph, params)
            return Value(value)

        elif isinstance(operand, BooleanLiteral):
            return Value(operand.value)

        elif isinstance(operand, NullLiteral):
            return Value(None)

        # Handle relative filter query (@.field or @.items[*][?...])
        elif isinstance(operand, RelativeFilterQuery):
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
                return self._convert_relative_query_to_wildcard(operand, params, self._in_item_context)

            # Field access (simple or nested): @.field or @.profile.age
            if query.segments and len(query.segments) > 0:
                # Collect all field names from segments
                field_chain = []
                for segment in query.segments:
                    if segment.selectors and len(segment.selectors) > 0:
                        selector = segment.selectors[0]
                        if isinstance(selector, NameSelector):
                            field_chain.append(selector.name)
                        else:
                            raise ValueError(f"Unsupported selector in nested path: {type(selector).__name__}")

                if not field_chain:
                    raise ValueError(f"No field names found in filter query: {operand}")

                # Build nested Field structure for nested paths
                # e.g., ["profile", "age"] -> Field(Object(parent, "profile"), "age")
                parent = Item() if self._in_item_context else GlobalScope()

                # Build Object chain for all fields except the last
                for field in field_chain[:-1]:
                    parent = Object(parent, field)

                # Last field
                field_name = field_chain[-1]
                return Field(parent, field_name)

            raise ValueError(f"Unsupported filter query: {operand}")

        # Handle nested expressions
        if isinstance(operand, (ComparisonExpression, LogicalExpression, PrefixExpression)):
            return self._convert_expression_to_spec(operand, params, self._in_item_context)

        raise ValueError(f"Unsupported operand type: {type(operand)}")

    def _get_placeholder_value(self, ph, params) -> Value:
        """Get actual value from parameters for a placeholder."""
        if ph["positional"]:
            param_idx = int(ph["name"])
            if param_idx < len(params):
                actual_value = params[param_idx]
            else:
                raise ValueError(
                    f"Missing positional parameter at index {param_idx}"
                )
        else:
            if ph["name"] in params:
                actual_value = params[ph["name"]]
            else:
                raise ValueError(f"Missing named parameter: {ph['name']}")

        self._placeholder_bind_index += 1
        return Value(actual_value)

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
            >>> spec = parse("$[?@.age > %d]")
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

        # Preprocess template
        processed_template = self._preprocess_template()

        # Parse with jsonpath-rfc9535
        query = self.env.compile(processed_template)

        # Extract filter expression and convert to Specification AST
        spec_ast = self._extract_filter_expression(query, params)

        # Evaluate using EvaluateVisitor
        visitor = EvaluateVisitor(data)
        spec_ast.accept(visitor)

        return visitor.result()


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
