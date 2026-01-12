"""
JSONPath parser for Specification Pattern using jsonpath2 library.

Parses JSONPath expressions with C-style placeholders (%s, %d, %f, %(name)s)
and converts them to Specification AST nodes using jsonpath2 library.
"""
from typing import Any, Dict, Tuple, Union
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
from jsonpath2.nodes.root import RootNode

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


class PlaceholderReference:
    """
    Reference to a placeholder location in the JSONPath AST.

    Stores where a placeholder is located so we can update it when binding parameters.
    """

    def __init__(self, name: str, format_type: str):
        """
        Initialize placeholder reference.

        Args:
            name: Placeholder name (for named) or index (for positional)
            format_type: Type hint ('s', 'd', 'f')
        """
        self.name = name
        self.format_type = format_type
        self.value = None

    def bind(self, value: Any):
        """Bind a value to this placeholder."""
        self.value = value


class ParametrizedSpecificationJsonPath2:
    """
    JSONPath specification parser using jsonpath2 library.

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
        self._in_item_context = False

        # Extract placeholders before parsing
        self._extract_placeholders()

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
        # Simple approach: replace == with = outside of string literals
        # We need to be careful not to replace == inside strings like "value=="

        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(template):
            char = template[i]

            # Track if we're inside a string literal
            if char in ('"', "'") and (i == 0 or template[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            # Replace == with = only outside strings
            if not in_string and char == '=' and i + 1 < len(template) and template[i + 1] == '=':
                result.append('=')
                i += 2  # Skip both = characters
                continue

            result.append(char)
            i += 1

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
        in_string = False
        string_char = None

        while i < len(template):
            char = template[i]

            # Track if we're inside a string literal
            if char in ('"', "'") and (i == 0 or template[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

            if not in_string:
                # Replace && with and
                if char == '&' and i + 1 < len(template) and template[i + 1] == '&':
                    result.append(' and ')
                    i += 2
                    continue

                # Replace || with or
                elif char == '|' and i + 1 < len(template) and template[i + 1] == '|':
                    result.append(' or ')
                    i += 2
                    continue

                # Replace ! with not (but not in !=)
                elif char == '!' and i + 1 < len(template) and template[i + 1] != '=':
                    result.append('not ')
                    i += 1
                    continue

            result.append(char)
            i += 1

        return ''.join(result)

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

    def _add_parentheses_to_filter(self, template: str) -> str:
        """
        Add parentheses around filter expressions if not present.

        jsonpath2 library requires parentheses: $[?(@.age > 25)] not $[?@.age > 25]

        Args:
            template: JSONPath template string

        Returns:
            Template with parentheses added
        """
        import re

        # Simple regex replacement:
        # [?@...] -> [?(@...)]
        # But don't replace if already has parentheses: [?(@...)]

        # Pattern: [? followed by @ (not followed by ()
        # Replace: [?@ with [?(@
        # Then find corresponding ] and replace with )]

        result = template

        # Step 1: Find all [?@ patterns that don't have ( after ?
        pattern = r'\[\?(?!\()'  # [? not followed by (
        positions = []
        for match in re.finditer(pattern, result):
            # Check if next char is @ or space then @
            pos = match.end()
            if pos < len(result) and (result[pos] == '@' or (result[pos] == ' ' and pos + 1 < len(result) and result[pos + 1] == '@')):
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
        # RFC 9535 standard defines ==, but jsonpath2 library uses single =
        # We replace == with = to provide better UX and compatibility with the library
        processed = self._normalize_equality_operator(processed)

        # Normalize logical operators for jsonpath2 library compatibility
        # RFC 9535 standard defines &&, ||, !, but jsonpath2 library uses and, or, not
        # We replace symbol operators with text operators
        processed = self._normalize_logical_operators(processed)

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

    def _contains_wildcard(self, path: Path) -> bool:
        """Check if JSONPath contains wildcard [*]."""
        current_node = path.root_node
        while current_node:
            if isinstance(current_node, SubscriptNode):
                # Check if any subscript is a wildcard
                for subscript in current_node.subscripts:
                    if isinstance(subscript, WildcardSubscript):
                        return True
            current_node = getattr(current_node, "next_node", None)
        return False

    def _extract_filter_expression(
        self, path: Path, params: Union[Tuple[Any, ...], Dict[str, Any]]
    ) -> Visitable:
        """
        Extract and convert filter expression from JSONPath to Specification AST.

        Args:
            path: Parsed JSONPath
            params: Parameter values

        Returns:
            Specification AST node
        """
        # Reset placeholder binding index
        self._placeholder_bind_index = 0
        self._placeholder_refs = []

        # Check for wildcard
        has_wildcard = self._contains_wildcard(path)

        # Traverse nodes to find collection name and filter
        current_node = path.root_node.next_node  # Skip RootNode
        collection_name = None

        while current_node:
            if isinstance(current_node, SubscriptNode):
                # Process subscripts to find collection name, wildcard, and filter
                for subscript in current_node.subscripts:
                    if isinstance(subscript, ObjectIndexSubscript):
                        # This is the collection name (e.g., "items" in $.items[*])
                        collection_name = subscript.index
                    elif isinstance(subscript, FilterSubscript):
                        # Found filter expression
                        if has_wildcard and collection_name:
                            return self._create_wildcard_spec(
                                subscript.expression, collection_name, params
                            )
                        else:
                            return self._convert_expression_to_spec(
                                subscript.expression, params, False
                            )

            current_node = getattr(current_node, "next_node", None)

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

    def _convert_expression_to_spec(
        self, expression, params, in_item_context: bool
    ) -> Visitable:
        """
        Convert jsonpath2 expression to Specification AST.

        Args:
            expression: JSONPath expression node
            params: Parameter values
            in_item_context: Whether we're in a wildcard/item context

        Returns:
            Specification AST node
        """
        self._in_item_context = in_item_context

        # Handle unary NOT operator
        if isinstance(expression, NotUnaryOperatorExpression):
            # Get the operand expression
            operand = self._convert_expression_to_spec(
                expression.expression, params, in_item_context
            )
            return Not(operand)

        # Handle variadic operators (AND, OR)
        if isinstance(expression, (AndVariadicOperatorExpression, OrVariadicOperatorExpression)):
            # Get all operands
            operands = []
            for operand in expression.expressions:
                operands.append(self._convert_expression_to_spec(operand, params, in_item_context))

            # Combine with AND or OR
            if isinstance(expression, AndVariadicOperatorExpression):
                result = operands[0]
                for operand in operands[1:]:
                    result = And(result, operand)
                return result
            else:  # OR
                result = operands[0]
                for operand in operands[1:]:
                    result = Or(result, operand)
                return result

        # Handle SomeExpression (nested wildcards)
        if isinstance(expression, SomeExpression):
            return self._convert_some_expression(expression, params, in_item_context)

        # Handle binary operators
        if isinstance(expression, BinaryOperatorExpression):
            # Get left and right operands
            left = self._convert_node_or_value(expression.left_node_or_value, params)
            right = self._convert_node_or_value(expression.right_node_or_value, params)

            # Map expression type to Specification node
            if isinstance(expression, EqualBinaryOperatorExpression):
                return Equal(left, right)
            elif isinstance(expression, NotEqualBinaryOperatorExpression):
                return NotEqual(left, right)
            elif isinstance(expression, GreaterThanBinaryOperatorExpression):
                return GreaterThan(left, right)
            elif isinstance(expression, LessThanBinaryOperatorExpression):
                return LessThan(left, right)
            elif isinstance(expression, GreaterThanOrEqualToBinaryOperatorExpression):
                return GreaterThanEqual(left, right)
            elif isinstance(expression, LessThanOrEqualToBinaryOperatorExpression):
                return LessThanEqual(left, right)
            else:
                raise ValueError(f"Unsupported binary operator: {type(expression)}")

        raise ValueError(f"Unsupported expression type: {type(expression)}")

    def _convert_some_expression(
        self, expression: SomeExpression, params, in_item_context: bool
    ) -> Wildcard:
        """
        Convert SomeExpression (nested wildcard) to Wildcard Specification node.

        SomeExpression represents nested wildcard patterns like:
        @.items[*][?@.price > 500]

        Structure:
          SomeExpression
            next_node_or_value: CurrentNode (@)
              next_node: SubscriptNode (field: "items")
                next_node: SubscriptNode (wildcard: [*])
                  next_node: SubscriptNode (filter: [?...])
                    expression: predicate

        Args:
            expression: SomeExpression from jsonpath2
            params: Parameter values
            in_item_context: Whether we're in a wildcard/item context

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
            raise ValueError(f"Expected ObjectIndexSubscript for collection field")

        collection_name = current.subscripts[0].index

        # Move to next node (should be SubscriptNode with WildcardSubscript)
        current = current.next_node

        if not isinstance(current, SubscriptNode):
            raise ValueError(f"Expected SubscriptNode with wildcard, got {type(current)}")

        if not current.subscripts or not isinstance(current.subscripts[0], WildcardSubscript):
            raise ValueError(f"Expected WildcardSubscript, got {type(current.subscripts[0]) if current.subscripts else 'no subscripts'}")

        # Move to next node (should be SubscriptNode with FilterSubscript)
        current = current.next_node

        if not isinstance(current, SubscriptNode):
            raise ValueError(f"Expected SubscriptNode with filter, got {type(current)}")

        if not current.subscripts or not isinstance(current.subscripts[0], FilterSubscript):
            raise ValueError(f"Expected FilterSubscript, got {type(current.subscripts[0]) if current.subscripts else 'no subscripts'}")

        # Extract filter expression (predicate)
        filter_expression = current.subscripts[0].expression

        # Convert filter expression to predicate
        # We're now in item context because we're inside a wildcard
        predicate = self._convert_expression_to_spec(filter_expression, params, True)

        # Build parent: Item() or GlobalScope() + Object(collection_name)
        parent = Item() if in_item_context else GlobalScope()
        parent = Object(parent, collection_name)

        # Create Wildcard node
        return Wildcard(parent, predicate)

    def _convert_node_or_value(self, node_or_value, params) -> Visitable:
        """
        Convert jsonpath2 node or value to Specification AST.

        Args:
            node_or_value: JSONPath node or literal value
            params: Parameter values

        Returns:
            Specification AST node
        """
        # Check if it's a literal value
        if isinstance(node_or_value, (int, float, str, bool, type(None))):
            # Check if it's a placeholder marker
            if self._placeholder_bind_index < len(self._placeholder_info):
                ph = self._placeholder_info[self._placeholder_bind_index]

                # Check if this is a placeholder marker
                is_placeholder = False
                if ph["format_type"] == "s" and node_or_value == "__PLACEHOLDER__":
                    is_placeholder = True
                elif ph["format_type"] in ("d", "f") and node_or_value == 999999:
                    is_placeholder = True

                if is_placeholder:
                    # Get actual value from params
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

            return Value(node_or_value)

        # Check if it's a CurrentNode (@)
        if isinstance(node_or_value, CurrentNode):
            # CurrentNode has a chain of SubscriptNodes for nested paths
            # Example: @.profile.age becomes:
            #   CurrentNode -> SubscriptNode["profile"] -> SubscriptNode["age"] -> TerminalNode

            field_chain = []
            current = node_or_value.next_node

            # Walk through the chain and collect all field names
            while current and not current.__class__.__name__ == 'TerminalNode':
                if isinstance(current, SubscriptNode):
                    # Get the first subscript (should be ObjectIndexSubscript)
                    if current.subscripts and isinstance(current.subscripts[0], ObjectIndexSubscript):
                        field_chain.append(current.subscripts[0].index)
                        current = getattr(current, "next_node", None)
                    else:
                        break
                else:
                    break

            if field_chain:
                # Build nested Field structure for nested paths
                # e.g., ["profile", "age"] -> Field(Object(parent, "profile"), "age")
                parent = Item() if self._in_item_context else GlobalScope()

                # Build Object chain for all fields except the last
                for field in field_chain[:-1]:
                    parent = Object(parent, field)

                # Last field
                field_name = field_chain[-1]
                return Field(parent, field_name)

        # Check for nested expression
        if isinstance(node_or_value, (BinaryOperatorExpression, AndVariadicOperatorExpression, OrVariadicOperatorExpression, NotUnaryOperatorExpression)):
            return self._convert_expression_to_spec(node_or_value, params, self._in_item_context)

        raise ValueError(f"Unsupported node type: {type(node_or_value)}")

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

        # Preprocess template
        processed_template = self._preprocess_template()

        # Parse with jsonpath2
        path = Path.parse_str(processed_template)

        # Extract filter expression and convert to Specification AST
        spec_ast = self._extract_filter_expression(path, params)

        # Evaluate using EvaluateVisitor
        visitor = EvaluateVisitor(data)
        spec_ast.accept(visitor)

        return visitor.result()


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
