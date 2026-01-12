"""
Extension for jsonpath2 parser to support parameterized expressions.

Extends the parser to recognize placeholders (%s, %d, %f, %(name)s)
and bind values at execution time.
"""
from typing import Any, Dict, Tuple, Union, Iterator
import re
from jsonpath2.path import Path
from jsonpath2.node import MatchData
from jsonpath2.expression import Expression


class PlaceholderReference:
    """
    Reference to a placeholder location in the AST.

    Stores where a placeholder is located so we can update it when binding parameters.
    """

    def __init__(self, expression, attribute: str, name: str, format_type: str):
        """
        Initialize placeholder reference.

        Args:
            expression: The BinaryOperatorExpression that contains the placeholder
            attribute: The attribute name ('left_node_or_value' or 'right_node_or_value')
            name: Placeholder name (for named) or index (for positional)
            format_type: Type hint ('s', 'd', 'f')
        """
        self.expression = expression
        self.attribute = attribute
        self.name = name
        self.format_type = format_type

    def bind(self, value: Any):
        """Bind a value to this placeholder by updating the AST."""
        setattr(self.expression, self.attribute, value)


class ParametrizedPath:
    """
    JSONPath with placeholder support.

    Parses template once, binds different values at execution time.
    """

    def __init__(self, template: str):
        """
        Parse template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders
        """
        self.template = template
        self._placeholder_info = []  # Info from preprocessing
        self._placeholder_refs = []  # References to AST locations
        self._placeholder_index = 0  # Track which placeholder to use next

        # Parse template and replace placeholders with markers
        processed_template, self._placeholder_info = self._preprocess_template(template)

        # Parse with jsonpath2
        self._path = Path.parse_str(processed_template)

        # Post-process AST to create placeholder references
        self._inject_placeholders()

    @property
    def placeholders(self):
        """Return placeholder info for compatibility."""
        return self._placeholder_info

    @staticmethod
    def _normalize_equality_operator(template: str) -> str:
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

    @staticmethod
    def _normalize_logical_operators(template: str) -> str:
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
                    # Special case: ?!(...) should become ?(not ...) not ?not (...)
                    # Check if previous non-space char is ?
                    if result and ''.join(result).rstrip().endswith('?'):
                        # Convert ?!(...) to ?(not ...)
                        # Skip the ! and the opening paren, add (not
                        if i + 1 < len(template) and template[i + 1] == '(':
                            result.append('(not ')
                            i += 2  # Skip ! and (
                            continue
                    result.append('not ')
                    i += 1
                    continue

            result.append(char)
            i += 1

        return ''.join(result)

    @staticmethod
    def _add_parentheses_to_filter(template: str) -> str:
        """
        Add parentheses around filter expressions if not present.

        jsonpath2 library requires parentheses: $[?(@.age > 25)] not $[?@.age > 25]
        RFC 9535 allows both syntaxes, so we normalize to jsonpath2 format.

        Args:
            template: JSONPath template string

        Returns:
            Template with parentheses added
        """
        result = template

        # Find all [?@ patterns that don't have ( after ?
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

    def _preprocess_template(self, template: str) -> Tuple[str, list]:
        """
        Replace placeholders with temporary markers and normalize operators.

        Returns:
            (processed_template, list of placeholder info)
        """
        placeholders = []

        # Add parentheses to filter expressions (required by jsonpath2)
        processed = self._add_parentheses_to_filter(template)

        # Normalize == to = for jsonpath2 library compatibility
        processed = self._normalize_equality_operator(processed)

        # Normalize logical operators for jsonpath2 library compatibility
        processed = self._normalize_logical_operators(processed)

        # Find named placeholders: %(name)s, %(age)d, %(price)f
        named_pattern = r'%\((\w+)\)([sdf])'
        for match in re.finditer(named_pattern, processed):
            name = match.group(1)
            format_type = match.group(2)
            placeholder_str = match.group(0)

            # Replace with a valid literal based on type
            if format_type == 's':
                replacement = '"__PLACEHOLDER__"'
            elif format_type == 'd':
                replacement = '999999'
            elif format_type == 'f':
                replacement = '999999.0'

            placeholders.append({
                'original': placeholder_str,
                'name': name,
                'format_type': format_type,
                'replacement': replacement,
                'positional': False
            })

            processed = processed.replace(placeholder_str, replacement, 1)

        # Find positional placeholders: %s, %d, %f
        positional_pattern = r'%([sdf])'
        position = 0
        for match in re.finditer(positional_pattern, processed):
            format_type = match.group(1)
            placeholder_str = match.group(0)

            # Replace with a valid literal
            if format_type == 's':
                replacement = '"__PLACEHOLDER__"'
            elif format_type == 'd':
                replacement = '999999'
            elif format_type == 'f':
                replacement = '999999.0'

            placeholders.append({
                'original': placeholder_str,
                'name': str(position),
                'format_type': format_type,
                'replacement': replacement,
                'positional': True
            })

            processed = processed.replace(placeholder_str, replacement, 1)
            position += 1

        return processed, placeholders

    def _inject_placeholders(self):
        """
        Walk the parsed AST and replace marker literals with PlaceholderExpression.

        This modifies the AST in-place.
        """
        # Traverse the node chain starting from root
        current_node = self._path.root_node
        while current_node:
            # Check if it's a SubscriptNode with subscripts
            if hasattr(current_node, 'subscripts'):
                for subscript in current_node.subscripts:
                    self._process_subscript(subscript)

            # Move to next node in chain
            current_node = getattr(current_node, 'next_node', None)

    def _process_subscript(self, subscript):
        """Process a subscript node, recursively replacing placeholders."""
        # Check if it's a FilterSubscript
        if hasattr(subscript, 'expression'):
            self._process_expression(subscript.expression)

    def _process_expression(self, expression):
        """Recursively process expression nodes."""
        # Handle VariadicOperatorExpression (AND/OR) which has 'expressions' list
        if hasattr(expression, 'expressions'):
            for sub_expr in expression.expressions:
                self._process_expression(sub_expr)
            return

        # Handle UnaryOperatorExpression (NOT) which has 'expression' in jsonpath2
        if hasattr(expression, 'expression') and not hasattr(expression, 'expressions'):
            inner = expression.expression
            if hasattr(inner, 'evaluate'):
                self._process_expression(inner)
            return

        # Check for BinaryOperatorExpression with left_node_or_value/right_node_or_value
        if hasattr(expression, 'left_node_or_value') and hasattr(expression, 'right_node_or_value'):
            # Check left and right for placeholder markers
            self._check_and_create_placeholder_ref(
                expression, 'left_node_or_value', expression.left_node_or_value
            )
            self._check_and_create_placeholder_ref(
                expression, 'right_node_or_value', expression.right_node_or_value
            )

            # Recursively process sub-expressions (if they have evaluate methods)
            if hasattr(expression.left_node_or_value, 'evaluate'):
                self._process_expression(expression.left_node_or_value)
            if hasattr(expression.right_node_or_value, 'evaluate'):
                self._process_expression(expression.right_node_or_value)

    def _check_and_create_placeholder_ref(self, expression, attribute: str, node_or_value):
        """Check if node_or_value is a placeholder marker and create reference."""
        # Check if this is one of our placeholder markers
        if self._placeholder_index < len(self._placeholder_info):
            ph = self._placeholder_info[self._placeholder_index]

            # Check if the value matches the expected marker
            is_match = False
            if ph['format_type'] == 's' and node_or_value == "__PLACEHOLDER__":
                is_match = True
            elif ph['format_type'] in ('d', 'f') and node_or_value == 999999:
                is_match = True

            if is_match:
                # Create a reference to this placeholder location
                placeholder_ref = PlaceholderReference(
                    expression, attribute, ph['name'], ph['format_type']
                )
                self._placeholder_refs.append(placeholder_ref)
                self._placeholder_index += 1

    def match(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> Iterator[MatchData]:
        """
        Match data with bound parameters.

        Args:
            data: Data to match
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            Iterator of MatchData
        """
        # Bind parameter values to PlaceholderExpression nodes
        self._bind_placeholders(params)

        # Execute the path with bound parameters
        return self._path.match(data)

    def _bind_placeholders(self, params: Union[Tuple[Any, ...], Dict[str, Any]]):
        """Bind parameter values to all placeholders in AST."""
        for placeholder_ref in self._placeholder_refs:
            # Get the value for this placeholder
            if placeholder_ref.name.isdigit():
                # Positional
                idx = int(placeholder_ref.name)
                if idx >= len(params):
                    raise ValueError(f"Missing positional parameter at index {idx}")
                value = params[idx]
            else:
                # Named
                if placeholder_ref.name not in params:
                    raise ValueError(f"Missing named parameter: {placeholder_ref.name}")
                value = params[placeholder_ref.name]

            # Bind the value by updating the AST
            placeholder_ref.bind(value)

    def find(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> list:
        """Find all matching values."""
        return [match.current_value for match in self.match(data, params)]

    def find_one(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> Any:
        """Find first matching value."""
        for match in self.match(data, params):
            return match.current_value
        return None


def parse(template: str) -> ParametrizedPath:
    """
    Parse JSONPath expression with C-style placeholders.

    Args:
        template: JSONPath with %s, %d, %f or %(name)s placeholders

    Returns:
        ParametrizedPath that can be executed with different parameter values

    Examples:
        >>> path = parse("$[?(@.age > %d)]")
        >>> results = path.match(data, (27,))
        >>>
        >>> path = parse("$[?(@.name = %(name)s)]")
        >>> results = path.match(data, {"name": "Alice"})
    """
    return ParametrizedPath(template)
