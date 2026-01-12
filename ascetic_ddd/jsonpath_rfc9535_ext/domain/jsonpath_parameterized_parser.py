"""
Extension for jsonpath-rfc9535 parser to support parameterized expressions.

Extends the parser to recognize placeholders (%s, %d, %f, %(name)s)
and bind values at execution time.
"""
from typing import Any, Dict, Tuple, Union
import re
from jsonpath_rfc9535 import JSONPathEnvironment


class ParametrizedExpression:
    """
    JSONPath expression with placeholder support (RFC 9535 compliant).

    Parses template once, binds different values at execution time.
    """

    def __init__(self, template: str, env: JSONPathEnvironment = None):
        """
        Parse template with placeholders.

        Args:
            template: JSONPath with %s, %d, %f or %(name)s placeholders
            env: JSONPath environment (optional, creates default if not provided)
        """
        self.template = template
        self.env = env or JSONPathEnvironment()
        self._placeholder_info = []  # Info from preprocessing

        # Parse template and replace placeholders with markers
        self._processed_template, self._placeholder_info = self._preprocess_template(template)

    @property
    def placeholders(self):
        """Return placeholder info for compatibility."""
        return self._placeholder_info

    def _preprocess_template(self, template: str) -> Tuple[str, list]:
        """
        Replace placeholders with temporary markers.

        Returns:
            (processed_template, list of placeholder info)
        """
        placeholders = []
        processed = template

        # Find named placeholders: %(name)s, %(age)d, %(price)f
        named_pattern = r'%\((\w+)\)([sdf])'
        for match in re.finditer(named_pattern, template):
            name = match.group(1)
            format_type = match.group(2)
            placeholder_str = match.group(0)

            # Replace with a valid JSONPath literal based on type
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

            # Replace with a valid JSONPath literal
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

    def _build_bound_expression(self, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> str:
        """Build JSONPath expression with bound parameter values."""
        result = self._processed_template

        # Process placeholders in order
        for i, ph_info in enumerate(self._placeholder_info):
            # Get the value for this placeholder
            if ph_info['positional']:
                # Positional
                idx = int(ph_info['name'])
                if idx >= len(params):
                    raise ValueError(f"Missing positional parameter at index {idx}")
                value = params[idx]
            else:
                # Named
                if ph_info['name'] not in params:
                    raise ValueError(f"Missing named parameter: {ph_info['name']}")
                value = params[ph_info['name']]

            # Replace the placeholder marker with the actual value
            if ph_info['format_type'] == 's':
                # String - need to escape and quote
                if isinstance(value, bool):
                    # Boolean - use lowercase true/false
                    replacement = 'true' if value else 'false'
                else:
                    # String - escape quotes
                    escaped_value = str(value).replace('"', '\\"')
                    replacement = f'"{escaped_value}"'
                marker = '"__PLACEHOLDER__"'
            elif ph_info['format_type'] == 'd':
                # Integer
                replacement = str(int(value))
                marker = '999999'
            elif ph_info['format_type'] == 'f':
                # Float
                replacement = str(float(value))
                marker = '999999.0'
            else:
                continue

            # Replace first occurrence
            result = result.replace(marker, replacement, 1)

        return result

    def find(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> list:
        """
        Find all matching values.

        Args:
            data: Data to search
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            List of matching values
        """
        # Build expression with bound values
        bound_expression = self._build_bound_expression(params)

        # Compile and execute
        query = self.env.compile(bound_expression)
        result = query.find(data)

        # Return values
        return result.values()

    def find_one(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]) -> Any:
        """
        Find first matching value.

        Args:
            data: Data to search
            params: Parameter values (tuple for positional, dict for named)

        Returns:
            First matching value or None
        """
        # Build expression with bound values
        bound_expression = self._build_bound_expression(params)

        # Compile and execute
        query = self.env.compile(bound_expression)
        node = query.find_one(data)

        # Return value or None
        return node.value if node else None

    def finditer(self, data: Any, params: Union[Tuple[Any, ...], Dict[str, Any]]):
        """
        Iterate over matching values.

        Args:
            data: Data to search
            params: Parameter values (tuple for positional, dict for named)

        Yields:
            Matching values
        """
        # Build expression with bound values
        bound_expression = self._build_bound_expression(params)

        # Compile and execute
        query = self.env.compile(bound_expression)

        # Iterate and yield values
        for node in query.finditer(data):
            yield node.value


def parse(template: str, env: JSONPathEnvironment = None) -> ParametrizedExpression:
    """
    Parse JSONPath expression with C-style placeholders.

    Args:
        template: JSONPath with %s, %d, %f or %(name)s placeholders
        env: JSONPath environment (optional)

    Returns:
        ParametrizedExpression that can be executed with different parameter values

    Examples:
        >>> expr = parse("$[?@.age > %d]")
        >>> result = expr.find(data, (27,))
        >>>
        >>> expr = parse("$[?@.name == %(name)s]")
        >>> result = expr.find(data, {"name": "Alice"})
    """
    return ParametrizedExpression(template, env)
