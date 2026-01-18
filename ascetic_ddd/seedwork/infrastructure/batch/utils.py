"""Utility functions for batch query operations."""
import re
import typing


__all__ = (
    "is_insert_query",
    "is_autoincrement_insert_query",
    "convert_named_to_positional",
    "RE_INSERT_VALUES",
    "RE_NAMED_PARAM",
)


# Pattern for matching VALUES clause in INSERT queries
# Matches: VALUES (%s, %s, ...) or VALUES (%(a)s, %(b)s, ...)
# Uses (?:[^)]|\)s)+ to handle )s inside %(name)s
RE_INSERT_VALUES = re.compile(r"VALUES\s*(\((?:[^)]|\)s)+\))", re.IGNORECASE)

# Pattern for matching named parameters %(name)s
RE_NAMED_PARAM = re.compile(r"%\((\w+)\)s")


def convert_named_to_positional(
    query: str,
    params: typing.Mapping[str, typing.Any],
) -> tuple[str, tuple[typing.Any, ...]]:
    """
    Convert query with named params to positional params.

    Args:
        query: SQL query with named placeholders %(name)s
        params: Mapping of parameter names to values

    Returns:
        Tuple of (converted_query, positional_params)

    Example:
        query = "INSERT INTO t (a, b) VALUES (%(a)s, %(b)s)"
        params = {'a': 1, 'b': 'x'}
        result = ("INSERT INTO t (a, b) VALUES (%s, %s)", (1, 'x'))
    """
    # Extract param names in order of appearance
    param_names = RE_NAMED_PARAM.findall(query)

    # Build positional params tuple
    positional_params = tuple(params[name] for name in param_names)

    # Convert query: %(name)s -> %s
    converted_query = RE_NAMED_PARAM.sub("%s", query)

    return converted_query, positional_params


def is_insert_query(query: str) -> bool:
    """
    Check if query is an INSERT without RETURNING clause.

    Args:
        query: SQL query string

    Returns:
        True if it's an INSERT without RETURNING
    """
    trimmed = query.strip().upper()
    return trimmed.startswith("INSERT") and "RETURNING" not in trimmed


def is_autoincrement_insert_query(query: str) -> bool:
    """
    Check if query is an INSERT with RETURNING clause (auto-increment PK).

    Args:
        query: SQL query string

    Returns:
        True if it's an INSERT with RETURNING
    """
    trimmed = query.strip().upper()
    return trimmed.startswith("INSERT") and "RETURNING" in trimmed
