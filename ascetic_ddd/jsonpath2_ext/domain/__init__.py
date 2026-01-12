"""
JSONPath specification infrastructure.

This package provides extension for jsonpath2 parser to support
parameterized expressions with placeholders.
"""

from .jsonpath2_parameterized_parser import parse, ParametrizedPath
from .jsonpath2_parameterized_filter_fix import install_filter_fix

# Auto-install filter fix on import
install_filter_fix()

__all__ = [
    "parse",
    "ParametrizedPath",
]
