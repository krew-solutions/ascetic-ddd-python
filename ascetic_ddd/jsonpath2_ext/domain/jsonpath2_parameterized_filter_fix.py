"""
Fix for jsonpath2 FilterSubscript.match() bug.

The original implementation returns a list instead of yielding,
which breaks the Generator contract.
"""
import jsonpath2.subscripts.filter
from jsonpath2.node import MatchData
from jsonpath2.nodes.terminal import TerminalNode
from typing import Generator


def patched_filter_match(
    self, root_value: object, current_value: object
) -> Generator[MatchData, None, None]:
    """
    Fixed match method for FilterSubscript.

    Original bug: returns [MatchData(...)] instead of yielding.
    Fixed version: yields MatchData when filter matches.
    """
    if self.expression.evaluate(root_value, current_value):
        yield MatchData(TerminalNode(), root_value, current_value)


def install_filter_fix():
    """Install the filter fix by monkey-patching jsonpath2."""
    jsonpath2.subscripts.filter.FilterSubscript.match = patched_filter_match


def uninstall_filter_fix():
    """Restore original (buggy) implementation."""
    # This is for testing purposes only
    def original_match(self, root_value: object, current_value: object):
        if self.expression.evaluate(root_value, current_value):
            return [MatchData(TerminalNode(), root_value, current_value)]
        return []

    jsonpath2.subscripts.filter.FilterSubscript.match = original_match
