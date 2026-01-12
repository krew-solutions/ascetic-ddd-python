"""Interfaces for Specification pattern operands."""
from typing import Protocol, runtime_checkable


@runtime_checkable
class IEqualOperand(Protocol):
    """Interface for operands that support equality comparison."""

    def __eq__(self, other: "IEqualOperand") -> bool:
        """Check if this operand equals another."""
        ...


@runtime_checkable
class ILessThanOperand(Protocol):
    """Interface for operands that support less-than comparison."""

    def __lt__(self, other: "ILessThanOperand") -> bool:
        """Check if this operand is less than another."""
        ...


@runtime_checkable
class IGreaterThanOperand(Protocol):
    """Interface for operands that support greater-than comparison."""

    def __gt__(self, other: "IGreaterThanOperand") -> bool:
        """Check if this operand is greater than another."""
        ...


@runtime_checkable
class IGreaterThanEqualOperand(Protocol):
    """Interface for operands that support greater-than-or-equal comparison."""

    def __ge__(self, other: "IGreaterThanEqualOperand") -> bool:
        """Check if this operand is greater than or equal to another."""
        ...


@runtime_checkable
class ILessThanEqualOperand(Protocol):
    """Interface for operands that support less-than-or-equal comparison."""

    def __le__(self, other: "ILessThanEqualOperand") -> bool:
        """Check if this operand is less than or equal to another."""
        ...
