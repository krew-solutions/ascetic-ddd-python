from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .bank import Bank
    from .money import Money


class Expression(ABC):
    """
    Interface for monetary expressions.

    This is the core abstraction that allows Money and Sum to be treated uniformly,
    enabling complex operations like adding different currencies.
    """

    @abstractmethod
    def reduce(self, bank: "Bank", to: str) -> "Money":
        """
        Reduce this expression to a Money object in the target currency.

        Args:
            bank: The bank to use for currency conversion
            to: The target currency code

        Returns:
            Money object in the target currency
        """
        pass

    @abstractmethod
    def plus(self, addend: "Expression") -> "Expression":
        """
        Add another expression to this one.

        Args:
            addend: The expression to add

        Returns:
            A new Expression representing the sum
        """
        pass

    @abstractmethod
    def times(self, multiplier: int) -> "Expression":
        """
        Multiply this expression by a scalar.

        Args:
            multiplier: The multiplier

        Returns:
            A new Expression representing the product
        """
        pass
