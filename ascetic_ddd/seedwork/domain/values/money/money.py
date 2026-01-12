from abc import ABCMeta
from typing import TYPE_CHECKING

from .currency import Currency
from .expression import Expression

if TYPE_CHECKING:
    from .bank import Bank


class Money(Expression):
    """
    Represents a monetary amount in a specific currency.

    This is the fundamental class in the Money pattern, representing
    an amount with its associated currency.
    """

    def __init__(self, amount: int, currency: Currency):
        """
        Create a Money object.

        Args:
            amount: The amount (in minor units, e.g., cents)
            currency: The currency code (e.g., "USD", "CHF")
        """
        self._amount = amount
        self._currency = currency

    @property
    def amount(self) -> int:
        """Get the amount."""
        return self._amount

    def currency(self) -> str:
        """Get the currency code."""
        return self._currency

    @staticmethod
    def dollar(amount: int) -> "Money":
        """Create a Money object in USD."""
        return Money(amount, Currency.USD)

    def __eq__(self, other: object) -> bool:
        """
        Check equality with another Money object.

        Two Money objects are equal if they have the same amount
        and currency.
        """
        if not isinstance(other, Money):
            return False
        return self._amount == other._amount and self._currency == other._currency

    def __hash__(self) -> int:
        """Make Money hashable."""
        return hash((self._amount, self._currency))

    def __str__(self) -> str:
        """String representation."""
        return f"{self._amount} {self._currency}"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Money({self._amount}, '{self._currency}')"

    def times(self, multiplier: int) -> Expression:
        """
        Multiply this Money by a scalar.

        Args:
            multiplier: The multiplier

        Returns:
            A new Money object with the multiplied amount
        """
        return Money(self._amount * multiplier, self._currency)

    def plus(self, addend: Expression) -> Expression:
        """
        Add another expression to this Money.

        Args:
            addend: The expression to add

        Returns:
            A Sum expression representing the addition
        """
        from .sum import Sum
        return Sum(self, addend)

    def reduce(self, bank: "Bank", to: Currency) -> "Money":
        """
        Reduce this Money to the target currency.

        Args:
            bank: The bank to use for currency conversion
            to: The target currency code

        Returns:
            Money object in the target currency
        """
        rate = bank.rate(self._currency, to)
        return Money(self._amount // rate, to)

    def export(self, exporter: "IMoneyExporter") -> None:
        exporter.set_amount(self._amount)
        exporter.set_currency(self._currency)


class IMoneyExporter(metaclass=ABCMeta):
    def set_amount(self, value: int) -> None:
        raise NotImplementedError

    def set_currency(self, value: Currency) -> None:
        raise NotImplementedError

