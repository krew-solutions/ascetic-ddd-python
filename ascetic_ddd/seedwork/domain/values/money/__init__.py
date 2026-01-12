"""
Money pattern implementation.

This is the final version of the Money pattern from Kent Beck's
"Test Driven Development By Example" book.

The pattern provides:
- Multi-currency support
- Expression-based arithmetic operations
- Currency conversion through a Bank
- Composite pattern for complex expressions (Sum)

Example usage:
    >>> from money import Money, Bank
    >>>
    >>> # Create money objects
    >>> five_dollars = Money.dollar(5)
    >>> ten_francs = Money.franc(10)
    >>>
    >>> # Set up a bank with exchange rates
    >>> bank = Bank()
    >>> bank.add_rate("CHF", "USD", 2)  # 2 CHF = 1 USD
    >>>
    >>> # Add different currencies
    >>> sum_expr = five_dollars.plus(ten_francs)
    >>> result = bank.reduce(sum_expr, "USD")
    >>> print(result)  # 10 USD
    >>>
    >>> # Multiply
    >>> result = Money.dollar(5).times(2)
    >>> print(result)  # 10 USD
"""

from .bank import Bank
from .currency import Currency
from .expression import Expression
from .money import Money, IMoneyExporter
from .money_exporter import MoneyExporter
from .sum import Sum

__all__ = ["Expression", "Money", "IMoneyExporter", "MoneyExporter", "Sum", "Bank", "Currency"]
