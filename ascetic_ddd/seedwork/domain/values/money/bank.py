from typing import Dict, Tuple

from .currency import Currency
from .expression import Expression
from .money import Money


class Bank:
    """
    Handles currency conversion using exchange rates.

    The Bank maintains a table of exchange rates and provides
    the reduce operation to convert expressions to a target currency.
    """

    def __init__(self):
        """Create a Bank with an empty rate table."""
        self._rates: Dict[Tuple[Currency, Currency], int] = {}

    def add_rate(self, from_currency: Currency, to_currency: Currency, rate: int) -> None:
        """
        Add an exchange rate.

        Args:
            from_currency: The source currency code
            to_currency: The target currency code
            rate: The exchange rate (how many from units equal one to unit)
        """
        self._rates[(from_currency, to_currency)] = rate

    def rate(self, from_currency: Currency, to_currency: Currency) -> int:
        """
        Get the exchange rate between two currencies.

        Args:
            from_currency: The source currency code
            to_currency: The target currency code

        Returns:
            The exchange rate. Returns 1 if converting to the same currency.
        """
        if from_currency == to_currency:
            return 1
        return self._rates[(from_currency, to_currency)]

    def reduce(self, source: Expression, to: Currency) -> Money:
        """
        Reduce an expression to a Money object in the target currency.

        Args:
            source: The expression to reduce
            to: The target currency code

        Returns:
            Money object in the target currency
        """
        return source.reduce(self, to)
