from typing import TYPE_CHECKING

from .expression import Expression

if TYPE_CHECKING:
    from .bank import Bank
    from .money import Money


class Sum(Expression):
    """
    Represents the sum of two monetary expressions.

    This class enables complex expressions like (5 USD + 10 CHF) + 20 EUR
    to be built up and then reduced to a single currency when needed.
    """

    def __init__(self, augend: Expression, addend: Expression):
        """
        Create a Sum expression.

        Args:
            augend: The first expression
            addend: The second expression
        """
        self.augend = augend
        self.addend = addend

    def reduce(self, bank: "Bank", to: str) -> "Money":
        """
        Reduce this Sum to a Money object in the target currency.

        This reduces both operands to the target currency and adds them.

        Args:
            bank: The bank to use for currency conversion
            to: The target currency code

        Returns:
            Money object representing the sum in the target currency
        """
        amount = self.augend.reduce(bank, to).amount + self.addend.reduce(bank, to).amount
        from .money import Money
        return Money(amount, to)

    def plus(self, addend: Expression) -> Expression:
        """
        Add another expression to this Sum.

        Args:
            addend: The expression to add

        Returns:
            A new Sum expression
        """
        return Sum(self, addend)

    def times(self, multiplier: int) -> Expression:
        """
        Multiply this Sum by a scalar.

        This distributes the multiplication: (a + b) * n = a*n + b*n

        Args:
            multiplier: The multiplier

        Returns:
            A new Sum expression with both operands multiplied
        """
        return Sum(self.augend.times(multiplier), self.addend.times(multiplier))
