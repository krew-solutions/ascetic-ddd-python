import typing
from enum import Enum


class Currency(str, Enum):
    """Валюта"""

    USD = "USD"
    EUR = "EUR"
    RUB = "RUB"
    GBP = "GBP"

    def to_symbol(self) -> str:
        """Получить символ валюты"""
        mapping = {
            Currency.USD: "$",
            Currency.EUR: "€",
            Currency.RUB: "₽",
            Currency.GBP: "£",
        }
        return mapping[self]

    def export(self, setter: typing.Callable[[str], None]) -> None:
        setter(self.value)
