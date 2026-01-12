from .currency import Currency
from .money import IMoneyExporter

__all__ = ('MoneyExporter',)


class MoneyExporter(IMoneyExporter):
    def __init__(self):
        self.data = {}

    def set_amount(self, value: int) -> None:
        self.data['amount'] = value

    def set_currency(self, value: Currency) -> None:
        value.export(lambda val: self.data.update({'currency': val}))
