import dataclasses
import datetime
from decimal import Decimal
from dataclasses import dataclass, astuple
from enum import Enum
from typing import Collection, Tuple, Optional


@dataclass
class ExternalRateData:
    currency: str
    timestamp_open: int
    rate_open: str
    close: str
    low: str
    high: str
    volume: int
    timestamp_received: float


@dataclass
class BcseData:
    currency: str
    timestamp_operation: int
    timestamp_received: int
    rate: str


@dataclass
class LocalRates:
    # eur/usd
    eur: Decimal
    # usd/rub
    rub: Decimal
    # usd/uah
    uah: Decimal
    # dollar index
    dxy: Optional[Decimal]

    def to_global(self) -> 'LocalRates':
        return LocalRates(
            eur=1 / self.eur / self.dxy,
            rub=self.rub / self.dxy,
            uah=self.uah / self.dxy,
            dxy=None
        )

    def __add__(self, other: 'LocalRates'):
        if other == 0:
            return self

        return LocalRates(*(a + b for a, b in zip(astuple(self), astuple(other))))

    def __radd__(self, other):
        return self.__add__(other)

    def __truediv__(self, divider):
        return LocalRates(*(x / divider for x in astuple(self)))


class PredictCommand(Enum):
    NEW_BCSE = 'NEW_BCSE'
    REBUILD = 'REBUILD'
    PREDICT = 'PREDICT'
