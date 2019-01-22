import dataclasses
import datetime
from decimal import Decimal
from typing import Collection, Tuple


@dataclasses.dataclass
class ExternalRateData:
    currency: str
    timestamp_open: int
    rate_open: str
    close: str
    low: str
    high: str
    volume: int
    timestamp_received: float


@dataclasses.dataclass
class BcseData:
    currency: str
    ms_timestamp_operation: int
    ms_timestamp_received: int
    rate: str


@dataclasses.dataclass
class PredictInput:
    eur: Decimal
    rub: Decimal
    uah: Decimal
    dxy: Decimal
    usd_byn: Collection[Tuple[datetime.datetime, Decimal]]


@dataclasses.dataclass
class PredictOutput:
    rate: Decimal
