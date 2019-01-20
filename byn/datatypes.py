import dataclasses
from decimal import Decimal


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