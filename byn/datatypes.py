import dataclasses


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
