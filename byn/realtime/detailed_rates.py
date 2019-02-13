from typing import Iterable

import numpy as np
from scipy.interpolate import interp1d

from byn.datatypes import LocalRates


class OneRateDetailedExtractor:
    def __init__(self, pairs: np.ndarray):
        self._timestamps = pairs[:, 0]
        self._rates = pairs[:,1]

        if len(pairs) > 1:
            self.model = interp1d(self._timestamps, self._rates)

    def get_by_timestamp(self, timestamp) -> float:
        if timestamp > self._timestamps[-1]:
            return float(self._rates[-1])

        return self.model(timestamp).item()

    def get_by_timestamps(self, timestamps: Iterable[int]) -> Iterable[float]:
        return [self.get_by_timestamp(x) for x in timestamps]


class RatesDetailedExtractor:
    def __init__(self, currency_to_rates: dict):
        self.extractors = {
            currency: OneRateDetailedExtractor(rates)
            for currency, rates in currency_to_rates.items()
        }

    def get_by_timestamp(self, timestamp: int) -> LocalRates:
        return LocalRates(
            eur=self.extractors['EUR'].get_by_timestamp(timestamp),
            rub=self.extractors['RUB'].get_by_timestamp(timestamp),
            uah=self.extractors['UAH'].get_by_timestamp(timestamp),
            dxy=self.extractors['DXY'].get_by_timestamp(timestamp),
        )

    def get_by_timestamps(self, timestamps: Iterable[int]) -> Iterable[LocalRates]:
        eur = self.extractors['EUR'].get_by_timestamps(timestamps)
        rub = self.extractors['RUB'].get_by_timestamps(timestamps)
        uah = self.extractors['UAH'].get_by_timestamps(timestamps)
        dxy = self.extractors['DXY'].get_by_timestamps(timestamps)

        for eur, rub, uah, dxy in zip(eur, rub, uah, dxy):
            yield LocalRates(
                eur=eur,
                rub=rub,
                uah=uah,
                dxy=dxy,
            )
