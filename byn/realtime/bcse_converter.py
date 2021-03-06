import asyncio
import datetime
import logging
from itertools import chain

import numpy as np

from byn.postgres_db import (
    get_latest_external_rates,
    get_external_rate_live,
)
from byn.realtime.detailed_rates import RatesDetailedExtractor


logger = logging.getLogger(__name__)


class BcseConverter:
    def __init__(self):
        self.resolved_bcse_rates = {}
        self.fake_rates = {}


    async def update(self, bcse_pairs):
        new_bcse = [x for x in bcse_pairs if x[0] not in self.resolved_bcse_rates]

        if not new_bcse:
            logger.debug("Actually, no new bcse data.")
            return

        start_dt = datetime.datetime.fromtimestamp(new_bcse[0][0])
        external_rates_extractor = await build_rates_extractor(start_dt)

        for point in new_bcse:
            self.resolved_bcse_rates[point[0]] = external_rates_extractor.get_by_timestamp(point[0])

    def get_by_timestamp(self, timestamp):
        return self.resolved_bcse_rates[timestamp]

    def get_fake_rate(self, timestamp: int):
        return self.fake_rates.get(timestamp)

    def set_fake_rate(self, timestamp: int, rate: float):
        self.fake_rates[timestamp] = rate


async def build_rates_extractor(start_dt: datetime.datetime):
    external_live_data = await get_external_rate_live(start_dt=start_dt - datetime.timedelta(minutes=1))
    external_historical_data = await get_latest_external_rates(start_dt=start_dt, at_least_one=True)
    return RatesDetailedExtractor(
        _join_external_rates(external_live_data, external_historical_data)
    )


def _join_external_rates(one, two):
    currencies = set(one.keys())
    currencies.update(two.keys())

    return {
        currency: np.array(tuple(
            sorted(chain(one.get(currency, ()), two.get(currency, ())), key=lambda x: x[0])
        ))
        for currency in currencies
    }

