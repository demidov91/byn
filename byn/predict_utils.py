import asyncio
import datetime
import logging
from collections import defaultdict
from typing import Tuple, List
from itertools import chain

import numpy as np

import byn.constants as const
from byn.predict.predictor import Predictor, RidgeWeight
from byn.predict.processor import GlobalToNormlizedDataProcessor
from byn.datatypes import LocalRates

from byn.postgres_db import (
    NbrbKind,
    get_accumulated_error,
    get_nbrb_lte,
    get_rolling_average_lte,
    get_nbrb_rate,
)



logger = logging.getLogger(__name__)


_number_of_rates = 3
ROLLING_AVERAGE_LENGTH = len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates
X_LENGTH = _number_of_rates + len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates
EXTERNAL_RATES_COLUMNS = 'eur', 'rub', 'uah'



async def _get_full_X_Y(date: datetime.date) -> Tuple[np.ndarray, np.ndarray, Tuple[datetime.date]]:
    x = []
    y = []

    future_nbrb = asyncio.create_task(get_nbrb_lte(date, NbrbKind.GLOBAL))
    future_rolling = asyncio.create_task(get_rolling_average_lte(date))

    rates = await future_nbrb
    rolling_averages = await future_rolling


    ##############

    rates_as_dict = {}
    byn_rates = {}

    async for row in rates:
        rates_as_dict[row.date] = [row[x] for x in EXTERNAL_RATES_COLUMNS]
        byn_rates[row.date] = row.byn


    rolling_averages_as_dict = defaultdict(dict)

    async for row in rolling_averages:
        rolling_averages_as_dict[row.date][row.duration] = [
            row[column] for column in EXTERNAL_RATES_COLUMNS
        ]

    for today in rates_as_dict:
        todays_X = np.full(X_LENGTH, None)
        todays_X[:3] = rates_as_dict[today]
        rolling = tuple(chain(*rolling_averages_as_dict[today].values()))
        todays_X[3:3 + len(rolling)] = rolling

        x.append(todays_X)
        y.append(byn_rates[today])

    return (
        np.array(x, dtype='float64'),
        np.array(y, dtype='float64'),
        tuple(rates_as_dict.keys())
    )


async def _get_X_Y_with_empty_rolling(date: datetime.date) -> Tuple[
    np.ndarray,
    np.ndarray,
    List[datetime.date]
]:
    rows = await get_nbrb_lte(date, NbrbKind.GLOBAL)

    x = np.full((len(rows), X_LENGTH), None, dtype='float64')
    for x_row, rate_row in zip(x, rows):
        x_row[:3] = [getattr(rate_row, col) for col in EXTERNAL_RATES_COLUMNS]

    y = np.array([x.byn for x in rows], dtype='float64')

    return x, y, [x.date for x in rows]


def build_predictor(date: datetime.date, *, use_rolling=True) -> Predictor:
    if use_rolling:
        x, y, dates = asyncio.run(_get_full_X_Y(date))
    else:
        x, y, dates = asyncio.run(_get_X_Y_with_empty_rolling(date))

    pre_processor = GlobalToNormlizedDataProcessor()
    pre_processor.fit(x, y)
    x = pre_processor.transform_global_vectorized(x)

    accumulated_error = asyncio.run(get_accumulated_error(date))
    if accumulated_error is None:
        logger.warning('Got no accumulated error for %s', date)
        accumulated_error = 0

    _cache_prefix = 'byn-7'

    predictor = Predictor(
        pre_processor=pre_processor,
        cache_prefix=_cache_prefix,
        accumulated_ridge_error=float(accumulated_error),
    )

    if use_rolling:
        predictor.rebuild(x, y, cache_key=date.strftime('%Y-%m-%d'))

    else:
        predictor.x_train = x
        predictor.y_train = y
        predictor._rebuild_ridge_model(cache_key=f'{_cache_prefix}_{date:%Y-%m-%d}')

    predictor.meta.last_date = dates[-1]
    predictor.meta.last_rolling_average = x[-1][3:]

    return predictor


def build_and_predict_linear(date: datetime.date) -> float:
    predictor = build_predictor(
        date - datetime.timedelta(days=1),
        use_rolling=False
    )
    rates = asyncio.run(get_nbrb_rate(date, NbrbKind.GLOBAL))
    if not rates:
        raise ValueError(f"No rates for {date}")

    rates = LocalRates(
        eur=rates.eur,
        rub=rates.rub,
        uah=rates.uah,
        dxy=rates.dxy,
    )

    x = predictor.pre_processor.transform_global(
        rates,
        rolling_average=[None] * ROLLING_AVERAGE_LENGTH
    )[:3]

    logger.debug('Train: %s', predictor.x_train[:,:3])
    logger.debug('x: %s', x)

    return predictor._ridge_predict_with_one_model(x, weight=RidgeWeight.LINEAR)
