import datetime
import logging
from collections import defaultdict
from typing import Tuple
from itertools import chain

import numpy as np

import byn.constants as const
from byn.predict.predictor import Predictor, RidgeWeight
from byn.predict.processor import GlobalToNormlizedDataProcessor
from byn.datatypes import LocalRates
from byn.hbase_db import (
    db,
    bytes_to_date,
    date_to_next_bytes,
    table,
    key_part,
    get_decimal,
    get_accumulated_error,
    NbrbKind,
)



logger = logging.getLogger(__name__)


_number_of_rates = 3
ROLLING_AVERAGE_LENGTH = len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates
X_LENGTH = _number_of_rates + len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates
EXTERNAL_RATES_COLUMNS = b'rate:eur', b'rate:rub', b'rate:uah'



def _get_full_X_Y(date: datetime.date) -> Tuple[np.ndarray, np.ndarray, Tuple[datetime.date]]:
    x = []
    y = []

    with db.connection() as connection:
        nbrb_table = connection.table('nbrb')
        rates = nbrb_table.scan(
            row_start=b'global|',
            row_stop=b'global|' + date_to_next_bytes(date)
        )

        rolling_average_table = connection.table('rolling_average')
        rolling_averages = rolling_average_table.scan(row_stop=date_to_next_bytes(date))

    rates_as_dict = {}
    byn_rates = {}

    for key, data in rates:
        _, date = key.split(b'|')
        rates_as_dict[date] = [data[x] for x in EXTERNAL_RATES_COLUMNS]
        byn_rates[date] = data[b'rate:byn']


    rolling_averages_as_dict = defaultdict(dict)

    for key, data in rolling_averages:
        date, duration = key.split(b'|')
        rolling_averages_as_dict[date][duration] = data.values()

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
        tuple(bytes_to_date(x) for x in rates_as_dict.keys())
    )


def _get_X_Y_with_empty_rolling(date: datetime.date) -> Tuple[np.ndarray, np.ndarray, Tuple[datetime.date]]:
    with table('nbrb') as nbrb_table:
        keys, rates = zip(
            *nbrb_table.scan(
                row_start=NbrbKind.GLOBAL.as_prefix,
                row_stop=NbrbKind.GLOBAL.as_prefix + date_to_next_bytes(date)
            )
        )


    x = np.full((len(rates), X_LENGTH), None, dtype='float64')
    for x_row, rate_row in zip(x, rates):
        x_row[:3] = [rate_row[col] for col in EXTERNAL_RATES_COLUMNS]

    y = np.array([x[b'rate:byn'] for x in rates], dtype='float64')

    return x, y, tuple(bytes_to_date(key_part(x, 1)) for x in keys)


def build_predictor(date: datetime.date, *, use_rolling=True) -> Predictor:
    if use_rolling:
        x, y, dates = _get_full_X_Y(date)
    else:
        x, y, dates = _get_X_Y_with_empty_rolling(date)

    pre_processor = GlobalToNormlizedDataProcessor()
    pre_processor.fit(x, y)
    x = pre_processor.transform_global_vectorized(x)

    accumulated_error = get_accumulated_error(date)
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


def get_rates_for_date(date: datetime.date) -> dict:
    with table('nbrb') as nbrb:
        return nbrb.row(f'global|{date:%Y-%m-%d}'.encode())


def build_and_predict_linear(date: datetime.date) -> float:
    predictor = build_predictor(
        date - datetime.timedelta(days=1),
        use_rolling=False
    )
    rates = get_rates_for_date(date)
    if not rates:
        raise ValueError(f"No rates for {date}")

    rates = LocalRates(
        eur=get_decimal(rates, b'rate:eur'),
        rub=get_decimal(rates, b'rate:rub'),
        uah=get_decimal(rates, b'rate:uah'),
        dxy=get_decimal(rates, b'rate:dxy'),
    )

    x = predictor.pre_processor.transform_global(
        rates,
        rolling_average=[None] * ROLLING_AVERAGE_LENGTH
    )[:3]

    logger.debug('Train: %s', predictor.x_train[:,:3])
    logger.debug('x: %s', x)

    return predictor._ridge_predict_with_one_model(x, weight=RidgeWeight.LINEAR)
