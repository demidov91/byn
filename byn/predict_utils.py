import datetime
import logging
from collections import defaultdict
from typing import Tuple
from itertools import chain

import numpy as np

import byn.constants as const
from byn.predict.predictor import Predictor, RidgeWeight
from byn.predict.processor import GlobalToNormlizedDataProcessor
from byn.cassandra_db import db
from byn.datatypes import LocalRates



logger = logging.getLogger(__name__)


_number_of_rates = 3
ROLLING_AVERAGE_LENGTH = len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates
X_LENGTH = _number_of_rates + len(const.ROLLING_AVERAGE_DURATIONS) * _number_of_rates



def _get_full_X_Y(date: datetime.date) -> Tuple[np.ndarray, np.ndarray, Tuple[datetime.date]]:
    x = []
    y = []

    rates = db.execute(
        'SELECT date, byn, eur, rub, uah FROM nbrb_global WHERE dummy=True and date<=%s ORDER BY date ASC',
        (date,)
    )
    rolling_averages = tuple(db.execute(
        'SELECT date, duration, eur, rub, uah FROM rolling_average WHERE date<=%s ALLOW FILTERING',
        (date,)
    ))

    rates_as_dict = {x[0]: x[1:] for x in list(rates)}
    rolling_averages_as_dict = defaultdict(dict)

    for row in rolling_averages:
        rolling_averages_as_dict[row.date][row.duration] = row[2:]

    for today in rates_as_dict:
        todays_X = np.full(X_LENGTH, None)
        todays_X[:3] = rates_as_dict[today][1:]
        rolling = tuple(chain(*rolling_averages_as_dict[today].values()))
        todays_X[3:3 + len(rolling)] = rolling

        x.append(todays_X)
        y.append(rates_as_dict[today][0])

    return np.array(x),  np.array(y), tuple((x.date() for x in rates_as_dict.keys()))


def _get_X_Y_with_empty_rolling(date: datetime.date) -> Tuple[np.ndarray, np.ndarray, Tuple[datetime.date]]:
    rates = tuple(db.execute(
        'SELECT date, byn, eur, rub, uah FROM nbrb_global WHERE dummy=True and date<=%s ORDER BY date ASC',
        (date,)
    ))

    for i in range(1, len(rates) - 1):
        if rates[i-1] > rates[i]:
            raise ValueError()

    x = np.full((len(rates), X_LENGTH), None)
    for x_row, rate_row in zip(x, rates):
        x_row[:3] = rate_row[2:]

    y = np.array([x[1] for x in rates])

    return x, y, tuple((x.date.date() for x in rates))


def build_predictor(date: datetime.date, *, ridge_weight: RidgeWeight, use_rolling=True) -> Predictor:
    if use_rolling:
        x, y, dates = _get_full_X_Y(date)
    else:
        x, y, dates = _get_X_Y_with_empty_rolling(date)

    x = np.array(x, dtype='float64')
    y = np.array(y, dtype='float64')

    pre_processor = GlobalToNormlizedDataProcessor()
    pre_processor.fit(x, y)
    x = pre_processor.transform_global_vectorized(x)

    predictor = Predictor(
        pre_processor=pre_processor,
        ridge_weights=ridge_weight, cache_prefix='byn-4'
    )

    if use_rolling:
        predictor.rebuild(x, y, cache_key=date.strftime('%Y-%m-%d'))

    else:
        predictor.x_train = x
        predictor.y_train = y
        predictor._rebuild_ridge_model(cache_key='byn-7_' + date.strftime('%Y-%m-%d'))

    predictor.meta.last_date = dates[-1]

    return predictor


def get_rates_for_date(date: datetime.date):
    return next(iter(db.execute(
        'SELECT eur, rub, uah, dxy FROM nbrb_global WHERE dummy=True and date=%s',
        (date, )
    )), None)


def build_and_predict_linear(date: datetime.date) -> float:
    predictor = build_predictor(
        date - datetime.timedelta(days=1),
        ridge_weight=RidgeWeight.LINEAR,
        use_rolling=False
    )
    rates = get_rates_for_date(date)
    if rates is None:
        raise ValueError(f"No rates for {date}")

    rates = LocalRates(eur=rates.eur, rub=rates.rub, uah=rates.uah, dxy=rates.dxy)

    x = predictor.pre_processor.transform_global(
        rates,
        rolling_average=[None] * ROLLING_AVERAGE_LENGTH
    )[:3]

    logger.debug('Train: %s', predictor.x_train[:,:3])
    logger.debug('x: %s', x)

    return predictor._ridge_predict_with_one_model(x, weight=RidgeWeight.LINEAR)
