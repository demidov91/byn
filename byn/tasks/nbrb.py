"""
nbrb official rates.

Periodical: once a day.
"""
import datetime
import json
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Collection, Dict, Tuple, Iterable

import numpy as np
import requests
import celery
from sklearn.neighbors import KNeighborsRegressor

import byn.constants as const
from byn import forexpf
from byn.tasks.launch import app
#from byn.tasks.accamulated_error import update_prediction_error
from byn.cassandra_db import (
    get_last_nbrb_rates,
    get_last_nbrb_local_rates,
    get_last_nbrb_global_record,
    get_last_nbrb_global_with_rates,
    get_last_rolling_average_date,
    get_nbrb_gt,
    get_nbrb_local_gt,
    get_nbrb_global_gt,
    insert_trade_dates,
    insert_nbrb_rates,
    insert_nbrb_local,
    add_nbrb_global,
    insert_dxy_12MSK,
    insert_rolling_average,
)


client = requests.Session()
logger = logging.getLogger(__name__)


CURR_IDS = {
    '145': 'USD',
    '292': 'EUR',
    '298': 'RUB',
    '290': 'UAH',
}

TRADE_DATES_URL = 'https://banki24.by/exchange/allowed?code=USD'


@app.task
def update_nbrb_rates_async():
    return (
        load_trade_dates.si() |
        extract_nbrb.s() |
        load_nbrb.s() |
        celery.group(
            load_nbrb_local.si(),
            load_dxy_12MSK.si()
        ) |
        load_nbrb_global.si() |
        load_rolling_average().si()# |
#        load_daily_predict().si()
    )()


@app.task
def load_trade_dates():
    dates = client.get(TRADE_DATES_URL).json()
    insert_trade_dates(dates)
    return dates[-50:]


@app.task
def extract_nbrb(last_trade_dates: Collection[str]) -> Iterable[Dict[str, str]]:
    record = get_last_nbrb_rates()
    if record is None:
        logger.error('No nbrb rates!')
        return ()

    date_to_start = record.date.date() + datetime.timedelta(days=2)
    date_to_end = datetime.date.today() + datetime.timedelta(days=1)

    if date_to_end < date_to_start:
        logger.info('Nothing to update.')
        return ()

    raw_rates = []
    for curr_id in CURR_IDS.keys():
        raw_rates.extend(
            client.get(
                f'http://www.nbrb.by/API/ExRates/Rates/Dynamics/{curr_id}?startDate={date_to_start:%Y-%m-%d}&endDate={date_to_end:%Y-%m-%d}'
            ).json(parse_int=Decimal, parse_float=Decimal)
        )

    formatted_rates = [
        {
            'Date': (datetime.datetime.strptime(x['Date'][:10], '%Y-%m-%d').date() - datetime.timedelta(days=1)).isoformat(),
            'cur': CURR_IDS[str(x['Cur_ID'])],
            'rate': str(x['Cur_OfficialRate']),
        } for x in raw_rates
    ]

    return [x for x in formatted_rates if x['Date'] in last_trade_dates]



@app.task
def load_dxy_12MSK() -> Tuple[Iterable]:
    record = get_last_nbrb_global_record()
    date = record and record.date.date()
    dates = [x.date.date() for x in get_nbrb_gt(date)]

    if len(dates) == 0:
        return ()

    start_date = dates[-1]
    end_date = dates[0] + datetime.timedelta(days=1)

    raw_data = forexpf.get_data(
        currency='DXY',
        resolution=60*4,
        start_dt=datetime.datetime.fromordinal(start_date.toordinal()),
        end_dt=datetime.datetime.fromordinal(end_date.toordinal())
    )

    dxy_regressor = KNeighborsRegressor(n_neighbors=2).fit(
        [[x] for x in raw_data['t']],
        raw_data['o'],
    )

    timestamps = [[datetime.datetime(x.year, x.month, x.day, 12).timestamp()] for x in dates]
    dates = [x.isoformat() for x in dates]
    rate_pairs = tuple(zip(dates, [Decimal(x) for x in dxy_regressor.predict(timestamps)]))

    insert_dxy_12MSK(rate_pairs)

    return rate_pairs


@app.task
def load_nbrb_local() -> Tuple[dict]:
    record = get_last_nbrb_local_rates()
    date = record and record.date.date()

    clean_data = get_nbrb_gt(date)

    data = tuple({
        'date': x.date,
        'USD': x.usd,
        'EUR': x.usd / x.eur,
        'RUB': x.usd / x.rub * 100,
        'UAH': x.usd / x.uah * 100,
    } for x in clean_data)
    insert_nbrb_local(data)
    return data


@app.task
def load_nbrb_global():
    record = get_last_nbrb_global_with_rates()
    date = record and record.date.date()

    nbrb_local = get_nbrb_local_gt(date)
    dxy = {x.date: x.dxy for x in get_nbrb_global_gt(date)}

    data = tuple({
        'date': x.date,
        'BYN': x.usd / dxy[x.date],
        'EUR': x.eur / dxy[x.date],
        'RUB': x.rub / dxy[x.date],
        'UAH': x.uah / dxy[x.date],
    } for x in nbrb_local)

    add_nbrb_global(data)

    return data



@app.task
def load_nbrb(rates):
    cass_rates = defaultdict(dict)
    for rate in rates:
        cass_rates[rate['Date']][rate['cur']] = Decimal(rate['rate'])

    for date, rates in cass_rates.items():
        rates['date'] = date

    data = tuple(cass_rates.values())
    insert_nbrb_rates(data)
    return data


@app.task
def load_rolling_average():
    last_rolling_average_date = get_last_rolling_average_date()
    data = tuple(get_nbrb_global_gt(0))[::-1]
    dates = [x[1].date() for x in data]
    rates = np.array([(x.eur, x.rub, x.uah, x.dxy) for x in data])

    if last_rolling_average_date is None:
        start_index = 0
    else:
        start_index = dates.index(last_rolling_average_date) + 1

    rolling_averages = {}

    for i in range(start_index, len(rates)):
        per_duration = {x: [] for x in const.ROLLING_AVERAGE_DURATIONS}
        rolling_averages[dates[i]] = per_duration
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if i < duration:
                continue

            for data_column in range(4):
                per_duration[duration].append(
                    Decimal(np.mean(rates[i - duration:i, data_column]))
                )

    for date in rolling_averages:
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if rolling_averages[date][duration]:
                insert_rolling_average(date, duration, rolling_averages[date][duration])



def nbrb_file_to_cassandra():
    with open(const.CLEAN_NBRB_DATA, mode='rt') as f:
        data = json.load(f)

    load_nbrb(data)


def dxy_12MSK_to_cassandra():
    with open(const.DXY_12MSK_DARA, mode='rt') as f:
        data = json.load(f)

    insert_dxy_12MSK([(x[0], Decimal(x[1])) for x in data])
