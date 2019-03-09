"""
nbrb official rates.

Periodical: once a day.
"""
import asyncio
import datetime
import json
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Tuple, Iterable, Sequence

import numpy as np
import requests
import celery
from sklearn.neighbors import KNeighborsRegressor

import byn.constants as const
from byn import forexpf
from byn.datatypes import PredictCommand
from byn.tasks.launch import app
from byn.tasks.daily_predict import daily_predict
from byn.hbase_db import (
    add_nbrb_global,
    get_last_nbrb_record,
    get_last_nbrb_global_with_rates,
    get_last_rolling_average_date,
    get_nbrb_gt,
    insert_nbrb,
    insert_nbrb_local,
    insert_trade_dates,
    insert_rolling_average,
    insert_dxy_12MSK,
    key_part,
    key_part_as_date,
    get_decimal,
    NbrbKind,
)
from byn.utils import create_redis
from byn.realtime.synchronization import send_predictor_command


client = requests.Session()
logger = logging.getLogger(__name__)


CURR_IDS = {
    '145': 'USD',
    '292': 'EUR',
    '298': 'RUB',
    '290': 'UAH',
}

TRADE_DATES_URL = 'https://banki24.by/exchange/allowed?code=USD'


class NoNewNbrbRateError(ValueError):
    pass


@app.task
def update_nbrb_rates_async(need_last_date=True):
    return (
        load_trade_dates.si() |
        extract_nbrb.s(need_last_date=need_last_date) |
        load_nbrb.s() |
        celery.group(
            load_nbrb_local.si(),
            load_dxy_12MSK.si()
        ) |
        load_nbrb_global.si() |
        celery.group(
            load_rolling_average.si(),
            daily_predict.si(),
        ) |
        notify_predictor.si()
    )()


@app.task
def load_trade_dates() -> Sequence[str]:
    dates = client.get(TRADE_DATES_URL).json()
    insert_trade_dates(dates)
    return dates[-50:]


@app.task(
    retry_backoff=60,
    retry_backoff_max=32 * 60,
    autoretry_for=(NoNewNbrbRateError, ),
    retry_kwargs={'max_retries': 3 * 24 * 2},
)
def extract_nbrb(last_trade_dates: Sequence[str], need_last_date=False) -> Iterable[Dict[str, str]]:
    record = get_last_nbrb_record(NbrbKind.OFFICIAL)
    if record is None:
        logger.error('No nbrb rates!')
        return ()

    date_to_start = key_part_as_date(record[0], 1) + datetime.timedelta(days=2)
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

    required_nbrb_records = [x for x in formatted_rates if x['Date'] in last_trade_dates]
    if need_last_date:
        for record in required_nbrb_records:
            if record['Date'] == last_trade_dates[-1]:
                break
        else:
            raise NoNewNbrbRateError(last_trade_dates[-1])

    return required_nbrb_records



@app.task
def load_dxy_12MSK() -> Tuple[Iterable]:
    record = get_last_nbrb_record(NbrbKind.GLOBAL)
    date = record and key_part_as_date(record[0], 1)
    dates = [key_part_as_date(x[0], 1) for x in get_nbrb_gt(date, kind=NbrbKind.OFFICIAL)]

    if len(dates) == 0:
        return ()

    start_date = dates[0]
    end_date = dates[-1] + datetime.timedelta(days=1)

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
    rate_pairs = tuple(zip(dates, [str(x) for x in dxy_regressor.predict(timestamps)]))

    insert_dxy_12MSK(rate_pairs)

    return rate_pairs


@app.task
def load_nbrb_local() -> Tuple[dict]:
    record = get_last_nbrb_record(NbrbKind.LOCAL)
    date = record and key_part_as_date(record[0], 1)

    clean_data = get_nbrb_gt(date, NbrbKind.OFFICIAL)

    data = tuple({
        'date': key_part(x[0], 1),
        'USD': get_decimal(x, b'rate:usd'),
        'EUR': get_decimal(x, b'rate:usd') / get_decimal(x, b'rate:usd'),
        'RUB': get_decimal(x, b'rate:usd') / get_decimal(x, b'rate:rub') * 100,
        'UAH': get_decimal(x, b'rate:usd') / get_decimal(x, b'rate:uah') * 100,
    } for x in clean_data)
    insert_nbrb_local(data)
    return data


@app.task
def load_nbrb_global():
    record = get_last_nbrb_global_with_rates()
    date = record and key_part_as_date(record[0], 1)

    nbrb_local = get_nbrb_gt(date, kind=NbrbKind.LOCAL)
    dxy = {x.date: x.dxy for x in get_nbrb_global_gt(date)}

    data = tuple({
        'date': key_part(x[0], 1),
        'BYN': get_decimal(x, b'rate:usd') / dxy[key_part(x[0], 1)],
        'EUR': get_decimal(x, b'rate:eur') / dxy[key_part(x[0], 1)],
        'RUB': get_decimal(x, b'rate:rub') / dxy[key_part(x[0], 1)],
        'UAH': get_decimal(x, b'rate:uah') / dxy[key_part(x[0], 1)],
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
    insert_nbrb(data)
    return data


@app.task
def load_rolling_average():
    last_rolling_average_date = get_last_rolling_average_date()
    data = tuple(get_nbrb_gt(None, kind=NbrbKind.GLOBAL))
    dates = [key_part_as_date(x[0], 1) for x in data]
    rates = np.array([
        (
            get_decimal(x, b'rate:eur'),
            get_decimal(x, b'rate:rub'),
            get_decimal(x, b'rate:uah'),
            get_decimal(x, b'rate:dxy'),
        )
        for x in data
    ])

    if last_rolling_average_date is None:
        start_index = 0
    else:
        start_index = dates.index(last_rolling_average_date) + 1

    rolling_averages = {}

    for i in range(start_index, len(rates)):
        per_duration = {x: [] for x in const.ROLLING_AVERAGE_DURATIONS}
        rolling_averages[dates[i]] = per_duration
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if i + 1 < duration:
                continue

            for data_column in range(4):
                per_duration[duration].append(
                    Decimal(np.mean(rates[i - duration + 1:i+1, data_column]))
                )

    for date in rolling_averages:
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if rolling_averages[date][duration]:
                insert_rolling_average(date, duration, rolling_averages[date][duration])


@app.task
def notify_predictor():
    async def _notify_predictor():
        redis = await create_redis()
        await send_predictor_command(redis, PredictCommand.REBUILD)

    asyncio.run(_notify_predictor())


def nbrb_file_to_cassandra():
    with open(const.CLEAN_NBRB_DATA, mode='rt') as f:
        data = json.load(f)

    load_nbrb(data)
