"""
nbrb official rates.

Periodical: once a day.
"""
import asyncio
import datetime
import logging
from collections import defaultdict
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple, Iterable, Sequence, Coroutine

import numpy as np
import celery
from sklearn.neighbors import KNeighborsRegressor

import byn.constants as const
from byn import forexpf
from byn.datatypes import PredictCommand
from byn.tasks.launch import app
from byn.tasks.daily_predict import daily_predict
from byn.postgres_db import (
    get_last_nbrb_record,
    get_last_nbrb_global_with_rates,
    get_last_rolling_average_date,
    get_nbrb_gt,
    insert_nbrb,
    insert_trade_dates,
    insert_rolling_average,
    insert_dxy_12MSK,
    LAST_ROLLING_AVERAGE_MAGIC_DATE,
    NbrbKind,
)
from byn.utils import create_redis
from byn.realtime.synchronization import (
    NBRB,
    mark_as_ready,
    send_predictor_command,
)


try:
    import requests
    client = requests.Session()
except ImportError:
    pass

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


class NotifyAction(Enum):
    REBUILD = 0
    MARK_DONE = 1



@app.task
def update_nbrb_rates_async(
        need_last_date=True,
        notify_action: NotifyAction=NotifyAction.REBUILD
):
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
        notify.si(notify_action)
    )()


@app.task(
    autoretry_for=(Exception, ),
    retry_backoff=True,
)
def load_trade_dates() -> Sequence[str]:
    dates = client.get(TRADE_DATES_URL).json()
    asyncio.run(insert_trade_dates(dates))
    return dates[-50:]


@app.task(
    retry_backoff=60,
    retry_backoff_max=32 * 60,
    autoretry_for=(NoNewNbrbRateError, ),
    retry_kwargs={'max_retries': 3 * 24 * 2},
)
def extract_nbrb(last_trade_dates: Sequence[str], need_last_date=False) -> Iterable[Dict[str, str]]:
    record = asyncio.run(get_last_nbrb_record(NbrbKind.OFFICIAL))
    if record is None:
        logger.error('No nbrb rates!')
        return ()

    date_to_start = record.date + datetime.timedelta(days=2)
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



@app.task(
    autoretry_for=(Exception, ),
    retry_backoff=True,
)
def load_dxy_12MSK() -> Tuple[Iterable]:
    async def _implementation():
        record = await get_last_nbrb_record(NbrbKind.GLOBAL)
        date = record and record.date
        dates = [x.date for x in await get_nbrb_gt(date, kind=NbrbKind.OFFICIAL)]

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

        await insert_dxy_12MSK(rate_pairs)

        return rate_pairs

    return asyncio.run(_implementation())


@app.task
def load_nbrb_local() -> Tuple[dict]:
    async def _implementation():
        record = await get_last_nbrb_record(NbrbKind.LOCAL)
        date = record and record.date

        clean_data = await get_nbrb_gt(date, NbrbKind.OFFICIAL)

        data = tuple({
            'date': x.date,
            'USD': x.usd,
            'EUR': x.usd / x.eur,
            'RUB': x.usd / x.rub * 100,
            'UAH': x.usd / x.uah * 100,
        } for x in clean_data)

        await insert_nbrb(data, kind=NbrbKind.LOCAL)
        return data

    return asyncio.run(_implementation())


@app.task
def load_nbrb_global():
    async def _implementation():
        record = await get_last_nbrb_global_with_rates()
        date = record and record.date

        nbrb_local = await get_nbrb_gt(date, kind=NbrbKind.LOCAL)
        dxy = {
            x.date: x.dxy
            for x in await get_nbrb_gt(date, kind=NbrbKind.GLOBAL)
        }

        data = tuple({
            'date': x.date,
            'BYN': x.usd / dxy[x.date],
            'EUR': x.eur / dxy[x.date],
            'RUB': x.rub / dxy[x.date],
            'UAH': x.uah / dxy[x.date],
        } for x in nbrb_local)

        await insert_nbrb(data, kind=NbrbKind.GLOBAL)
        return data

    return asyncio.run(_implementation())



@app.task
def load_nbrb(rates):
    db_rates = defaultdict(dict)
    for rate in rates:
        db_rates[rate['Date']][rate['cur']] = Decimal(rate['rate'])

    for date, rates in db_rates.items():
        rates['date'] = date

    data = tuple(db_rates.values())
    asyncio.run(insert_nbrb(data, kind=NbrbKind.OFFICIAL))
    return data


@app.task
def load_rolling_average():
    last_rolling_average_date = asyncio.run(get_last_rolling_average_date())
    nbrb_rows = asyncio.run(get_nbrb_gt(None, kind=NbrbKind.GLOBAL))

    dates = [x.date for x in nbrb_rows]
    dates.append(LAST_ROLLING_AVERAGE_MAGIC_DATE)
    rates = np.array([(
        x.eur,
        x.rub,
        x.uah,
        x.dxy,
    ) for x in nbrb_rows])

    if last_rolling_average_date is None:
        start_index = 0
    else:
        start_index = dates.index(last_rolling_average_date) + 1

    rolling_averages = {}

    for i in range(start_index, len(rates) + 1):
        per_duration = {x: [] for x in const.ROLLING_AVERAGE_DURATIONS}
        rolling_averages[dates[i]] = per_duration
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if i < duration:
                continue

            for data_column in range(4):
                per_duration[duration].append(
                    Decimal(np.mean(rates[i - duration:i, data_column]))
                )

    mass_insert = []

    for date in rolling_averages:
        for duration in const.ROLLING_AVERAGE_DURATIONS:
            if rolling_averages[date][duration]:
                mass_insert.append(
                    insert_rolling_average(
                        date, duration, rolling_averages[date][duration]
                    )
                )

    async def _run_mass_insert(chunk: List[Coroutine]):
        await asyncio.gather(*chunk)

    for i in range(0, len(mass_insert), 10):
        asyncio.run(_run_mass_insert(mass_insert[i:i+10]))



@app.task
def notify(notify_action: NotifyAction):
    async def _notify_predictor():
        redis = await create_redis()
        if notify_action == NotifyAction.REBUILD:
            await send_predictor_command(redis, PredictCommand.REBUILD)
        elif notify_action == NotifyAction.MARK_DONE:
            await mark_as_ready(NBRB)
        else:
            raise ValueError(notify_action)

    asyncio.run(_notify_predictor())
