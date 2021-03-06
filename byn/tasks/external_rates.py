"""
RUB, UAH, EUR, DXY rates from profinance.ru (former forexpf) with a minute detalization.

Periodical: once a day.
"""
import asyncio
import datetime
import simplejson

from celery import group

from byn import constants as const
from byn import forexpf
from byn.postgres_db import insert_external_rates, get_last_external_currency_datetime
from byn.tasks.launch import app


RESOLUTIONS = (1, 3, 5, 15, 30, 60, 120)


@app.task(autoretry_for=(Exception, ), retry_backoff=True)
def extract_one_currency(start_dt: datetime.datetime, currency: str):
    end_dt = datetime.datetime.now()
    data_to_store = []


    last_time = end_dt

    for resolution in RESOLUTIONS:
        if last_time <= start_dt:
            break

        data = forexpf.get_forexpf_tuples(
            currency=currency, resolution=resolution, start_dt=start_dt, end_dt=last_time
        )

        data_to_store.extend(data)

        last_time = datetime.datetime.fromtimestamp(data[0][0])

    with open(const.EXTERNAL_RATE_DATA % currency, mode='wt') as f:
        simplejson.dump(data_to_store, f)


@app.task(autoretry_for=(Exception, ), retry_backoff=True)
def load_one_currency(currency: str):
    with open(const.EXTERNAL_RATE_DATA % currency, mode='rt') as f:
        data = simplejson.load(f, parse_float=str)

    asyncio.run(insert_external_rates(currency, data))


async def build_task_update_one_currency(currency: str):
    start_dt = await get_last_external_currency_datetime(currency)
    return extract_one_currency.si(start_dt, currency) | load_one_currency.si(currency)


async def build_task_update_all_currencies():
    return group([await build_task_update_one_currency(x) for x in forexpf.CURRENCY_CODES.keys()])


@app.task
def update_all_currencies_async():
    asyncio.run(build_task_update_all_currencies())()


def extend_dump_by_forexpf_file(currency, file_path):
    """
    Helper method to create an initial dump.
    """
    with open(const.EXTERNAL_RATE_DATA % currency, mode='rt') as f:
        existing_data = simplejson.load(f)

    with open(file_path, mode='rt') as f:
        raw_new_data = simplejson.load(f)

    last_timestamp = min((x[0] for x in existing_data), default=10**10)

    new_data = [
        x for x in forexpf.forexpf_data_into_tuples(raw_new_data)
        if x[0] < last_timestamp
    ]

    existing_data.extend(new_data)

    with open(const.EXTERNAL_RATE_DATA % currency, mode='wt') as f:
        simplejson.dump(existing_data, f)
