"""
There is no live api for bcse, so let's try to read it periodically. Once per 15 seconds.

"""
import asyncio
import datetime
import simplejson
import logging
from collections import OrderedDict
from typing import List, Optional, Sequence

from aiohttp.client import ClientSession
from aioredis import Redis

import byn.constants as const
from byn.postgres_db import insert_bcse, get_bcse_in
from byn.datatypes import BcseData, PredictCommand
from byn.utils import always_on_coroutine, create_redis, atuple
from byn.realtime.synchronization import (
    mark_as_ready,
    BCSE as BCSE_IS_READY,
    send_predictor_command,
)


logger = logging.getLogger(__name__)

EXTRA_BCSE_HOLIDAYS_ANY_YEAR = (
    (1, 1),
    (1, 7),
    (3, 8),
    (5, 1),
    (5, 9),
    (5, 9),
    (7, 3),
    (11, 7),
    (12, 25),
)

EXTRA_BCSE_HOLIDAYS = (
    datetime.date(2019, 5, 6),
    datetime.date(2019, 5, 7),
    datetime.date(2019, 5, 8),
    datetime.date(2019, 11, 8),
)

EXTRA_BCSE_WORKDAYS = (
    datetime.date(2019, 5, 4),
    datetime.date(2019, 5, 11),
    datetime.date(2019, 11, 16),
)


def _get_todays_bcse_start(date: datetime.date):
    return datetime.datetime(date.year, date.month, date.day, 9, 55)


def _get_todays_bcse_finish(date: datetime.date):
    return datetime.datetime(date.year, date.month, date.day, 13, 15)


def bcse_is_open(current_dt: datetime.datetime) -> bool:
    today = current_dt.date()

    if is_holiday(today):
        return False

    if _get_todays_bcse_start(today) <= current_dt < _get_todays_bcse_finish(today):
        return True

    return False


def _get_open_time(current_dt: datetime.datetime) -> datetime.datetime:
    """

    :param current_dt: we're sure that this is not a bcse work time.
    :return: closest future bcse open time.
    """

    if current_dt > _get_todays_bcse_finish(current_dt.date()) or is_holiday(current_dt.date()):
        current_dt = datetime.datetime(current_dt.year, current_dt.month, current_dt.day) + datetime.timedelta(days=1)

    while is_holiday(current_dt.date()):
        current_dt += datetime.timedelta(days=1)


    return _get_todays_bcse_start(current_dt.date())


async def _build_initial_current_records(today: datetime.date):
    return OrderedDict(await get_bcse_in(
        'USD',
        _get_todays_bcse_start(today),
        datetime.datetime.fromordinal((today + datetime.timedelta(days=1)).toordinal())
    ))


@always_on_coroutine
async def _listen_to_bcse_till(finish_datetime):
    today = datetime.date.today()
    current_records = await _build_initial_current_records(today)
    redis = await create_redis()
    await mark_as_ready(BCSE_IS_READY)

    async with ClientSession() as client:
        while datetime.datetime.now() < finish_datetime:
            await _extract_and_publish(
                redis=redis,
                client=client,
                today=today,
                current_records=current_records
            )
            await asyncio.sleep(const.BCSE_UPDATE_INTERVAL)


async def _extract_and_publish(today, current_records, redis, client):
    data = await _extract_bcse_rates(client, today)
    if data is None:
        return

    data = [(dt // 1000 - 60 * 60 * const.FIX_BCSE_TIMESTAMP, rate) for dt, rate in data]
    current_timestamp = int(datetime.datetime.now().timestamp())

    new_data = [
        BcseData(
            currency='USD',
            timestamp_operation=dt,
            timestamp_received=current_timestamp,
            rate=rate
        )
        for dt, rate in data
        if (dt not in current_records) or (current_records[dt] != rate)
    ]

    logger.debug('New bcse data: %s', new_data)

    asyncio.create_task(insert_bcse(new_data))

    if len(new_data) > 0:
        asyncio.create_task(_notify_about_new_bcse(redis, data))

    current_records.update([(x.timestamp_operation, x.rate) for x in new_data])


async def _extract_bcse_rates(client: ClientSession, date: datetime.date) -> Optional[List[List]]:
    try:
        response = await client.get(
            f'https://banki24.by/exchange/last/USD/{date.isoformat()}'
        )
    except asyncio.CancelledError as e:
        raise e
    except:
        logger.exception('Unexpected exception while extracting bcse rates.')
        return None

    raw_data = await response.read()
    raw_data = simplejson.loads(raw_data.decode(), parse_float=str)
    required_raw_data_item = next(
        filter(lambda x: x['color'] == const.BCSE_LAST_OPERATION_COLOR, raw_data),
        None
    )
    if required_raw_data_item is None:
        logger.error('Unexpected bcse data format: %s', raw_data)
        return None

    if 'data' not in required_raw_data_item:
        logger.info('No bcse data.')
    elif not required_raw_data_item['data']:
        logger.debug('Empty bcse data.')

    return required_raw_data_item.get('data')


@always_on_coroutine
async def _notify_about_new_bcse(redis: Redis, data: List[Sequence]):
    await send_predictor_command(
        redis,
        command=PredictCommand.NEW_BCSE,
        data={
            'rates': data
        }
    )


def is_holiday(date: datetime.date) -> bool:
    if date in EXTRA_BCSE_WORKDAYS:
        return False

    if date in EXTRA_BCSE_HOLIDAYS:
        return True

    if (date.month, date.day) in EXTRA_BCSE_HOLIDAYS_ANY_YEAR:
        return True

    if date.isoweekday() in (6, 7):
        return True

    return False


@always_on_coroutine
async def listen_bcse():
    while True:
        current_dt = datetime.datetime.now()

        if bcse_is_open(current_dt):
            await _listen_to_bcse_till(_get_todays_bcse_finish(current_dt.date()))
            current_dt = datetime.datetime.now()

        else:
            await mark_as_ready(BCSE_IS_READY)

        next_time = _get_open_time(current_dt)
        wait_for = (next_time - current_dt).total_seconds()
        logger.info('BCSE reader gonna sleep for %s', wait_for)
        await asyncio.sleep(wait_for)
