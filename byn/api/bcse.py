"""
There is no live api for bcse, so let's try to read it periodically. Once per 15 seconds.

"""
import asyncio
import datetime
import json
import logging
from collections import OrderedDict
from typing import List, Optional

from aiohttp.client import ClientSession
from aioredis import Redis

import byn.constants as const
from byn.cassandra_db import insert_bcse_async
from byn.datatypes import BcseData
from byn.utils import always_on_coroutine, create_redis


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
    return datetime.datetime(date.year, date.month, date.day, 9, 0)


def _get_todays_bcse_finish(date: datetime.date):
    return datetime.datetime(date.year, date.month, date.day, 13, 0)


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


@always_on_coroutine
async def _listen_to_bcse_till(finish_datetime):
    today = datetime.date.today()
    current_records = OrderedDict()
    is_first_iteration = True
    redis = await create_redis()

    async with ClientSession() as client:
        while datetime.datetime.now() < finish_datetime:
            if is_first_iteration:
                is_first_iteration = False
            else:
                await asyncio.sleep(const.BCSE_UPDATE_INTERVAL)


            data = await _extract_bcse_rates(client, today)
            if data is None:
                continue

            current_ms_timestamp = int(datetime.datetime.now().timestamp() * 1000)

            new_data = [
                BcseData(
                    currency='USD',
                    ms_timestamp_operation=dt,
                    ms_timestamp_received=current_ms_timestamp,
                    rate=rate
                )
                for dt, rate in data
                if (dt not in current_records) or (current_records[dt] != rate)
            ]

            results = insert_bcse_async(new_data)
            await _publish_bcse_in_redis(redis, data)

            for r in results:
                try:
                    r.result()
                except asyncio.CancelledError as e:
                    raise e
                except:
                    logger.exception("BCSE rate wasn't saved in cassandra.")

            else:
                current_records.update(new_data)


async def _extract_bcse_rates(client: ClientSession, date: datetime.date) -> Optional[List[List[int, str]]]:
    try:
        response = await client.get(
            f'https://banki24.by/exchange/last/USD/{date.isoformat()}'
        )
    except asyncio.CancelledError as e:
        raise e
    except:
        logger.exception('Unexpected exception while extracting bcse rates.')
        return None

    raw_data = await response.json(parse_float=str)
    data = next(
        filter(lambda x: x['color'] == const.BCSE_LAST_OPERATION_COLOR, raw_data),
        None
    )
    if data is None:
        logger.error('Unexpected bcse data format: %s', raw_data)
        return None

    return data


async def _publish_bcse_in_redis(redis: Redis, data: List[List[int, str]]):
    try:
        str_data = json.dumps(data)
        await redis.set(const.BCSE_REDIS_KEY, str_data)
    except asyncio.CancelledError as e:
        raise e
    except:
        logger.error("Couldn't publish bcse rates in redis.")


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
            await _listen_to_bcse_till(
                _get_todays_bcse_finish(current_dt.date()) +
                datetime.timedelta(minutes=15)
            )
            current_dt = datetime.datetime.now()

        next_time = _get_open_time(current_dt)
        wait_for = (next_time - current_dt).total_seconds()
        logger.info('BCSE reader gonna sleep for %s', wait_for)
        await asyncio.sleep(wait_for)
