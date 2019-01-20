"""
There is no live api for bcse, so let's try to read it periodically. Once per 15 seconds.

"""
import asyncio
import datetime
import logging

from aiohttp.client import ClientSession

import byn.constants as const
from byn.utils import always_on_coroutine


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


async def _listen_to_bcse_till(finish_datetime):
    async with ClientSession() as client:
        while datetime.datetime.now() < finish_datetime:
            print('work')
            await asyncio.sleep(const.BCSE_UPDATE_INTERVAL)


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

        next_time = _get_open_time(current_dt)
        wait_for = (next_time - current_dt).total_seconds()
        logger.info('BCSE reader gonna sleep for %s', wait_for)
        await asyncio.sleep(wait_for)
