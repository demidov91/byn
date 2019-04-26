import asyncio
import datetime
import simplejson
import logging
import os
import time
from enum import Enum
from functools import wraps, partial

import aioredis

from byn import constants as const

logger = logging.getLogger(__name__)


def always_on_coroutine(coro=None, expected_exceptions=()):
    if coro is None:
        return partial(always_on_coroutine, expected_exceptions=expected_exceptions)

    @wraps(coro)
    async def  wrapper(*args, **kwargs):
        retry_counter = 0

        while True:
            if retry_counter > 0:
                logger.info('Trying to restart %s', coro)

            start_time = datetime.datetime.now()

            try:
                return await coro(*args, **kwargs)
            except asyncio.CancelledError as e:
                raise e

            except Exception as e:
                current_time = datetime.datetime.now()
                if current_time - start_time < datetime.timedelta(seconds=60):
                    retry_counter += 1
                else:
                    retry_counter = 1

                wait_for_before_restart = 2 ** (retry_counter - 1) if retry_counter < 10 else 8 * 60

                if type(e) in expected_exceptions:
                    logger.warning(
                        'Expected error %s while running %s. Restarting in %s seconds...',
                        type(e).__name__, coro.__name__, wait_for_before_restart
                    )

                else:
                    logger.exception(
                        'Unexpected error while running %s. Restarting in %s seconds...',
                        coro.__name__, wait_for_before_restart
                    )

                await asyncio.sleep(wait_for_before_restart)

    return wrapper


def always_on_sync(func=None, expected_exceptions=()):
    if func is None:
        return partial(always_on_sync, expected_exceptions=expected_exceptions)

    @wraps(func)
    def  wrapper(*args, **kwargs):
        retry_counter = 0

        while True:
            if retry_counter > 0:
                logger.info('Trying to restart %s', func.__name__)

            start_time = datetime.datetime.now()

            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt as e:
                raise e

            except Exception as e:
                current_time = datetime.datetime.now()
                if current_time - start_time < datetime.timedelta(seconds=60):
                    retry_counter += 1
                else:
                    retry_counter = 1

                wait_for_before_restart = 2 ** (retry_counter - 1) if retry_counter < 10 else 8 * 60

                if type(e) in expected_exceptions:
                    logger.warning(
                        'Expected error %s while running %s. Restarting in %s seconds...',
                        type(e).__name__, func.__name__, wait_for_before_restart
                    )

                else:
                    logger.exception(
                        'Unexpected error while running %s. Restarting in %s seconds...',
                        func.__name__, wait_for_before_restart
                    )

                time.sleep(wait_for_before_restart)

    return wrapper


async def create_redis() -> aioredis.Redis:
    return await aioredis.create_redis(os.environ["REDIS_URL"], db=const.REDIS_CACHE_DB)


class EnumAwareEncoder(simplejson.JSONEncoder):
    def default(self, o):
        if isinstance(o, Enum):
            return o.value
        return super().default(o)


async def alist(coro) -> list:
    return [x async for x in coro]


async def atuple(coro) -> tuple:
    return tuple(await alist(coro))


_not_set = object()

async def anext(async_iter, default=_not_set):
    try:
        return await (await async_iter.__aiter__()).__anext__()
    except StopAsyncIteration as e:
        if default is _not_set:
            raise e

        return default
