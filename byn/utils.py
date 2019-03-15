import asyncio
import datetime
import json
import logging
import os
import time
from decimal import Decimal
from enum import Enum
from functools import wraps, partial
from typing import Container, Iterable, Union, Type

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
                if current_time - start_time < datetime.timedelta(seconds=10):
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
                if current_time - start_time < datetime.timedelta(seconds=10):
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


class DecimalAwareEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            # It was a bad choice, don't do like this. `str` is a better option.
            return float(o)
        if isinstance(o, Enum):
            return o.value
        return super().default(o)


def retryable(
        func=None,
        expected_exception: Union[Type[Exception], Container[Type[Exception]]]=(),
        *,
        retry_count: int=1,
        callback=None,
):
    if func is None:
        return partial(
            retryable,
            expected_exception=expected_exception,
            retry_count=retry_count,
            callback=callback
        )

    @wraps(func)
    def wrapper(*args, **kwargs):
        i = 0

        prev_exceptions = []

        while True:
            try:
                object_to_return = func(*args, **kwargs)

                if isinstance(object_to_return, Iterable):
                    try:
                        yield next(object_to_return)
                    except StopIteration:
                        return
                    except GeneratorExit:
                        pass
                else:
                    return object_to_return
            except expected_exception as e:
                prev_exceptions.append(e)
                if i >= retry_count:
                    logger.error(
                        'Got too many exceptions: %s\nRaising the last one.',
                        prev_exceptions
                    )
                    raise e

                logger.info(
                    'Got expected exception %s while running %s. %s attempts left.',
                    type(e).__name__,
                    func.__name__ if hasattr(func, '__name__') else func,
                    retry_count - i
                )

                if callback is not None:
                    callback(*args, **kwargs)

                i += 1
            else:
                yield from object_to_return
                return

    return wrapper

