import asyncio
import datetime
import json
import logging
import os
import time
from decimal import Decimal
from enum import Enum
from functools import wraps

import aioredis

from byn import constants as const

logger = logging.getLogger(__name__)


def always_on_coroutine(coro):
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

            except:
                current_time = datetime.datetime.now()
                if current_time - start_time < datetime.timedelta(seconds=10):
                    retry_counter += 1
                else:
                    retry_counter = 1

                wait_for_before_restart = 2 ** (retry_counter - 1) if retry_counter < 10 else 8 * 60

                logger.exception(
                    'Unexpected exception while running %s. Restarting in %s seconds...',
                    coro, wait_for_before_restart
                )

                await asyncio.sleep(wait_for_before_restart)

    return wrapper


def always_on_sync(func):
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

            except:
                current_time = datetime.datetime.now()
                if current_time - start_time < datetime.timedelta(seconds=10):
                    retry_counter += 1
                else:
                    retry_counter = 1

                wait_for_before_restart = 2 ** (retry_counter - 1) if retry_counter < 10 else 8 * 60

                logger.exception(
                    'Unexpected exception while running %s. Restarting in %s seconds...',
                    func, wait_for_before_restart
                )

                time.sleep(wait_for_before_restart)

    return wrapper


async def create_redis() -> aioredis.Redis:
    return await aioredis.create_redis(os.environ["REDIS_URL"], db=const.REDIS_CACHE_DB)


class DecimalAwareEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, Enum):
            return o.value
        return super().default(o)
