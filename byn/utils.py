import asyncio
import datetime
import json
import logging
import os
from decimal import Decimal
from functools import wraps

import aioredis

from byn import constants as const

logger = logging.getLogger(__name__)


def always_on_coroutine(coro):
    @wraps(coro)
    async def  wrapper(*args, **kwargs):
        retry_counter = 0
        has_finished_without_an_error = False

        while not has_finished_without_an_error:
            if retry_counter > 0:
                logger.info('Trying to restart %s', coro)

            start_time = datetime.datetime.now()

            try:
                await coro(*args, **kwargs)
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

            else:
                has_finished_without_an_error = True

    return wrapper


async def create_redis() -> aioredis.Redis:
    return await aioredis.create_redis(
        f'redis://{os.environ["REDIS_CACHE_HOST"]}',
        db=const.REDIS_CACHE_DB
    )


class DecimalAwareEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)
