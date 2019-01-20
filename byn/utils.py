import asyncio
import datetime
import logging
from functools import wraps


logger = logging.getLogger(__name__)


def always_on_coroutine(coro):
    @wraps(coro)
    async def  wrapper(*args, **kwargs):
        retry_counter = 0

        while True:
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
                    retry_counter = 0

                wait_for_before_restart = 2 ** (retry_counter - 1) if retry_counter < 10 else 8 * 60

                logger.exception(
                    'Unexpected exception while running %s. Restarting in %s seconds...',
                    coro, wait_for_before_restart
                )

                await asyncio.sleep(wait_for_before_restart)

    return wrapper
