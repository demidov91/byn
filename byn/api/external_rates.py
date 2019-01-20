"""
profinance.ru (forexpf) long poll ===> redis cache & cassandra
"""
import asyncio
import logging
import datetime
import os
from asyncio.queues import Queue

import aioredis
from aiohttp.client import ClientSession

from byn import constants as const
from byn.cassandra_db import insert_external_rate_live_async
from byn.datatypes import ExternalRateData
from byn.forexpf import sse_to_tuple, CURRENCY_CODES
from byn.utils import always_on_coroutine


logger = logging.getLogger(__name__)
CURRENCIES_TO_LISTEN = 'EUR', 'RUB', 'UAH', 'DXY'
forexpf_code_to_currency = {y: x for x, y in CURRENCY_CODES.items()}


@always_on_coroutine
async def listen_forexpf():
    current_dt = datetime.datetime.now()

    if not _forexpf_works(current_dt):
        wait_for = _get_time_to_monday(current_dt)
        logger.info('Gonna wait for %s seconds for forexpf to start.', wait_for)
        await asyncio.sleep(wait_for)

    async with ClientSession() as client:
        long_poll_response = await client.get(const.FOREXPF_LONG_POLL_SSE)

        session_id = None

        async for line in long_poll_response.content:
            logger.debug('Raw connection data: %s', line)
            data = sse_to_tuple(line)

            if data is None:
                continue

            if len(data) == 2:
                session_id = data[1]

            break

        if session_id is None:
            logger.error('Failed to get session id.')
            return

        logger.debug('sid: %s', session_id)

        async with ClientSession() as subscribe_client:
            for currency in CURRENCIES_TO_LISTEN:
                response = await subscribe_client.get(
                    'https://charts.profinance.ru/html/tw/subscribe?'
                    f'sid={session_id}&symbol={CURRENCY_CODES[currency]}&resolution=1&subscribe=true'
                )

                if response.status == 200:
                    logger.debug('Subscribed to %s', currency)
                else:
                    logger.error("Couldn't subscribe to %s", currency)

        queue = Queue()
        for _ in range(const.FOREXPF_WORKERS_COUNT):
            asyncio.create_task(_worker(queue))

        await _producer(long_poll_response, queue)


@always_on_coroutine
async def _producer(long_poll_response, queue: Queue):
    async for line in long_poll_response.content:
        logger.debug(line)

        data = sse_to_tuple(line)

        if data is None:
            continue

        try:
            _, product_id, resolution, timestamp_open, rate_open, high, low, close, volume = data
        except ValueError:
            logger.exception("Couldn't split an event:\n%s", data)
            continue

        try:
            await queue.put(ExternalRateData(
                currency=forexpf_code_to_currency[int(product_id)],
                timestamp_open=int(timestamp_open),
                rate_open=rate_open,
                close=close,
                low=low,
                high=high,
                volume=int(volume),
                timestamp_received=datetime.datetime.now().timestamp(),
            ))
        except ValueError:
            logger.exception("Error while sending external rate data into queue.")
            continue

        _inspect_queue(queue)


@always_on_coroutine
async def _worker(queue: Queue):
    keep_running = True
    redis_client = await aioredis.create_redis(
        f'redis://{os.environ["REDIS_CACHE_HOST"]}',
        db=const.REDIS_CACHE_DB
    )

    while keep_running:
        data = await queue.get()    # type: ExternalRateData
        logger.debug(data)

        # Send to cassandra.
        async_result = None
        try:
            async_result = insert_external_rate_live_async(data)
        except asyncio.CancelledError:
            keep_running = False

        except:
            logger.exception("External rate record wasn't sent into cassandra.")

        # Save in redis.
        try:
            await redis_client.mset(
                data.currency, data.close,
                f'{data.currency}_timestamp', int(data.timestamp_received * 1000)
            )
        except asyncio.CancelledError:
            keep_running = False

        except:
            logger.exception("External rate record wasn't saved into redis cache.")

        # Check cassandra result.
        try:
            if async_result is not None:
                async_result.result()
        except asyncio.CancelledError:
            keep_running = False

        except:
            logger.exception("External rate record wasn't saved in cassandra.")


def _forexpf_works(current_dt: datetime.datetime) -> bool:
    return current_dt.isoweekday() not in (6, 7)


def _get_time_to_monday(current_dt: datetime.datetime) -> float:
    next_monday = current_dt.date() + datetime.timedelta(days=8 - current_dt.isoweekday())
    return (datetime.datetime.fromordinal(next_monday.toordinal()) - current_dt).total_seconds()


def _inspect_queue(queue: Queue):
    """
    Log queue size.
    """
    queue_size = queue.qsize()
    if queue_size < 2:
        return

    if queue_size > 15:
        logging_level = logging.ERROR
    elif queue_size > 10:
        logging_level = logging.WARNING
    elif queue_size > 5:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG

    logger.log(logging_level, 'External rates queue size: %s', queue_size)
