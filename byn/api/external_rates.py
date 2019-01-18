"""
profinance.ru (forexpf) long poll ===> redis cache & cassandra
"""
import logging

from aiohttp.client import ClientSession

from byn import constants as const
from byn.forexpf import sse_to_tuple, CURRENCY_CODES


logger = logging.getLogger(__name__)

CURRENCIES_TO_LISTEN = 'EUR', 'RUB', 'UAH', 'DXY'


async def listen_forexpf():
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

        async for line in long_poll_response.content:
            logger.debug(line)

            data = sse_to_tuple(line)

            if data is None:
                continue

            try:
                _, product_id, resolution, data_column, rate_open, high, low, close, volume = data
            except ValueError:
                logger.exception("Couldn't split an event:\n%s", data)
                continue

            logger.debug(
                'product: %s, column: %s, open: %s, close: %s, low: %s, high: %s, volume: %s',
                product_id, data_column, rate_open, close, low, high, volume
            )