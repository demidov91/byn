"""
register clients' websockets
"""
import asyncio
import json
import logging
from decimal import Decimal

import byn.constants as const
from byn.utils import create_redis, always_on_coroutine


logger = logging.getLogger(__name__)


async def listen_api():
    # Here should go declaration of a websocket handler.
    asyncio.create_task(_listen_subscribe_for_predictions())


@always_on_coroutine
async def _listen_subscribe_for_predictions():
    redis = await create_redis()
    channel,  = await redis.subscribe(const.PUBLISH_PREDICT_REDIS_CHANNEL)

    while True:
        raw_message = await channel.get()
        message = json.loads(raw_message, parse_float=Decimal)
        logger.debug(message)

