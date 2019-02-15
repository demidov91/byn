"""
register clients' websockets
"""
import asyncio
import json
import logging
from decimal import Decimal
from functools import partial

import aiohttp
from aiohttp import web
from aiohttp import WSCloseCode

import byn.constants as const
from byn.utils import create_redis, always_on_coroutine, DecimalAwareEncoder


logger = logging.getLogger(__name__)


async def listen_api():
    app = web.Application()

    app['websockets'] = []
    app.add_routes([web.get('/predict.ws', websocket_handler)])

    app.on_shutdown.append(close_ws)
    asyncio.create_task(_subscribe_for_predictions(app))

    await web._run_app(app, port=5000)


async def close_ws(app):
    for ws in app['websockets']:
        await ws.close(code=WSCloseCode.GOING_AWAY)


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    request.app['websockets'].append(ws)
    logger.debug('New client. %s client(s) are connected', len(request.app['websockets']))

    # Actually, do nothing and wait for disconnection.
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                logger.info('ws is closing with an expect exception: %s', ws.exception())
    finally:
        request.app['websockets'].remove(ws)
        logger.debug('Client disconnected. %s client(s) connected.', len(request.app['websockets']))

    return ws


@always_on_coroutine
async def _subscribe_for_predictions(app):
    redis = await create_redis()
    channel,  = await redis.subscribe(const.PUBLISH_PREDICT_REDIS_CHANNEL)

    while True:
        raw_message = await channel.get()
        message = json.loads(raw_message, parse_float=Decimal)
        logger.debug(message)

        message['type'] = 'prediction'

        for ws in app['websockets']:
            asyncio.create_task(
                ws.send_json(message, dumps=partial(json.dumps, cls=DecimalAwareEncoder))
            )

