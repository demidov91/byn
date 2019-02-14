import asyncio
import json
import logging
import math
import time
from typing import Optional, Union
from dataclasses import asdict
from decimal import Decimal

from aioredis import Redis

from byn.predict.predictor import PredictionRecord, RidgePredictionRecord
from byn.utils import create_redis, DecimalAwareEncoder
from byn.datatypes import PredictCommand, LocalRates


logger = logging.getLogger(__name__)

WAIT_KEY = 'DATA_THREADS_ARE_READY'
EXTERNAL_HISTORY = 'EH'
EXTERNAL_LIVE = 'EL'
BCSE = 'BCSE'

PREDICTOR_COMMAND_QUEUE = 'PREDICTOR_COMMAND'
PREDICTION_READY_QUEUE = 'PREDICTION_READY'

async def start():
    await (await create_redis()).hmset_dict(WAIT_KEY, {
        EXTERNAL_HISTORY: 0,
        EXTERNAL_LIVE: 0,
        BCSE: 0,
    })


async def wait_for_data_threads():
    redis = await create_redis()

    while True:
        data = await redis.hgetall(WAIT_KEY)
        if not all(x == b'1' for x in data.values()):
            logger.info('Data threads status: %s', data)
            await asyncio.sleep(1)
        else:
            break


async def mark_as_ready(thread_name: str):
    redis = await create_redis()
    await redis.hset(WAIT_KEY, thread_name, 1)
    logger.debug('%s is marked as ready.', thread_name)


async def send_predictor_command(
        redis: Redis,
        command: PredictCommand,
        data: Optional[dict]=None
):
    await redis.rpush(PREDICTOR_COMMAND_QUEUE, json.dumps({
        'command': command.value,
        'data': data,
    }, cls=DecimalAwareEncoder))


async def receive_predictor_command(redis: Redis) -> dict:
    message = None

    while message is None:
        message = json.loads((await redis.blpop(PREDICTOR_COMMAND_QUEUE))[1], parse_float=Decimal)
        expires = message.get('data') and message['data'].pop('expires', None)
        if (
            expires is not None and
            time.time() * 1000 >= expires
        ):
            logger.info('Ignore expired command %s', message.get('command'))
            message = None

    return message



async def send_prediction(redis: Redis, record: PredictionRecord, *, message_guid):
    data = asdict(record)
    data['message_guid'] = message_guid
    await redis.rpush(PREDICTION_READY_QUEUE, json.dumps(data, cls=DecimalAwareEncoder))


async def receive_next_prediction(redis: Redis, *, timeout: int) -> Optional[dict]:
    """
    :param redis: connection object.
    :param timeout: timeout in seconds.
    :return: full prediction data including *ms_timestamp* field.
    """
    data = await redis.blpop(PREDICTION_READY_QUEUE, timeout=timeout)
    if data is None:
        return None

    return json.loads(data[1])


async def predict_with_timeout(redis: Redis, external_rates: LocalRates, *, timeout: float=0.5) -> Optional[PredictionRecord]:
    start_time = time.time()
    finish_time = start_time + timeout

    input_data = asdict(external_rates)
    input_data['message_guid'] = int(start_time * 1000)
    input_data['expires'] = int(finish_time * 1000)
    await send_predictor_command(redis, PredictCommand.PREDICT, input_data)

    while True:
        remaining_seconds = finish_time - time.time()

        if remaining_seconds < 0.001:
            logger.info('Got no prediction for %s.', input_data['message_guid'])
            return None

        prediction_data = await receive_next_prediction(
            redis,
            timeout=int(math.ceil(remaining_seconds))
        )

        if prediction_data is None:
            continue

        if prediction_data.pop('message_guid') != input_data['message_guid']:
            logger.debug('Ignore old prediction.')

        else:
            prediction_data['ridge_info'] = RidgePredictionRecord(**prediction_data['ridge_info'])
            return PredictionRecord(**prediction_data)
