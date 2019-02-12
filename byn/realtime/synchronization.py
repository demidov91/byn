import asyncio
import json
import logging
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
        if not all(data.values()):
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
        data: Optional[Union[dict, list]]=None
):
    await redis.rpush(PREDICTOR_COMMAND_QUEUE, json.dumps({
        'command': command.value,
        'data': data,
    }, cls=DecimalAwareEncoder))


async def receive_predictor_command(redis: Redis) -> dict:
    return json.loads((await redis.blpop(PREDICTOR_COMMAND_QUEUE))[1], parse_float=Decimal)


async def send_prediction(redis: Redis, record: PredictionRecord, *, ms_timestamp):
    data = asdict(record)
    data['ms_timestamp'] = ms_timestamp
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


async def predict_with_timeout(redis: Redis, external_rates: LocalRates, *, timeout: int=500) -> Optional[PredictionRecord]:
    start_time = int(time.time() * 1000)

    input_data = asdict(external_rates)
    input_data['ms_timestamp'] = start_time
    await send_predictor_command(redis, PredictCommand.PREDICT, input_data)

    finish_time = start_time + timeout

    while True:
        prediction_data = await receive_next_prediction(redis, timeout=1)

        if (prediction_data is None) or (finish_time - time.time() * 1000 < 1):
            logger.info('Got no prediction for %s.', input_data['ms_timestamp'])
            return None

        if prediction_data.pop('ms_timestamp') != input_data['ms_timestamp']:
            logger.debug('Ignore old prediction.')

        else:
            prediction_data['ridge_info'] = RidgePredictionRecord(**prediction_data['ridge_info'])
            return PredictionRecord(**prediction_data)
