import asyncio
import json
import logging
import time
from typing import Optional
from dataclasses import asdict

from aioredis import Redis

from byn.predict.predictor import PredictionRecord
from byn.utils import create_redis
from byn.datatypes import PredictCommand, LocalRates


logger = logging.getLogger(__name__)

WAIT_KEY = 'DATA_THREADS_ARE_READY'
EXTERNAL_HISTORY = 'EH'
EXTERNAL_LIVE = 'EL'
BCSE = 'BCSE'

PREDICTOR_COMMAND_QUEUE = 'PREDICTOR_COMMAND'
PREDICTION_READY_QUEUE = 'PREDICTION_READY'

async def start():
    (await create_redis()).hmset_dict(WAIT_KEY, {
        EXTERNAL_HISTORY: 0,
        EXTERNAL_LIVE: 0,
        BCSE: 0,
    })


async def wait_for_data_threads():
    redis = await create_redis()

    while True:
        data = await redis.hgetall(WAIT_KEY)
        if any(data.values()):
            logger.info('Data threads status: %s', data)
            await asyncio.sleep(1)
        else:
            break


async def mark_as_ready(thread_name: str):
    redis = await create_redis()
    await redis.hset(WAIT_KEY, thread_name, 1)
    logger.debug('%s is marked as ready.', thread_name)


async def send_predictor_command(redis: Redis, command: PredictCommand, data: Optional[dict]=None):
    await redis.rpush(PREDICTOR_COMMAND_QUEUE, json.dumps({
        'command': command.value(),
        'data': data,
    }))


async def receive_predictor_command(redis: Redis) -> dict:
    return json.loads(await redis.blpop(PREDICTOR_COMMAND_QUEUE))


async def send_prediction(redis: Redis, record: PredictionRecord, *, ms_timestamp):
    data = asdict(record)
    data['ms_timestamp'] = ms_timestamp

    await redis.rpush(PREDICTION_READY_QUEUE, json.dumps(data))


async def receive_next_prediction(redis: Redis, *, timeout: float) -> dict:
    return await json.loads(redis.blpop(PREDICTION_READY_QUEUE, timeout=timeout))


async def predict_with_timeout(redis: Redis, external_rates: LocalRates, *, timeout: float=0.5) -> Optional[PredictionRecord]:
    start_time = int(time.time() * 1000)

    input_data = asdict(external_rates)
    input_data['ms_timestamp'] = start_time
    await send_predictor_command(redis, PredictCommand.PREDICT, input_data)

    finish_time = start_time / 1000 + timeout

    while True:
        time_left = time.time() < finish_time
        if time_left < 1:
            return None

        prediction_data = await receive_next_prediction(redis, timeout=time_left)
        if prediction_data.pop('ms_timestamp') != input_data['ms_timestamp']:
            logger.debug('Ignore old prediction.')

        else:
            break

    return PredictionRecord(**prediction_data)
