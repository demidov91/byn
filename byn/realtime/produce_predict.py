"""
Publish current prediction into a queue. Once a second.
"""
import asyncio
import logging
import json
import dataclasses
import datetime
from decimal import Decimal

from byn import constants as const
from byn.datatypes import PredictInput, PredictOutput, LocalRates
from byn.utils import create_redis, always_on_coroutine, DecimalAwareEncoder
from byn.cassandra_db import insert_prediction_async
from byn.predict.predictor import Predictor, PredictionRecord


logger = logging.getLogger(__name__)


active_predictor = None     # type: Predictor


@always_on_coroutine
async def start_predictor():
    global active_predictor

    while True:
        active_predictor = _initialize_nbrb_predictor()
        asyncio.create_task(_predict_scheduler())
        await _listen_to_commands()
        active_predictor.acc


@always_on_coroutine
async def _predict_scheduler():
    redis = await create_redis()

    while True:
        external_rates = (x.decode() for x in await redis.mget(*const.FOREXPF_CURRENCIES_TO_LISTEN))

        input_data = _build_predict_input(
            names=const.FOREXPF_CURRENCIES_TO_LISTEN,
            values=external_rates,
        )

        output_data = predict(input_data)

        insert_prediction_async(input_data, output_data)

        await redis.publish(const.PUBLISH_PREDICT_REDIS_CHANNEL, json.dumps({
            'input': dataclasses.asdict(input_data),
            'output': dataclasses.asdict(output_data),
        }, cls=DecimalAwareEncoder))

        await asyncio.sleep(const.PREDICT_UPDATE_INTERVAL)


def _build_predict_input(*, names, values) -> LocalRates:
    return LocalRates(**dict(zip((x.lower() for x in names), values)))


def predict(data: LocalRates) -> PredictionRecord:
    return active_predictor.predict_current_by_local_for_record(data)


@always_on_coroutine
async def _listen_to_commands():
    redis = create_redis()
    channel, = await redis.subscribe(const.PREDICT_COMMNDS_REDIS_CHANNEL)
    keep_working = True
    while keep_working:
        command = json.loads(await channel.get())
        command_type = PredictCommand(command['type'])
        logger.info('Predict command: %s', command_type)

        if command_type == PredictCommand.REBUILD:
            return

        if command_type == PredictCommand.NEW_BCSE:
            pass















