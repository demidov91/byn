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
from byn.datatypes import PredictInput, PredictOutput
from byn.utils import create_redis, always_on_coroutine, DecimalAwareEncoder
from byn.cassandra_db import insert_prediction_async


logger = logging.getLogger(__name__)


@always_on_coroutine
async def start_predicting():
    redis = await create_redis()

    while True:
        external_rates = (x.decode() for x in await redis.mget(*const.FOREXPF_CURRENCIES_TO_LISTEN))
        usd_byn_data = await redis.get(const.BCSE_USD_REDIS_KEY) or []

        input_data = _build_predict_input(
            names=const.FOREXPF_CURRENCIES_TO_LISTEN,
            values=external_rates,
            usd_byn=usd_byn_data,
        )

        output_data = predict(input_data)

        insert_prediction_async(input_data, output_data)

        await redis.publish(const.PREDICT_REDIS_CHANNEL, json.dumps({
            'input': dataclasses.asdict(input_data),
            'output': dataclasses.asdict(output_data),
        }, cls=DecimalAwareEncoder))

        await asyncio.sleep(const.PREDICT_UPDATE_INTERVAL)


def _build_predict_input(*, names, values, usd_byn) -> PredictInput:
    data = dict(zip((x.lower() for x in names), values))
    data['usd_byn'] = tuple(
        (datetime.datetime.fromtimestamp(timestamp), Decimal(rate))
        for timestamp, rate in usd_byn
    )

    return PredictInput(**data)


def predict(data: PredictInput) -> PredictOutput:
    return PredictOutput(rate=1)



