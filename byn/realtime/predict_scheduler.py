"""

"""
import asyncio
import dataclasses
import json
import logging

from byn import constants as const
from byn.cassandra_db import (
    insert_prediction_async,
)
from byn.datatypes import LocalRates
from byn.utils import create_redis, always_on_coroutine, DecimalAwareEncoder
from byn.realtime.synchronization import predict_with_timeout

logger = logging.getLogger(__name__)


@always_on_coroutine
async def predict_scheduler():
    redis = await create_redis()

    while True:
        external_rates = (x.decode() for x in await redis.mget(*const.FOREXPF_CURRENCIES_TO_LISTEN))

        input_data = _build_predict_input(
            names=const.FOREXPF_CURRENCIES_TO_LISTEN,
            values=external_rates,
        )

        output_data = await predict_with_timeout(redis, input_data, timeout=0.5)
        if output_data is not None:
            
            await redis.publish(const.PUBLISH_PREDICT_REDIS_CHANNEL, json.dumps({
                'external': dataclasses.asdict(input_data),
                'predicted': dataclasses.asdict(output_data.to_local()),
            }, cls=DecimalAwareEncoder))

        await asyncio.sleep(const.PREDICT_UPDATE_INTERVAL)


def _build_predict_input(*, names, values) -> LocalRates:
    return LocalRates(**dict(zip((x.lower() for x in names), values)))
