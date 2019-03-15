"""

"""
import asyncio
import dataclasses
import datetime
import json
import logging

from byn import constants as const
from byn.hbase_db import (
    get_the_last_external_rates,
)
from byn.datatypes import LocalRates
from byn.utils import create_redis, always_on_coroutine, DecimalAwareEncoder
from byn.realtime.synchronization import (
    predict_with_timeout,
    wait_for_any_data_thread,
    EXTERNAL_HISTORY,
    EXTERNAL_LIVE,
)

logger = logging.getLogger(__name__)


@always_on_coroutine
async def predict_scheduler():
    await wait_for_any_data_thread([EXTERNAL_HISTORY, EXTERNAL_LIVE])

    raw_input_data = get_the_last_external_rates(
        const.FOREXPF_CURRENCIES_TO_LISTEN,
        datetime.datetime.now()
    )
    raw_input_data = {
        k.lower(): str(v['rate_close'])
        for k, v in raw_input_data.items()
    }

    redis = await create_redis()
    logger.debug('Prediction scheduler has started.')

    while True:
        external_rates = (
            x.decode() if x is not None else None
            for x in await redis.mget(*const.FOREXPF_CURRENCIES_TO_LISTEN)
        )

        raw_input_data.update(_build_predict_input_data(
            names=const.FOREXPF_CURRENCIES_TO_LISTEN,
            values=external_rates,
        ))

        input_data = LocalRates(**raw_input_data)

        output_data = await predict_with_timeout(redis, input_data, timeout=0.5)
        if output_data is not None:
            
            await redis.publish(const.PUBLISH_PREDICT_REDIS_CHANNEL, json.dumps({
                'external': dataclasses.asdict(input_data),
                'predicted': dataclasses.asdict(output_data.to_local()),
            }, cls=DecimalAwareEncoder))

        await asyncio.sleep(const.PREDICT_UPDATE_INTERVAL)


def _build_predict_input_data(*, names, values) -> dict:
    return {k: v for k, v in zip((x.lower() for x in names), values) if v is not None}
