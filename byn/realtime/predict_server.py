"""
Coroutine which listens for a command redis queue
    predicting rates and rebuilding the model whenever required by a queue command.
"""
import datetime
import logging
from decimal import Decimal

import numpy as np

from byn.utils import always_on_coroutine, create_redis
from byn.predict_utils import build_predictor
from byn.predict.predictor import RidgeWeight, Predictor
from byn.predict.utils import build_trust_array
from byn.datatypes import LocalRates, PredictCommand
from byn.cassandra_db import get_bcse_in
from byn.predict.utils import ignore_containing_nan
from byn.realtime.synchronization import (
    wait_for_data_threads,
    receive_predictor_command,
    send_prediction,
)
from byn.realtime.bcse_converter import BcseConverter


logger = logging.getLogger(__name__)


@always_on_coroutine
async def run():
    redis = await create_redis()

    bcse_converter = BcseConverter()

    while True:
        today = datetime.date.today()

        logger.debug('Creating predictor...')

        predictor = build_predictor(today, use_rolling=True)
        rolling_average = predictor.meta.last_rolling_average

        logger.debug('Predictor is created.')

        await wait_for_data_threads()

        if predictor.meta.last_date < today:
            start_dt = datetime.datetime.fromordinal(today.toordinal())
            bcse_data = np.array([
                (int(dt.timestamp()), Decimal(rate))
                for dt, rate in
                get_bcse_in('USD', start_dt=start_dt)
            ], dtype=np.dtype(object))

            set_active_bcse(
                predictor=predictor,
                bcse_converter=bcse_converter,
                bcse_pairs=bcse_data,
                rolling_average=rolling_average,
            )

        logger.debug('Predictor is configured.')

        while True:
            message = await receive_predictor_command(redis)
            command = PredictCommand(message['command'])

            if command == PredictCommand.REBUILD:
                break

            elif command == PredictCommand.NEW_BCSE:
                bcse_data = np.array([
                    (ts // 1000, rate) for ts, rate in message['data']
                ], dtype=np.dtype(object))

                set_active_bcse(
                    predictor=predictor,
                    bcse_converter=bcse_converter,
                    bcse_pairs=bcse_data,
                    rolling_average=rolling_average,
                )

            elif command == PredictCommand.PREDICT:
                message_guid = message['data'].pop('message_guid')
                data = {x: Decimal(message['data'][x]) for x in message['data']}

                local_rates = LocalRates(**data)
                prediction = predictor.predict_current_by_local_for_record(
                    local_rates,
                    rolling_average=rolling_average
                )

                await send_prediction(redis, prediction, message_guid=message_guid)


def set_active_bcse(
        *,
        predictor: Predictor,
        bcse_converter: BcseConverter,
        bcse_pairs,
        rolling_average
):
    if len(bcse_pairs) == 0:
        logger.debug('Empty bcse data. Skipping.')
        return

    logger.debug('Bcse data to set: %s', bcse_pairs)
    bcse_converter.update(bcse_pairs)

    open_timestamp = bcse_pairs[0][0]
    fake_rate = bcse_converter.get_fake_rate(open_timestamp)

    if fake_rate is None:
        neighbor_x, neighbor_y = ignore_containing_nan(predictor.x_train, predictor.y_train)
        predictor.set_todays_rates(neighbor_x, neighbor_y)

        fake_rate = predictor.predict_by_local(
            bcse_converter.get_by_timestamp(open_timestamp),
            rolling_average=rolling_average
        )

        bcse_converter.set_fake_rate(open_timestamp, fake_rate)

    logger.debug("Fake rate is %s (real rate is %s)", fake_rate, bcse_pairs[0][1])

    trust = build_trust_array(fake_rate, bcse_pairs[:, 0], bcse_pairs[:, 1])
    trusted_bcse = bcse_pairs[trust]

    X = [bcse_converter.get_by_timestamp(x[0]) for x in trusted_bcse]
    Y = np.array(trusted_bcse[:, 1], dtype='float64')

    predictor.set_todays_local_rates(X, Y, rolling_average)

    logger.debug("Active bcse rates are set.")
