"""
Coroutine which listens for a command redis queue
    predicting rates and rebuilding the model whenever required by a queue command.
"""
import datetime
import json
import logging
from dataclasses import asdict

import numpy as np

from byn.utils import always_on_coroutine, create_redis
from byn.predict_utils import build_predictor
from byn.predict.predictor import RidgeWeight, Predictor
from byn.predict.utils import build_trust_array
from byn.datatypes import LocalRates, PredictCommand
from byn.cassandra_db import (
    get_plain_rolling_average_by_date,
    get_bcse_in
)
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

        predictor = build_predictor(today, ridge_weight=RidgeWeight.LINEAR, use_rolling=True)
        rolling_average = get_plain_rolling_average_by_date(predictor.meta.last_date)

        logger.debug('Predictor is created.')

        await wait_for_data_threads()

        if predictor.meta.last_date < today:
            start_dt = datetime.datetime.fromordinal(today.toordinal())
            bcse_data = np.array([
                (int(dt.timestamp()), rate)
                for dt, rate in
                get_bcse_in('USD', start_dt=start_dt)
            ])

            set_active_bcse(
                predictor=predictor,
                bcse_converter=bcse_converter,
                bcse_pairs=bcse_data,
                rolling_average=rolling_average,
            )

        while True:
            message = await receive_predictor_command(redis)
            command = PredictCommand(message['command'])

            if command == PredictCommand.REBUILD:
                break

            elif command == PredictCommand.NEW_BCSE:
                set_active_bcse(
                    predictor=predictor,
                    bcse_converter=bcse_converter,
                    bcse_pairs=message['data'],
                    rolling_average=rolling_average,
                )

            elif command == PredictCommand.PREDICT:
                ms_timestamp = message['data'].pop('ms_timestamp')

                local_rates = LocalRates(**command['data'])
                prediction = predictor.predict_current_by_local_for_record(
                    local_rates,
                    rolling_average=rolling_average
                )

                await send_prediction(redis, prediction, ms_timestamp=ms_timestamp)


def set_active_bcse(
        *,
        predictor: Predictor,
        bcse_converter: BcseConverter,
        bcse_pairs,
        rolling_average
):
    if not bcse_pairs:
        logger.debug('Empty bcse data. Skipping.')
        return

    bcse_converter.update(bcse_pairs)

    open_timestamp = bcse_pairs[0][0]
    fake_rate = bcse_converter.get_fake_rate(open_timestamp)

    if fake_rate is None:
        predictor.set_todays_rates(predictor.x_train, predictor.y_train)

        fake_rate = predictor.predict_by_local(
            bcse_converter.get_by_timestamp(open_timestamp),
            rolling_average=rolling_average
        )

        bcse_converter.set_fake_rate(open_timestamp, fake_rate)

    logger.debug("Fake rate is %s (real rate is %s)", fake_rate, bcse_pairs[0][1])

    trust = build_trust_array(fake_rate, bcse_pairs[:, 0], bcse_pairs[:, 1])
    trusted_bcse = bcse_pairs[trust]

    X = [bcse_converter.get_by_timestamp(x[0]) for x in trusted_bcse]
    Y = trusted_bcse[:, 1]

    predictor.set_todays_local_rates(X, Y, rolling_average)

    logger.debug("Active bcse rates are set.")