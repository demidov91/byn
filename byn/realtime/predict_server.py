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
from byn.predict.predictor import Predictor
from byn.predict.utils import build_trust_array
from byn.datatypes import LocalRates, PredictCommand
from byn.hbase_db import insert_prediction, get_bcse_in
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
        todays_bcse_config = TodaysRatesConfigurer(predictor=predictor, bcse_converter=bcse_converter)

        logger.debug('Predictor is created.')

        await wait_for_data_threads()

        if predictor.meta.last_date < today:
            start_dt = datetime.datetime.fromordinal(today.toordinal())
            bcse_data = np.array(
                tuple(get_bcse_in('USD', start_dt=start_dt)),
                dtype=np.dtype(object)
            )

            todays_bcse_config.configure(bcse_pairs=bcse_data, rolling_average=rolling_average)

        logger.debug('Predictor is configured.')

        while True:
            message = await receive_predictor_command(redis)
            command = PredictCommand(message['command'])

            if command == PredictCommand.REBUILD:
                break

            elif command == PredictCommand.NEW_BCSE:
                bcse_data = np.array([
                    (ts, rate) for ts, rate in message['data']['rates']
                ], dtype=np.dtype(object))

                todays_bcse_config.configure(bcse_pairs=bcse_data, rolling_average=rolling_average)

            elif command == PredictCommand.PREDICT:
                message_guid = message['data'].pop('message_guid')
                data = {x: Decimal(message['data'][x]) for x in message['data']}

                local_rates = LocalRates(**data)
                prediction = predictor.predict_current_by_local_for_record(
                    local_rates,
                    rolling_average=rolling_average
                )
                prediction.timestamp = int(datetime.datetime.now().timestamp())

                await send_prediction(redis, prediction, message_guid=message_guid)

                insert_prediction(
                    timestamp=message_guid,
                    external_rates=data,
                    bcse_full=todays_bcse_config.bcse_full,
                    bcse_trusted_global=todays_bcse_config.bcse_trusted_global,
                    prediction=prediction,
                )



class TodaysRatesConfigurer:
    """
    Object to keep track of what was configured as todays active rates in the predictor.
    """

    bcse_trusted_global = None  # type: np.ndarray
    bcse_trusted = None # type: np.ndarray
    bcse_full = None    # type: np.ndarray


    def __init__(
        self,
        *,
        predictor: Predictor,
        bcse_converter: BcseConverter,

    ):
        self.predictor = predictor
        self.bcse_converter = bcse_converter


    def configure(self, *, bcse_pairs: np.ndarray, rolling_average: np.ndarray):
        self.bcse_full = np.array([])
        self.bcse_trusted = np.array([])
        self.bcse_trusted_global = np.array([])

        if len(bcse_pairs) == 0:
            logger.debug('Empty bcse data. Skipping.')
            return

        logger.debug('Bcse data to set: %s', bcse_pairs)
        self.bcse_converter.update(bcse_pairs)

        open_timestamp = bcse_pairs[0][0]
        fake_rate = self.bcse_converter.get_fake_rate(open_timestamp)

        if fake_rate is None:
            self.predictor.ignore_todays_rates()

            fake_rate = self.predictor.predict_by_local(
                self.bcse_converter.get_by_timestamp(open_timestamp),
                rolling_average=rolling_average
            )

            self.bcse_converter.set_fake_rate(open_timestamp, fake_rate)

        logger.debug("Fake rate is %s (real rate is %s)", fake_rate, bcse_pairs[0][1])


        trust = build_trust_array(fake_rate, bcse_pairs[:, 0], bcse_pairs[:, 1])

        if not trust.any():
            logger.debug("There is no bcse rates to trust. Skipping.")
            self.predictor.ignore_todays_rates()
            return

        self.bcse_full = bcse_pairs
        self.bcse_trusted = self.bcse_full[trust]

        X = [self.bcse_converter.get_by_timestamp(x[0]) for x in self.bcse_trusted]
        Y = np.array(self.bcse_trusted[:, 1], dtype='float64')
        self.predictor.set_todays_local_rates(X, Y, rolling_average)

        self.bcse_trusted_global = self.bcse_trusted.copy()
        self.bcse_trusted_global[:,1] = self.predictor.neighbor_y
        logger.debug("Active bcse rates are set.")
