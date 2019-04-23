import asyncio
import datetime
import logging
from decimal import Decimal

from byn.predict_utils import build_and_predict_linear
from byn.tasks.launch import app
from byn.postgres_db import (
    NbrbKind,
    get_last_predicted_trade_date,
    get_valid_nbrb_gt,
    insert_trade_dates_prediction_data,
)


start_prediction_day = datetime.date(2018, 7, 15)
logger = logging.getLogger(__name__)


@app.task(
    autoretry_for=(Exception, ),
    retry_backoff=True,
)
def daily_predict():
    async def _implementation():
        last_record = await get_last_predicted_trade_date()
        start_date = last_record.date if last_record else start_prediction_day
        accumulated_error = last_record.accumulated_error if last_record else 0
        nbrb_data = await get_valid_nbrb_gt(start_date, NbrbKind.GLOBAL)

        new_data = []

        async for nbrb_row in nbrb_data:
            predicted = Decimal(await build_and_predict_linear(nbrb_row.date))
            prediction_error = predicted / nbrb_row.byn - 1
            accumulated_error += prediction_error

            data = {
                'date': nbrb_row.date,
                'predicted': predicted,
                'prediction_error': prediction_error,
                'accumulated_error': accumulated_error,
            }

            logger.debug(data)
            new_data.append(data)

        await insert_trade_dates_prediction_data(new_data)

    asyncio.run(_implementation())

