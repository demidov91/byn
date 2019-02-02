import datetime
import logging
from decimal import Decimal

from byn.cassandra_db import db
from byn.predict_utils import build_and_predict_linear
from byn.tasks.launch import app


start_prediction_day = datetime.date(2018, 7, 15)
logger = logging.getLogger(__name__)


@app.task
def daily_predict():
    last_record = next(iter(db.execute(
        'select date, accumulated_error from trade_date '
        'where '
        'dummy=true and '
        'predicted > 0 '
        'limit 1 ALLOW FILTERING'
    )), None)
    start_date = last_record[0].date() if last_record else start_prediction_day
    accumulated_error = last_record[1] if last_record else 0

    date_to_real_rate = db.execute(
        'SELECT date, byn from nbrb_global '
        'WHERE dummy=true and date>%s and eur>0 and rub>0 and uah>0 '
        'ORDER BY date ASC '
        'ALLOW FILTERING',
        (start_date, )
    )

    new_data = []

    for date, real_rate in date_to_real_rate:
        predicted = Decimal(build_and_predict_linear(date.date()))
        prediction_error = predicted/real_rate - 1
        accumulated_error += prediction_error

        data = {
            'date': date,
            'predicted': predicted,
            'prediction_error': prediction_error,
            'accumulated_error': accumulated_error,
        }

        logger.debug(data)
        new_data.append(data)

    for row in new_data:
        db.execute(
            'UPDATE trade_date set predicted=%(predicted)s, prediction_error=%(prediction_error)s, accumulated_error=%(accumulated_error)s'
            'WHERE dummy=true and date=%(date)s',
            row
        )
