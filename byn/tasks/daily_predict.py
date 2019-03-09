import datetime
import logging
from decimal import Decimal

from byn.predict_utils import build_and_predict_linear
from byn.tasks.launch import app
from byn.hbase_db import bytes_to_date, get_decimal, date_to_next_bytes, db, key_part


start_prediction_day = datetime.date(2018, 7, 15)
logger = logging.getLogger(__name__)


@app.task
def daily_predict():
    with db.connection() as connection:
        trade_date = connection.table('tarde_date')

        last_record = next(trade_date.scan(
            reverse=True,
            columns=[b'rate:accumulated_error'],
            limit=1,
            filter="SingleColumnValueFilter('rate', 'predicted', >, '', true)",
        ), None)

        start_date = bytes_to_date(last_record[0]) if last_record else start_prediction_day
        accumulated_error = get_decimal(last_record[1], b'rate:accumulated_error') if last_record else 0

        nbrb = connection.table('nbrb')
        key_to_data = nbrb.scan(
            row_start=b'global|' + date_to_next_bytes(start_date),
            columns=[b'rate:byn'],
            filter="SingleColumnValueFilter('rate', 'eur', >, '', true) AND "
                   "SingleColumnValueFilter('rate', 'rub', >, '', true) AND "
                   "SingleColumnValueFilter('rate', 'uah', >, '', true)",
        )

        new_data = []

        for key, data in key_to_data:
            date = key_part(key, 1)
            real_rate = get_decimal(data, b'rate:byn')

            predicted = Decimal(build_and_predict_linear(bytes_to_date(date)))
            prediction_error = predicted/real_rate - 1
            accumulated_error += prediction_error

            data = {
                'date': date,
                b'rate:predicted': str(predicted).encode(),
                b'rate:prediction_error': str(prediction_error).encode(),
                b'rate:accumulated_error': str(accumulated_error).encode(),
            }

            logger.debug(data)
            new_data.append(data)

        for row in new_data:
            key = b'global|' + row.pop('date')
            trade_date.put(key, row)
