import datetime
from decimal import Decimal

from byn.cassandra_db import db
from byn.predict_utils import build_and_predict_linear

start_prediction_day = datetime.date(2018, 7, 14)


def daily_predict():
    last_record = tuple(db.execute(
        'select date from nbrb_global '
        'where '
        'dummy=true and '
        'predicted > 0 '
        'limit 1'
    ))
    start_date = last_record[0].date() if last_record else start_prediction_day

    date_to_real_rate = db.execute(
        'select date, byn from nbrb_global where date>%s and eur>0 and rub>0 and uah>0',
        (start_date, )
    )

    for date, real_rate in date_to_real_rate:
        predicted = Decimal(build_and_predict_linear(date))
        db.execute(
            'INSERT into nbrb_global (dummy, date, predicted, prediction_error) '
            'VALUES (true, %s, %s, %s)',
            (date, predicted, predicted/real_rate - 1)
        )
