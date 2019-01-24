import datetime
from byn.predict.predictor import Predictor
from byn.predict.processor import GlobalToNormlizedDataProcessor
from byn.cassandra_db import db


def build_predictor(date: datetime.date) -> Predictor:
    x = []
    y = []
    for row in  db.execute(
        'SELECT byn, eur, rub, uah FROM nbrb_global WHERE dummy=True and date<%s',
        (date, )
    ):
        x.append(row[1:])
        y.append(row[0])

    pre_processor = GlobalToNormlizedDataProcessor()
    pre_processor.fit(x, y)

    predictor = Predictor(pre_processor=pre_processor)
    predictor.rebuild(x, y)
    predictor._ridge_predict_with_one_model()



def build_and_predict_linear(date: datetime.date) -> float:
    predictor = build_predictor(date)
    rates = get_rates_for_date(date)
    rolling_average = get_rolling_average_for_date(date)
    return predictor.predict_by_local(rates, rolling_average=rolling_average)
