import datetime
import json
import logging
import os
import threading
from collections import defaultdict
from dataclasses import asdict
from decimal import Decimal
from functools import lru_cache, partial
from itertools import chain
from typing import Collection, Iterable, Union, Tuple, Iterator, Any, Dict, Sequence, Optional, List

from celery.signals import worker_process_init, worker_process_shutdown
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import WhiteListRoundRobinPolicy

import byn.constants as const
from byn.datatypes import ExternalRateData, BcseData
from byn.predict.predictor import PredictionRecord
from byn.utils import DecimalAwareEncoder


logger = logging.getLogger(__name__)

CASSANDRA_HOSTS = os.environ['CASSANDRA_HOSTS'].split(',')
thread_local = threading.local()


def create_cassandra_session():
    try:
        cluster = Cluster(
            CASSANDRA_HOSTS,
            port=os.environ['CASSANDRA_PORT'],
            load_balancing_policy=WhiteListRoundRobinPolicy(hosts=CASSANDRA_HOSTS)
        )
        db = cluster.connect()
        db.execute(
            "CREATE KEYSPACE IF NOT EXISTS byn WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}")
        db.execute('USE byn')
    except NoHostAvailable:
        logger.exception('Couldnt connect to cassandra')
        return None

    return db

class PerThreadCassandraSession:
    def __getattr__(self, item):
        return getattr(thread_local.cassandra_session, item)

    def __setattr__(self, key, value):
        return setattr(thread_local.cassandra_session, key, value)

    def connect(self):
        thread_local.cassandra_session = create_cassandra_session()

    def disconnect(self):
        self.shutdown()


db = PerThreadCassandraSession() # type: Union[Session, PerThreadCassandraSession]
db.connect()


@worker_process_init.connect
def _init_cassandra_session(**kwargs):
    db.connect()


@worker_process_shutdown.connect
def _shutdown_cassandra_session(**kwargs):
    db.disconnect()



def get_last_nbrb_rates():
    return next(iter(db.execute('select * from nbrb limit 1')), None)

def get_last_nbrb_local_rates():
    return next(iter(db.execute('select * from nbrb_local limit 1')), None)

def get_last_nbrb_global_record():
    return next(iter(db.execute('select * from nbrb_global limit 1')), None)

def get_last_nbrb_global_with_rates():
    return next(iter(db.execute('select * from nbrb_global where dummy=true and byn>0 limit 1 ALLOW FILTERING')), None)

def get_last_external_currency_datetime(currency: str) -> datetime.datetime:
    current_year = datetime.date.today().year

    utc_datetime = max((
        x[0] for x in db.execute(
            'select datetime from external_rate where currency=%s and year in (%s, %s) per partition limit 1',
            (currency, current_year, current_year - 1)
        )
    ), default=datetime.datetime.fromtimestamp(0)).replace(tzinfo=datetime.timezone.utc)

    return datetime.datetime.fromtimestamp(utc_datetime.timestamp())


def get_last_rolling_average_date() -> datetime.date:
    dates = tuple(db.execute('SELECT date FROM rolling_average per partition limit 1'))
    logger.debug('%s rolling average dates retrieved while looking for the last', len(dates))
    return max([x[0] for x in dates], default=None)


def get_nbrb_gt(date):
    return db.execute('select * from nbrb where dummy=true and date>%s', (date or 0, ))

def get_nbrb_local_gt(date):
    return db.execute('select * from nbrb_local where dummy=true and date>%s', (date or 0, ))

def get_nbrb_global_gt(date):
    return db.execute('select * from nbrb_global where dummy=true and date>%s', (date or 0, ))

def get_bcse_in(currency: str, start_dt: datetime.datetime, end_dt: datetime.datetime=None) -> Iterable[Tuple[datetime.datetime, Decimal]]:
    end_dt = end_dt or datetime.datetime(2035, 1, 1)
    start_dt = int(start_dt.timestamp() * 1000)
    end_dt = int(end_dt.timestamp() * 1000)
    raw_output = db.execute(
        'SELECT timestamp_operation, rate FROM bcse '
        'WHERE currency=%s and timestamp_operation>=%s and timestamp_operation<%s '
        'ORDER BY timestamp_operation ASC',
        (currency, start_dt, end_dt)
    )

    return (
        (dt.replace(tzinfo=datetime.timezone.utc), rate)
        for dt, rate in raw_output
    )

def get_rolling_average_by_date(date: datetime.date) -> Iterable[Sequence[Decimal]]:
    return db.execute('SELECT duration, eur, rub, uah FROM rolling_average where date=%s', (date, ))


@lru_cache()
def get_plain_rolling_average_by_date(date: datetime.date) -> Tuple[Decimal]:
    logger.debug(date)
    data = get_rolling_average_by_date(date)

    data = {x[0]: x[1:] for x in data}
    return tuple(chain(*(data[x] for x in const.ROLLING_AVERAGE_DURATIONS)))


def get_external_rate_live(start_dt: datetime.datetime, end_dt: Optional[datetime.datetime]=None) -> Dict[str, Iterable[list]]:
    end_dt = end_dt or datetime.datetime(2100, 1, 1)
    currencies = 'EUR', 'RUB', 'UAH', 'DXY'

    data = {}
    rs = []

    for currency in currencies:
        rs.append(db.execute_async(
            'SELECT currency, timestamp_open, timestamp_received, close '
            'FROM external_rate_live '
            "WHERE currency=%s and timestamp_open>=%s and timestamp_open<%s "
            'ORDER BY timestamp_open ASC, volume ASC',
            (
                currency,
                int(start_dt.timestamp() * 1000),
                int(end_dt.timestamp() * 1000)
            )
        ).add_callback(
            partial(
                _process_external_rate_live_one_currency,
                data=data,
                currency=currency
            )
        ))

    for r in rs:
        r.result()

    return data


def _process_external_rate_live_one_currency(rows, data: dict, currency: str):
    rates = defaultdict(list)

    for row in rows:
        rates[int(row.timestamp_open.replace(tzinfo=datetime.timezone.utc).timestamp())].append([
            int(row.timestamp_received.replace(tzinfo=datetime.timezone.utc).timestamp()),
            Decimal(row.close)
        ])


    close_times = list(rates.keys())[1:]
    close_times.append(None)

    for open_time, close_time in zip(rates, close_times):
        for time_rate_pair in rates[open_time]:
            if time_rate_pair[0] < open_time:
                time_rate_pair[0] = open_time
            elif close_time is not None and time_rate_pair[0] >= close_time:
                time_rate_pair[0] = close_time - 1

    data[currency] = tuple(chain(*rates.values()))


def get_latest_external_rates(
        start_dt: datetime.datetime,
        *,
        at_least_one: bool=False
) -> Dict[str, Iterable[list]]:
    currencies = 'EUR', 'RUB', 'UAH', 'DXY'

    correct_data = db.execute_async(
        'SELECT currency, datetime, open, close '
        'FROM external_rate '
        f"WHERE currency in ('EUR', 'RUB', 'UAH', 'DXY') and year in (%s, %s) and "
        "datetime>=%s",
        (
            start_dt.date().year,
            start_dt.date().year + 1,
            int(start_dt.timestamp() * 1000),
        )
    )

    currency_to_rows = defaultdict(list)

    if at_least_one:
        last_data = get_the_last_external_rates(currencies, end_dt=start_dt)
        for currency, row in last_data.items():
            currency_to_rows[currency].append((
                row.datetime, row.open, row.close
            ))

    for row in correct_data.result():
        currency_to_rows[row[0]].append(row[1:])

    processed_data = {}
    for currency, rates in currency_to_rows.items():
        rates = tuple(
            (x[0].replace(tzinfo=datetime.timezone.utc), *x[1:])
            for x in sorted(rates, key=lambda x: x[0])
        )

        close_times = [
            x[0] - datetime.timedelta(seconds=1)
            for x in rates
        ][1:]
        pairs_to_return = []
        for i in range(len(close_times)):
            pairs_to_return.append(
                (
                    int(rates[i][0].timestamp()),
                    rates[i][1]
                )
            )
            pairs_to_return.append((int(close_times[i].timestamp()), rates[i][2]))

        pairs_to_return.append((
            int(rates[-1][0].timestamp()),
            rates[-1][1]
        ))

        processed_data[currency] = pairs_to_return

    return processed_data


def get_the_last_external_rates(currencies: Iterable[str], end_dt: datetime.datetime):
    futures = []
    data = {}

    def _set_data(rows, *, currency):
        row = max(tuple(rows), key=lambda x: x.datetime)
        data[currency] = row

    for currency in currencies:
        futures.append(db.execute_async(
            "SELECT * FROM external_rate "
            "WHERE currency=%s and year in (%s, %s) and datetime<%s"
            "PER PARTITION LIMIT 1 ALLOW FILTERING",
            (
                currency,
                end_dt.year - 1,
                end_dt.year,
                int(end_dt.timestamp() * 1000)
            )
        ).add_callback(partial(_set_data, currency=currency)))

    for future in futures:
        future.result()

    return data


def get_accumulated_error(date:datetime.date):
    record = next(iter(
        db.execute(
            'SELECT accumulated_error from trade_date '
            'WHERE dummy=true and date<=%s and '
            'accumulated_error<1000000 '    # Not null actually.
            'LIMIT 1 ALLOW FILTERING',
            (date, )
       )
    ), None)
    return record and record.accumulated_error


def _handle_async_exception(exception: BaseException):
    logger.exception('Error while executing async cassandra query: %s', exception)


def _launch_in_parallel(query: str, data: Iterable[Union[Iterable, Dict[str, Any]]], **kwargs):
    return (db.execute_async(query, row, **kwargs) for row in data)


def _execute_in_parallel(query: str, data: Iterable[Union[Iterable, Dict[str, Any]]]):
    rs = []
    for row in data:
        rs.append(db.execute_async(query, row))

    # Call for exception.
    for r in _launch_in_parallel(query, data):
        r.result()


def insert_nbrb(data):
    _execute_in_parallel(
        'INSERT into nbrb '
        '(dummy, date, usd, eur, rub, uah) '
        'VALUES '
        '(true, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
        data
    )


def insert_nbrb_local(data):
    _execute_in_parallel(
        'INSERT into nbrb_local '
        '(dummy, date, usd, eur, rub, uah) '
        'VALUES '
        '(true, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
        data
    )


def add_nbrb_global(data):
    _execute_in_parallel(
        'UPDATE nbrb_global set '
        'byn=%(BYN)s, eur=%(EUR)s, rub=%(RUB)s, uah=%(UAH)s '
        'WHERE dummy=true and date=%(date)s',
        data
    )


def insert_trade_dates(trade_dates: Collection[str]):
    _execute_in_parallel(
        'INSERT into trade_date (dummy, date) VALUES (true, %s) ',
        ((x, ) for x in trade_dates)
    )


def insert_nbrb_rates(rates: Iterable[dict]):
    _execute_in_parallel(
        'INSERT into nbrb '
        '(dummy, date, usd, eur, rub, uah) '
        'VALUES '
        '(true, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
        rates
    )


def insert_dxy_12MSK(data: Iterable[tuple]):
    _execute_in_parallel(
        'INSERT into nbrb_global (dummy, date, dxy) VALUES (true, %s, %s)',
        data
    )



def insert_external_rates(
        currency: str,
        data: Iterator[
            Tuple[
                datetime.datetime,
                Decimal,
                Decimal,
                Decimal,
                Decimal,
                int
            ]
        ]
):
    _execute_in_parallel(
        'INSERT into external_rate (year, currency, datetime, open, close, low, high, volume) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
        [
            (
                row[0].year,
                currency,
                int(row[0].timestamp() * 1000),
                row[1],
                row[2],
                row[3],
                row[4],
                row[5]
            )
            for row in data
        ]
    )


def insert_external_rate_live_async(row: ExternalRateData):
    data = asdict(row)
    data['timestamp_open'] *= 1000
    data['timestamp_received'] = int(data['timestamp_received'] * 1000)

    return db.execute_async(
        'INSERT into external_rate_live (currency, timestamp_open, volume, timestamp_received, close) '
        'VALUES (%(currency)s, %(timestamp_open)s, %(volume)s, %(timestamp_received)s, %(close)s) '
        'USING TTL 15811200', # Half a year.
        data
    ).add_errback(_handle_async_exception)


def insert_bcse_async(data: Iterable[BcseData], **kwargs):
    return _launch_in_parallel(
        'INSERT into bcse (currency, timestamp_operation, timestamp_received, rate) '
        'VALUES (%s, %s, %s, %s)',
        [
            (row.currency, row.ms_timestamp_operation, row.ms_timestamp_received, row.rate)
            for row in data
        ],
        **kwargs
    )


def insert_prediction_async(
        *,
        timestamp: int,
        external_rates: Dict[str, str],
        bcse_full: Sequence[Sequence],
        bcse_trusted_global: Sequence[Sequence],
        prediction: PredictionRecord
):
    if bcse_full is not None:
        bcse_full = _ndarray_to_tuple_of_tuples(bcse_full)

    if bcse_trusted_global is not None:
        bcse_trusted_global = _ndarray_to_tuple_of_tuples(bcse_trusted_global)

    db.execute_async(
        'INSERT into prediction (date, timestamp, external_rates, bcse_full, bcse_trusted_global, prediction) '
        'VALUES (%(date)s, %(timestamp)s, %(external_rates)s, %(bcse_full)s, %(bcse_trusted_global)s, %(prediction)s) '
        'USING TTL 5184000',    # 60 days
        {
            'date': datetime.datetime.fromtimestamp(timestamp // 1000).date(),
            'timestamp': timestamp,
            'external_rates': json.dumps(external_rates, cls=DecimalAwareEncoder),
            'bcse_full': json.dumps(bcse_full, cls=DecimalAwareEncoder),
            'bcse_trusted_global': json.dumps(bcse_trusted_global, cls=DecimalAwareEncoder),
            'prediction': json.dumps(asdict(prediction), cls=DecimalAwareEncoder),
        }
    ).add_errback(_handle_async_exception)


def _ndarray_to_tuple_of_tuples(numpy_array):
    return tuple(tuple(row) for row in numpy_array)


def insert_rolling_average(date: datetime.date, duration: int, data: Sequence[Decimal]):
    db.execute('INSERT into rolling_average (date, duration, eur, rub, uah, dxy) '
               'VALUES (%s, %s, %s, %s, %s, %s)',
               (date, duration, *data))
