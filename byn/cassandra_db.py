import datetime
import logging
import os
import threading
from dataclasses import asdict
from decimal import Decimal
from typing import Collection, Iterable, Union, Tuple, Iterator

from celery.signals import worker_process_init, worker_process_shutdown
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import WhiteListRoundRobinPolicy

from byn.datatypes import ExternalRateData, BcseData


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

    return max((
        x[0] for x in db.execute(
            'select datetime from external_rate where currency=%s and year in (%s, %s) per partition limit 1',
            (currency, current_year, current_year - 1)
        )
    ), default=datetime.datetime.fromtimestamp(0))


def get_nbrb_gt(date):
    return db.execute('select * from nbrb where dummy=true and date>%s', (date or 0, ))

def get_nbrb_local_gt(date):
    return db.execute('select * from nbrb_local where dummy=true and date>%s', (date or 0, ))

def get_nbrb_global_gt(date):
    return db.execute('select * from nbrb_global where dummy=true and date>%s', (date or 0, ))


def insert_nbrb_local(data):
    rs = []
    for row in data:
        rs.append(
            db.execute_async(
                'INSERT into nbrb_local '
                '(dummy, date, usd, eur, rub, uah) '
                'VALUES '
                '(true, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
                row
            )
        )

    # Call for exception.
    for r in rs:
        r.result()



def add_nbrb_global(data):
    rs = []
    for row in data:
        rs.append(
            db.execute_async(
                'UPDATE nbrb_global set '
                'byn=%(BYN)s, eur=%(EUR)s, rub=%(RUB)s, uah=%(UAH)s '
                'WHERE dummy=true and date=%(date)s',
                row
            )
        )

    # Call for exception.
    for r in rs:
        r.result()


def insert_trade_dates(trade_dates: Collection[str]):
    rs = []

    # from celery.contrib import rdb
    # rdb.set_trace()

    for row in trade_dates:
        rs.append(
            db.execute_async('INSERT into trade_date (dummy, date) VALUES (true, %s) ', (row, ))
        )

    # Call for exception.
    for r in rs:
        r.result()


def insert_nbrb_rates(rates: Iterable[dict]):
    rs = []
    for row in rates:
        rs.append(
            db.execute_async(
                'INSERT into nbrb '
                '(dummy, date, usd, eur, rub, uah) '
                'VALUES '
                '(true, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
                row
            )
        )

    # Call for exception.
    for r in rs:
        r.result()


def insert_dxy_12MSK(data: Iterable[Collection]):
    rs = []
    for date, dxy in data:
        db.execute_async('INSERT into nbrb_global (dummy, date, dxy) VALUES (true, %s, %s)', (date, dxy))
    # Call for exception.
    for r in rs:
        r.result()


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
    rs = []
    for row in data:
        rs.append(
            db.execute_async(
                'INSERT into external_rate (year, currency, datetime, open, close, low, high, volume) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (row[0].year, currency, row[0], row[1], row[2], row[3], row[4], row[5])
            )
        )

    # Call for exception.
    for r in rs:
        r.result()


def insert_external_rate_live_async(row: ExternalRateData):
    data = asdict(row)
    data['timestamp_open'] *= 1000
    data['timestamp_received'] = int(data['timestamp_received'] * 1000)

    return db.execute_async(
        'INSERT into external_rate_live (currency, timestamp_open, volume, timestamp_received, close) '
        'VALUES (%(currency)s, %(timestamp_open)s, %(volume)s, %(timestamp_received)s, %(close)s) '
        'USING TTL 15811200', # Half a year.
        data
    )


def insert_bcse_async(data: Iterable[BcseData]):
    rs = []

    for row in data:
        rs.append(db.execute_async(
            'INSERT into bcse (currency, timestamp_operation, timestamp_received, rate) '
            'VALUES (%s, %s, %s, %s)',
            (row.currency, row.epoch)
        ))

    return rs