import logging
import os
from decimal import Decimal
from typing import Collection, Iterable

from cassandra.cluster import Cluster, NoHostAvailable


logger = logging.getLogger(__name__)


try:
    cluster = Cluster(['cassandra_1', 'cassandra_2'], port=os.environ['CASSANDRA_PORT'])
    db = cluster.connect()
    db.execute("CREATE KEYSPACE IF NOT EXISTS byn WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    db.execute('USE byn')
except NoHostAvailable:
    logger.exception('Couldnt connect to cassandra')
    cluster = db = None


def get_last_nbrb_rates():
    return next(iter(db.execute('select * from nbrb limit 1')), None)

def get_last_nbrb_local_rates():
    return next(iter(db.execute('select * from nbrb_local limit 1')), None)

def get_last_nbrb_global_record():
    return next(iter(db.execute('select * from nbrb_global limit 1')), None)

def get_last_nbrb_global_with_rates():
    return next(iter(db.execute('select * from nbrb_global where dummy=true and byn>0 limit 1 ALLOW FILTERING')), None)

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
