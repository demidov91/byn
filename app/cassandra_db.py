import logging
import os
from typing import Collection, Iterable

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import BatchStatement


logger = logging.getLogger(__name__)


try:
    cluster = Cluster(['cassandra_1', 'cassandra_2'], port=os.environ['CASSANDRA_PORT'])
    db = cluster.connect()
    db.execute("CREATE KEYSPACE IF NOT EXISTS byn WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    db.execute('USE byn')
except NoHostAvailable:
    logger.exception('Couldnt connect to cassandra')
    cluster = db = None


def get_last_rates():
    return next(db.execute('select * from nbrb order by date desc limit 1'), None)


def insert_trade_dates(trade_dates: Collection[str]):
    batch = BatchStatement()
    insert = db.prepare('INSERT into trade_date (date) VALUES (%s) IF NOT EXISTS')
    for d in trade_dates:
        batch.add(insert, (d, ))
    db.execute(batch)


def insert_nbrb_rates(rates: Iterable[dict]):
    rs = []
    for row in rates:
        row['year'], row['month'], row['day'] = (int(x) for x in row['date'].split('-'))
        rs.append(
            db.execute_async(
                'INSERT into nbrb '
                '(year, month, day, date, usd, eur, rub, uah) '
                'VALUES '
                '(%(year)s, %(month)s, %(day)s, %(date)s, %(USD)s, %(EUR)s, %(RUB)s, %(UAH)s) ',
                row
            )
        )

    # Call for exception.
    for r in rs:
        r.result()
