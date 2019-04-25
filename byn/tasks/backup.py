import asyncio
import csv
import datetime
import gzip
import os
import shutil
from typing import Tuple

from celery import group

from byn.tasks.launch import app
from byn.postgres_db import metadata, connection
# To activate logging:
import byn.logging


import logging
logger = logging.getLogger(__name__)


CASSANDRA_BACKUP_TABLES = (
    (
        'bcse',
        ('currency', 'timestamp_operation', 'timestamp_received', 'rate')
    ),
    (
        'nbrb',
        ('dummy', 'date', 'usd', 'eur', 'rub', 'uah')
    ),
    (
        'nbrb_global',
        ('dummy', 'date', 'dxy', )
    ),
    (
        'prediction',
        ('date', 'timestamp', 'external_rates', 'bcse_full', 'bcse_trusted_global', 'prediction')
    ),
    (
        'trade_date',
        ('dummy', 'date', 'predicted', 'prediction_error', 'accumulated_error'),
    )
)


@app.task
def backup_async():
    group([
        _create_cassandra_table_backup.si(*args) | _send_table_backup_to_s3.si(args[0])
        for args in CASSANDRA_BACKUP_TABLES
    ])()


@app.task
def _create_cassandra_table_backup(table: str, columns: Tuple[str]):
    from byn.cassandra_db import db

    data = db.execute(f'SELECT {", ".join(columns)} from {table}', timeout=120)
    with gzip.open(f'/tmp/{table}.csv.gz', mode='wt') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(columns)
        writer.writerows([tuple(x) for x in data])


@app.task
def _send_table_backup_to_s3(folder: str, table: str):
    import boto3

    s3 = boto3.client('s3')
    s3.upload_file(
        os.path.join(folder, table + '.csv.gz'),
        os.environ['BACKUP_BUCKET'],
        f'{table}_{datetime.date.today():%Y-%m-%d}.csv.gz'
    )



HBASE_BACKUP_TABLES = (
    (
        'external_rate',
        ('open', 'close', 'low', 'high', 'volume')
    ),
    (
        'external_rate_live',
        ('timestamp_received', 'close')
    ),
    (
        'bcse',
        ('timestamp_received', 'rate')
    ),
    (
        'nbrb',
        ('usd', 'eur', 'rub', 'uah', 'byn', 'dxy')
    ),
    (
        'prediction',
        ('external_rates', 'bcse_full', 'bcse_trusted_global', 'prediction')
    ),
    (
        'trade_date',
        ('predicted', 'prediction_error', 'accumulated_error'),
    )
)


@app.task
def hbase_backup_async():
    group([
        _create_hbase_table_backup.si(*args) | _send_table_backup_to_s3.si(args[0])
        for args in HBASE_BACKUP_TABLES
    ])()


@app.task
def _create_hbase_table_backup(table_name: str, columns: Tuple[str]):
    from byn.hbase_db import table

    columns = ['rate:' + x for x in columns]

    with gzip.open(f'/tmp/{table_name}.csv.gz', mode='wt') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(tuple(('key', *columns)))

        with table(table_name) as the_table:
            data = (
                tuple(
                    (key.decode(),
                     *(value.get(x.encode(), b'').decode() for x in columns))
                )
                for key, value in the_table.scan()
            )
            writer.writerows(data)


PSQL_FOLDER = '/tmp/dump/'


async def dump_table(table_name: str):
    os.makedirs(PSQL_FOLDER, exist_ok=True)
    os.chmod(PSQL_FOLDER, 0o777)
    path = os.path.join(PSQL_FOLDER, f'{table_name}.csv')

    logger.info('Start dumping %s', table_name)

    async with connection() as conn:
        await conn.execute(f"COPY {table_name} TO %s DELIMITER ';' CSV HEADER", path)

    logger.info('Table %s is copied.', table_name)

    with open(path, mode='rb') as orig, gzip.open(path + '.gz', mode='wb') as dest:
        shutil.copyfileobj(orig, dest)

    os.remove(path)

@app.task
def postgresql_backup():
    async def _implemenation():
        await asyncio.gather(*[dump_table(x) for x in metadata.tables.keys()])
        for table_name in metadata.tables.keys():
            _send_table_backup_to_s3.delay(PSQL_FOLDER, table_name)

    asyncio.run(_implemenation())
