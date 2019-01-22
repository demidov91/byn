import csv
import datetime
import gzip
from typing import Tuple

import boto3
from celery import group

from byn import constants as const
from byn.cassandra_db import db
from byn.tasks.launch import app



CASSANDRA_BACKUP_TABLES = (
    (
        'external_rate',
        ('currency', 'year', 'datetime', 'open', 'close', 'low', 'high', 'volume')
    ),
    (
        'external_rate_live',
        ('currency', 'timestamp_open', 'volume', 'timestamp_received', 'close', 'writetime(close)', 'ttl(close)')
    ),
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
)


@app.task
def backup_async():
    group([
        _create_cassandra_table_backup.si(*args) | _send_cassandra_table_backup_to_s3.si(args[0])
        for args in CASSANDRA_BACKUP_TABLES
    ])()


@app.task
def _create_cassandra_table_backup(table: str, columns: Tuple[str]):
    data = db.execute(f'SELECT {", ".join(columns)} from {table}')
    with gzip.open(f'/tmp/{table}.csv.gz', mode='wt') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(columns)
        writer.writerows([tuple(x) for x in data])


@app.task
def _send_cassandra_table_backup_to_s3(table: str):
    s3 = boto3.client('s3')
    s3.upload_file(
        f'/tmp/{table}.csv.gz',
        const.BACKUP_BUCKET,
        f'{table}_{datetime.date.today():%Y-%m-%d}.csv.gz'
    )



