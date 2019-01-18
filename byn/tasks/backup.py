import csv
import datetime
import gzip
from typing import Tuple

import boto3
from celery import group

from byn import constants as const
from byn.cassandra_db import db
from byn.tasks.launch import app


@app.task
def backup_async():
    group([backup_nbrb_async.si(), backup_external_rates_async.si()])()


######################
######################


@app.task
def backup_nbrb_async():
    (_create_backup_nbrb.si() | _send_backup_nbrb_to_s3.si())()


@app.task
def _create_backup_nbrb():
    nbrb_data = db.execute("SELECT date, usd, eur, rub, uah from nbrb where dummy=true")

    data = {x.date: list(x) for x in nbrb_data}

    dxy_data = db.execute("SELECT date, dxy from nbrb_global where dummy=true")

    for date, dxy in dxy_data:
        data[date].append(dxy)

    with gzip.open(const.NBRB_BACKUP_PATH, mode='wt', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(('date', 'usd', 'eur', 'rub', 'uah', 'dxy'))
        writer.writerows(data.values())


@app.task
def _send_backup_nbrb_to_s3():
    filename = f'nbrb_{datetime.date.today():%Y-%m-%d}.csv.gz'
    s3 = boto3.client('s3')
    s3.upload_file(const.NBRB_BACKUP_PATH, const.BACKUP_BUCKET, filename)


########################################
########################################


CASSANDRA_BACKUP_TABLES = (
    (
        'external_rate',
        ('currency', 'datetime', 'open', 'close', 'low', 'high', 'volume')
    ),
    (
        'external_rate_live',
        ('currency', 'timestamp_open', 'volume', 'timestamp_received', 'close', 'writetime(close)', 'ttl(close)')
    ),
)


@app.task
def backup_external_rates_async():
    group([
        _create_cassandra_table_backup.si(*args) | _send_cassandra_table_backup_to_s3.si(args[0])
        for args in CASSANDRA_BACKUP_TABLES
    ])()


@app.task
def _create_cassandra_table_backup(table: str, columns: Tuple[str]):
    data = db.execute(f'SELECT {", ".join(columns)} from {table}')
    with gzip.open(f'/tmp/{table}.csv', mode='wt') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(columns)
        writer.writerows([tuple(x) for x in data])


@app.task
def _send_cassandra_table_backup_to_s3(table: str):
    s3 = boto3.client('s3')
    s3.upload_file(
        f'/tmp/{table}.csv',
        const.BACKUP_BUCKET,
        f'{table}_{datetime.date.today():%Y-%m-%d}.csv.gz'
    )



