import csv
import datetime
import gzip

import boto3

from byn.cassandra_db import db
from byn.tasks.launch import app
from byn import constants as const

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
