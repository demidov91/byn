import datetime
import json
import os
from contextlib import contextmanager
from dataclasses import asdict
from decimal import Decimal
from typing import Collection, Dict, Iterable, Iterator, Sequence, Tuple

import happybase

from byn.datatypes import ExternalRateData, BcseData
from byn.predict.predictor import PredictionRecord
from byn.utils import DecimalAwareEncoder

db = happybase.ConnectionPool(size=4, host=os.environ['HBASE_HOST'])


@contextmanager
def table(name: str):
    with db.connection() as connection:
        yield connection.table(name)


DATE_FORMAT = '%Y-%m-%d'


def next_date_to_string(date: datetime.date) -> str:
    return (date + datetime.timedelta(days=1)).strftime(DATE_FORMAT)


def next_date_to_bytes(date: datetime.date) -> bytes:
    return next_date_to_string(date).encode()


def date_to_bytes(date: datetime.date) -> bytes:
    return date.strftime(DATE_FORMAT).encode()


def bytes_to_date(data: bytes) -> datetime.date:
    return datetime.datetime.strptime(data.decode(), DATE_FORMAT).date()


def key_part(key: bytes, index: int) -> bytes:
    return key.split(b'|')[index]


def key_part_as_date(key: bytes, index: int) -> datetime.date:
    return bytes_to_date(key_part(key, index))


def get_decimal(row: dict, column: bytes) -> Decimal:
    return Decimal(row[column].decode())


############## INSERT ############

def _put_in_batch(
        table_name: str,
        key_to_data: Dict[bytes, Dict[bytes, bytes]],
        *,
        prefix: bytes=b'',
        transaction=True,
):
    with table(table_name) as a_table:
        with a_table.batch(transaction=True) as batch:
            for key, data in key_to_data.items():
                batch.put(prefix + key, data)


def insert_nbrb(data):
    data = {
        date_to_bytes(x['date']):
        {
            b'rate:usd': str(x['USD']).encode(),
            b'rate:eur': str(x['EUR']).encode(),
            b'rate:rub': str(x['RUB']).encode(),
            b'rate:uah': str(x['UAH']).encode(),
        }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=b'official|')



def insert_nbrb_local(data):
    data = {
        date_to_bytes(x['date']):
            {
                b'rate:usd': str(x['USD']).encode(),
                b'rate:eur': str(x['EUR']).encode(),
                b'rate:rub': str(x['RUB']).encode(),
                b'rate:uah': str(x['UAH']).encode(),
            }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=b'local|')


def add_nbrb_global(data):
    data = {
        date_to_bytes(x['date']):
            {
                b'rate:byn': str(x['BYN']).encode(),
                b'rate:eur': str(x['EUR']).encode(),
                b'rate:rub': str(x['RUB']).encode(),
                b'rate:uah': str(x['UAH']).encode(),
            }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=b'local|')


def insert_trade_dates(trade_dates: Collection[str]):
    _put_in_batch('trade_date', {x.encode(): {} for x in trade_dates})


def insert_dxy_12MSK(data: Iterable[Tuple[str ,str]]):
    data = {
        date.encode(): dxy.encode
        for date, dxy in data
    }
    _put_in_batch('nbrb', data, prefix=b'global|')


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
    data = {
        f'{currency}|{record[0]}'.encode(): {
            b'rate:open': record[1],
            b'rate:close': record[2],
            b'rate:low': record[3],
            b'rate:high': record[4],

        }
        for record in data
    }

    _put_in_batch('external_rate', key_to_data=data)



def insert_external_rate_live(row: ExternalRateData):
    with table('external_rate_live') as external_rate_live:
        external_rate_live.put(f'{row.currency}|{row.timestamp_open}|{row.volume}'.encode(), {
            b'rate:timestamp_received': str(row.timestamp_received).encode(),
            b'rate:close': str(row.close).encode(),
        })



def insert_bcse(data: Iterable[BcseData], **kwargs):
    data = {
        f'{x.currency}|{x.timestamp_operation}'.encode(): {
            b'rate:timestamp_received': x.timestamp_received,
            b'rate:rate': x.rate,
        }
        for x in data
    }

    _put_in_batch('bcse', key_to_data=data)


def insert_prediction(
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

    with table('prediction') as prediction_table:
        prediction_table.put(str(timestamp).encode(), {
            b'rate:external_rates': json.dumps(external_rates, cls=DecimalAwareEncoder).encode(),
            b'rate:bcse_full': json.dumps(bcse_full, cls=DecimalAwareEncoder).encode(),
            b'rate:bcse_trusted_global': json.dumps(bcse_trusted_global, cls=DecimalAwareEncoder).encode(),
            b'rate:prediction': json.dumps(asdict(prediction), cls=DecimalAwareEncoder).encode(),
        })


def insert_rolling_average(date: datetime.date, duration: int, data: Sequence[Decimal]):
    with table('rolling_average') as rolling_average:
        key = date_to_bytes(date) + b'|' + str(duration).encode()
        rolling_average.put(key, {
            b'rate:eur': data[0],
            b'rate:rub': data[1],
            b'rate:uah': data[2],
            b'rate:dxy': data[3],
        })


def _ndarray_to_tuple_of_tuples(numpy_array):
    return tuple(tuple(row) for row in numpy_array)


