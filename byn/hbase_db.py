import datetime
import json
import os
import threading
from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from dataclasses import asdict
from decimal import Decimal
from typing import Collection, Dict, Iterable, Iterator, Optional, Sequence, Tuple, Union
from enum import Enum

import happybase
from celery.signals import worker_process_init
from happybase.util import bytes_increment
from happybase.pool import ConnectionPool

from byn.datatypes import ExternalRateData, BcseData
from byn.predict.predictor import PredictionRecord
from byn.utils import DecimalAwareEncoder, always_on_sync


thread_local = threading.local()

@always_on_sync
def _get_pool():
    print('Pool initialized!')
    return happybase.ConnectionPool(size=4, host=os.environ['HBASE_HOST'], timeout=5000, protocol='binary')




class PerProcessHbasePool:
    def __getattr__(self, item):
        return getattr(thread_local.hbase_pool, item)

    def __setattr__(self, key, value):
        return setattr(thread_local.hbase_pool, key, value)

    def connect(self):
        thread_local.hbase_pool = _get_pool()


db = PerProcessHbasePool() # type: Union[ConnectionPool, PerProcessHbasePool]
db.connect()


@worker_process_init.connect
def _init_hbase_pool(**kwargs):
    db.connect()


@contextmanager
def table(name: str):
    with db.connection() as connection:
        yield connection.table(name)


DATE_FORMAT = '%Y-%m-%d'


def next_date_to_string(date: datetime.date) -> str:
    return (date + datetime.timedelta(days=1)).strftime(DATE_FORMAT)


def date_to_bytes(date: datetime.date) -> bytes:
    return date.strftime(DATE_FORMAT).encode()


def date_to_next_bytes(date: datetime.date) -> bytes:
    return bytes_increment(date_to_bytes(date))


def bytes_to_date(data: bytes) -> datetime.date:
    return datetime.datetime.strptime(data.decode(), DATE_FORMAT).date()


def key_part(key: bytes, index: int) -> bytes:
    return key.split(b'|')[index]


def key_part_as_date(key: bytes, index: int) -> datetime.date:
    return bytes_to_date(key_part(key, index))


def get_decimal(row: dict, column: bytes) -> Optional[Decimal]:
    if column not in row:
        return None

    return Decimal(row[column].decode())


class NbrbKind(Enum):
    OFFICIAL = b'official'
    LOCAL = b'local'
    GLOBAL = b'global'

    @property
    def as_prefix(self):
        return self.value + b'|'



############# SELECT ############
def get_last_nbrb_record(kind: NbrbKind) -> Tuple[bytes, Dict[bytes, bytes]]:
    with table('nbrb') as nbrb:
        return next(iter(nbrb.scan(reverse=True, limit=1, row_prefix=kind.as_prefix)), None)

def get_last_nbrb_global_with_rates():
    with table('nbrb') as nbrb:
        return next(iter(nbrb.scan(
            reverse=True,
            limit=1,
            row_prefix=NbrbKind.GLOBAL.as_prefix,
            filter="SingleColumnValueFilter('rate', 'byn', >, 'binary:', true, true)"
        )), None)


def get_last_external_currency_datetime(currency: str) -> datetime.datetime:
    with table('external_rate') as external_rate:
        data = next(external_rate.scan(reverse=True, row_prefix=currency.encode(), limit=1), None)

    if data is None:
        return datetime.datetime.fromtimestamp(0)

    timestamp = int(key_part(data[0], 1))
    return datetime.datetime.fromtimestamp(timestamp)



def get_last_rolling_average_date() -> Optional[datetime.date]:
    with table('rolling_average') as rolling_average:
        data = next(iter(rolling_average.scan(reverse=True, limit=1)), None)

    if data is None:
        return None

    return key_part_as_date(data[0], 0)


def get_nbrb_gt(date: Optional[datetime.date], kind: NbrbKind):
    start_date_part = date_to_next_bytes(date) if date else b''
    with table('nbrb') as nbrb:
        return nbrb.scan(
            row_start=kind.as_prefix + start_date_part,
            row_stop=bytes_increment(kind.as_prefix)
        )

def get_bcse_in(
        currency: str,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime = None
) -> Iterable[Tuple[int, Decimal]]:
    start_dt = int(start_dt.timestamp())
    end_dt = int((end_dt or datetime.datetime(2035, 1, 1)).timestamp())

    with table('bcse') as bcse:
        key_data_pairs = bcse.scan(
            row_start=f'{currency}|{start_dt}'.encode(),
            row_stop=f'{currency}|{end_dt}'.encode()
        )

    return (
        (int(key_part(key, 1)), get_decimal(data, b'rate:rate'))
        for key, data in key_data_pairs
    )


def get_external_rate_live(start_dt: datetime.datetime,
                           end_dt: Optional[datetime.datetime] = None) -> Dict[
    str, Iterable[Tuple[int, Decimal]]]:
    # Making 4 parallel requests in cassandra fitted this case better...

    end_dt = end_dt or datetime.datetime(2100, 1, 1)
    currencies = 'EUR', 'RUB', 'UAH', 'DXY'

    data = {}
    with table('external_rate_live') as external_rate_live:

        for currency in currencies:
            # Get the data.
            rows = external_rate_live.scan(
                row_start=f'{currency}|{int(start_dt.timestamp())}|'.encode(),
                row_stop=f'{currency}|{int(end_dt.timestamp())}|'.encode(),
            )
            # Decode what we got.
            rows = [{
                'ts_open': int(key_part(key, 1)),
                'ts_received': int(value[b'rate:timestamp_received']),
                'rate': get_decimal(value, b'rate:close'),
            } for key, value in rows]

            # Create close timestamps.
            open_to_close = OrderedDict()
            prev_key = None
            for row in rows:
                if row['ts_open'] not in open_to_close:
                    open_to_close[prev_key] = row['ts_open']

            # Set data with correct timestamps.
            data[currency] = tuple(
                (
                    _choose_real_time_for_live_event(
                        row['ts_open'],
                        row['ts_received'],
                        open_to_close.get(row['ts_received'])
                    ),
                    row['rate']
                ) for row in rows
            )

    return data



def _choose_real_time_for_live_event(
        timestamp_open: int,
        timestamp_received: int,
        timestamp_close: Optional[int]
):
    left_boundary = max(timestamp_open, timestamp_received)
    return min(left_boundary, timestamp_close - 1) if timestamp_close is not None else left_boundary


def get_latest_external_rates(
        start_dt: datetime.datetime,
        *,
        at_least_one: bool = False
) -> Dict[str, Iterable[list]]:


    currencies = 'EUR', 'RUB', 'UAH', 'DXY'
    currency_to_rows = defaultdict(list)


    if at_least_one:
        last_data = get_the_last_external_rates(currencies, end_dt=start_dt)
        for currency, row in last_data.items():
            currency_to_rows[currency].append(row)


    with table('external_rate') as external_rate:
        for currency in currencies:
            rows = external_rate.scan(
                row_start=f'{currency}|{int(start_dt.timestamp())}'.encode(),
                row_stop=bytes_increment(f'{currency}|'.encode()),
            )

            currency_to_rows[currency].extend(_parse_external_rate_row(*x) for x in rows)


    return {
        currency: _external_rate_data_into_pairs(currency_to_rows[currency])
        for currency in currencies
    }


def _external_rate_data_into_pairs(rates: Sequence[dict]):
    pairs = []

    for i in range(len(rates) - 1):
        pairs.append((
            rates[i]['ts_open'],
            rates[i]['rate_open'],
        ))
        pairs.append((
            rates[i+1]['ts_open'] - 1,
            rates[i]['rate_close'],
        ))

    pairs.append((
        rates[-1]['ts_open'],
        rates[-1]['rate_open'],
    ))

    return pairs


def get_the_last_external_rates(currencies: Iterable[str], end_dt: datetime.datetime) -> Dict[str, dict]:
    currency_to_data = {}

    with table('external_rate') as external_rate:
        for currency in currencies:
            currency_to_data[currency] = _parse_external_rate_row(
                *next(external_rate.scan(
                    row_start=f'{currency}|{int(end_dt.timestamp())}'.encode(),
                    reverse=True,
                    limit=1
                ))
            )

    return currency_to_data


def _parse_external_rate_row(key, value):
    return {
        'ts_open': int(key_part(key, 1)),
        'rate_open': get_decimal(value, b'rate:open'),
        'rate_close': get_decimal(value, b'rate:close'),
    }


def get_accumulated_error(date: datetime.date):
    with table('trade_date') as trade_date:
        record = next(
            trade_date.scan(
                reverse=True,
                limit=1,
                columns=[b'rate:accumulated_error'],
                filter="SingleColumnValueFilter('rate', 'accumulated_error', >, 'binary:', true, true)",
                row_start=date_to_bytes(date)
            ),
            None
        )

    return record and get_decimal(record[1], b'rate:accumulated_error')


############## INSERT ############

def _put_in_batch(
        table_name: str,
        data: Dict[bytes, Dict[bytes, bytes]],
        *,
        prefix: bytes=b'',
        transaction=True,
):
    with table(table_name) as a_table:
        with a_table.batch(transaction=transaction) as batch:
            for key, values in data.items():
                batch.put(prefix + key, values)


def insert_nbrb(data):
    data = {
        x['date'].encode():
        {
            b'rate:usd': str(x['USD']).encode(),
            b'rate:eur': str(x['EUR']).encode(),
            b'rate:rub': str(x['RUB']).encode(),
            b'rate:uah': str(x['UAH']).encode(),
        }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=NbrbKind.OFFICIAL.as_prefix)



def insert_nbrb_local(data):
    data = {
        x['date']:
            {
                b'rate:usd': str(x['USD']).encode(),
                b'rate:eur': str(x['EUR']).encode(),
                b'rate:rub': str(x['RUB']).encode(),
                b'rate:uah': str(x['UAH']).encode(),
            }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=NbrbKind.LOCAL.as_prefix)


def add_nbrb_global(data):
    data = {
        x['date']:
            {
                b'rate:byn': str(x['BYN']).encode(),
                b'rate:eur': str(x['EUR']).encode(),
                b'rate:rub': str(x['RUB']).encode(),
                b'rate:uah': str(x['UAH']).encode(),
            }
        for x in data
    }
    _put_in_batch('nbrb', data, prefix=NbrbKind.GLOBAL.as_prefix)


def insert_trade_dates(trade_dates: Collection[str]):
    _put_in_batch('trade_date', {x.encode(): {} for x in trade_dates})


def insert_dxy_12MSK(data: Iterable[Tuple[str ,str]]):
    data = {
        date.encode(): {
            b'rate:dxy': dxy.encode()
        }
        for date, dxy in data
    }
    _put_in_batch('nbrb', data, prefix=b'global|')


def insert_external_rates(
        currency: str,
        data: Iterator[
            Tuple[
                int,
                str,
                str,
                str,
                str,
                int
            ]
        ]
):
    data = {
        f'{currency}|{record[0]}'.encode(): {
            b'rate:open': record[1].encode(),
            b'rate:close': record[2].encode(),
            b'rate:low': record[3].encode(),
            b'rate:high': record[4].encode(),
            b'rate:volume': str(record[5]).encode(),
        }
        for record in data
    }

    _put_in_batch('external_rate', data=data)



def insert_external_rate_live(row: ExternalRateData):
    with table('external_rate_live') as external_rate_live:
        external_rate_live.put(f'{row.currency}|{row.timestamp_open}|{row.volume}'.encode(), {
            b'rate:timestamp_received': str(int(round(row.timestamp_received))).encode(),
            b'rate:close': str(row.close).encode(),
        })



def insert_bcse(data: Iterable[BcseData], **kwargs):
    data = {
        f'{x.currency}|{x.timestamp_operation}'.encode(): {
            b'rate:timestamp_received': str(x.timestamp_received).encode(),
            b'rate:rate': x.rate.encode(),
        }
        for x in data
    }

    _put_in_batch('bcse', data=data)


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
            b'rate:eur': str(data[0]).encode(),
            b'rate:rub': str(data[1]).encode(),
            b'rate:uah': str(data[2]).encode(),
            b'rate:dxy': str(data[3]).encode(),
        })


def _ndarray_to_tuple_of_tuples(numpy_array):
    return tuple(tuple(row) for row in numpy_array)


