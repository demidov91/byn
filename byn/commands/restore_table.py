import csv
import datetime
import gzip
import logging
import sys
from decimal import Decimal
from math import ceil
from typing import Tuple, Sequence, List, Union, Iterable, Optional

import byn.logging
from byn.cassandra_db import db, _launch_in_parallel



logger = logging.getLogger(__name__)


def _launch_restore_in_parallel_default(table: str, headers: Sequence[str], data: Sequence[list]):
    yield from _launch_in_parallel(
        f'INSERT into {table} ({", ".join(headers)}) VALUES ({", ".join(["%s"] * len(headers))})',
        data,
        timeout=ceil(len(data) / 50)
    )


def _launch_restore_in_parallel_with_ttl(
        table: str,
        headers: Sequence[str],
        data: Sequence[List[str]],
        ttl: Union[Iterable[int], int]
):
    if not isinstance(ttl, Iterable):
        ttl = [ttl] * len(data)

    for row, ttl in zip(data, ttl):
        row.append(ttl)

    yield from _launch_in_parallel(
        f'INSERT into {table} ({", ".join(headers)}) VALUES ({", ".join(["%s"] * len(headers))}) USING TTL %s',
        data,
        timeout=ceil(len(data) / 100)
    )


def _replace_data(
        table: str,
        headers: Sequence[str],
        data: Sequence[List[str]],
        ttl: Optional[Union[Iterable[int], int]]=None
):
    db.execute(f'truncate table {table}')

    step = 1000
    for chunk in range(0, len(data), step):
        _insert_data(table, headers, data[chunk:chunk+step], ttl)

    logger.info('All %s data (%s rows) is successfully restored.', table, len(data))


def _insert_data(
        table: str,
        headers: Sequence[str],
        data: Sequence[List[str]],
        ttl: Optional[Union[Iterable[int], int]]=None
):
    errors = []

    if ttl is None:
        rs = tuple(_launch_restore_in_parallel_default(table, headers, data))
    else:
        rs = tuple(_launch_restore_in_parallel_with_ttl(table, headers, data, ttl))

    for r in rs:
        try:
            r.result()
        except Exception as e:
            errors.append(e)

            if len(errors) > len(data) / 200:
                raise errors[0] from RuntimeError(errors)

    if len(errors) > 0:
        logger.warning(errors)

    else:
        logger.info('%s chunk is saved.', table)


def _parse_data(
        data: Sequence[List[str]],
        *,
        bool_columns=None,
        decimal_columns=None,
        int_columns=None,
        datetime_columns=None,
        remove_columns=None,
):
    remove_columns = remove_columns or []

    headers = data[0]
    data = data[1:]
    bool_columns = [headers.index(x) for x in bool_columns or ()]
    decimal_columns = [headers.index(x) for x in decimal_columns or ()]
    int_columns = [headers.index(x) for x in int_columns or ()]
    datetime_columns = [headers.index(x) for x in datetime_columns or ()]

    for row in data:
        for i in decimal_columns:
            row[i] = Decimal(row[i])

        for i in int_columns:
            row[i] = int(row[i])

        for i in datetime_columns:
            row[i] = datetime.datetime.strptime(
                row[i],
                '%Y-%m-%d %H:%M:%S.%f' if '.' in row[i] else '%Y-%m-%d %H:%M:%S'
            )

        for i in bool_columns:
            if row[i] == 'True':
                row[i] = True
            elif row[i] == 'False':
                row[i] = False
            else:
                raise ValueError(row[i])

    data = _remove_columns(headers, data, *remove_columns)
    return headers, data


def _restore_from_csv_data(
        table,
        data,
        *,
        ttl_column=None,
        **kwargs
):
    headers, data = _parse_data(data, **kwargs)
    if ttl_column:
        ttl = _pop_column(headers=headers, data=data, column=ttl_column)
        ttl = [int(x) for x in ttl]
    else:
        ttl = None
    _replace_data(table, headers, data, ttl)

def _remove_columns(headers, data, *columns):
    """
    Modifies headers in-place. Returns data.
    """
    to_remove = [x in columns for x in headers]

    for c in columns:
        headers.remove(c)

    return [
        [row[i] for i in range(len(row)) if not to_remove[i]]
        for row in data
    ]

def _pop_column(*, headers: List[str], data: List[List], column):
    i = headers.index(column)
    headers.pop(i)
    info = []
    for row in data:
        info.append(row.pop(i))

    return info


def run(table: str, path: str, override: str):
    if override != '--override':
        raise ValueError('Please, provide --override key to be sure you understand that all existing data will be nuked.')


    if path.endswith('.csv'):
        _open = open
    elif path.endswith('.csv.gz'):
        _open = gzip.open
    else:
        raise ValueError('Unexpected dump file extension: %s' % path)

    with _open(path, mode='rt') as f:
        data = tuple(csv.reader(f, delimiter=';'))

    if table == 'external_rate_live':
        _restore_from_csv_data(
            'external_rate_live',
            data,
            ttl_column='ttl(close)',
            int_columns=['volume'],
            remove_columns=['writetime(close)'],
            datetime_columns=['timestamp_received'],
        )

    elif table == 'nbrb':
        _restore_from_csv_data(
            table,
            data,
            bool_columns=['dummy'],
            decimal_columns=['usd', 'eur', 'rub', 'uah']
        )

    elif table == 'nbrb_global':
        _restore_from_csv_data(
            table,
            data,
            decimal_columns=['dxy'],
            bool_columns=['dummy'],
        )

    elif table == 'external_rate':
        _restore_from_csv_data(
            table,
            data,
            decimal_columns=['open', 'close', 'low', 'high'],
            int_columns=['volume', 'year'],
        )
    elif table == 'bcse':
        _restore_from_csv_data(
            table,
            data,
            datetime_columns=['timestamp_operation', 'timestamp_received'],
        )
    elif table == 'prediction':
        _restore_from_csv_data(
            table,
            data,
            datetime_columns=['timestamp'],
        )
    elif table == 'trade_date':
        _restore_from_csv_data(
            table,
            data,
            decimal_columns=['predicted', 'prediction_error', 'accumulated_error'],
        )

    else:
        logger.info('Restoring %s dump without pre-processing.', table)
        _replace_data(table, data[0], data[1:])



if __name__ == '__main__':
    run(*sys.argv[1:])