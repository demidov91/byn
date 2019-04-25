import gzip
import sys
from csv import DictReader, DictWriter
from typing import List


def value_or_key(mapping: dict, key):
    return mapping.get(key, key)


def _hbase_to_postgres_headers(
        hbase_reader: DictReader,
        key_columns: List[str],
        column_mapping: dict
):
    return key_columns + [
        value_or_key(column_mapping, x.split(':')[1])
        for x in hbase_reader.fieldnames[1:]
    ]


def _hbase_to_postgres_row(original_row: dict, key_columns: List[str], column_mapping: dict):
    row = dict(zip(key_columns, original_row.pop('key').split('|')))
    row.update((
        (
            value_or_key(column_mapping, x.split(':')[1]),
            original_row[x]
        ) for x in original_row
    ))
    return row


def convert_table(hbase_backup_path: str, postgres_backup_path: str, key_columns: List[str], column_mapping=None):
    column_mapping = column_mapping or {}

    with gzip.open(hbase_backup_path, mode='rt') as hbase_f, gzip.open(postgres_backup_path, mode='wt') as postgres_f:
        reader = DictReader(hbase_f, delimiter=';')

        writer = DictWriter(
            postgres_f,
            fieldnames=_hbase_to_postgres_headers(reader, key_columns, column_mapping),
            delimiter=';'
        )
        writer.writeheader()
        writer.writerows((_hbase_to_postgres_row(x, key_columns, column_mapping) for x in reader))


if __name__ == '__main__':
    column_mapping = None

    if sys.argv[1] == 'bcse':
        key_columns = ['currency', 'timestamp']
    elif sys.argv[1] == 'trade_date':
        key_columns = ['date']
    elif sys.argv[1] == 'nbrb':
        key_columns = ['kind', 'date']
    elif sys.argv[1] == 'external_rate_live':
        key_columns = ['currency', 'timestamp', 'volume']
        column_mapping = {'close': 'rate'}
    elif sys.argv[1] == 'external_rate':
        key_columns = ['currency', 'timestamp']
    elif sys.argv[1] == 'prediction':
        key_columns = ['timestamp']
    else:
        raise ValueError(sys.argv[0])

    convert_table(*sys.argv[2:], key_columns, column_mapping)