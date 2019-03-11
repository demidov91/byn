import datetime
import gzip
import sys
import csv
from csv import DictReader, DictWriter

def _cass_ts_to_hbase_ts(cass_ts: str) -> str:
    return str(int(datetime.datetime.strptime(
        cass_ts,
        '%Y-%m-%d %H:%M:%S.%f' if '.' in cass_ts else '%Y-%m-%d %H:%M:%S'
    ).replace(tzinfo=datetime.timezone.utc).timestamp()))


def convert_bcse(cassandra_backup_path: str, hbase_backup_path: str):
    with gzip.open(cassandra_backup_path, mode='rt') as cass_f, gzip.open(hbase_backup_path, mode='wt') as hbase_f:
        reader = DictReader(cass_f, delimiter=';')
        writer = DictWriter(
            hbase_f,
            fieldnames=['key', 'rate:timestamp_received', 'rate:rate'],
            delimiter=';'
        )
        writer.writeheader()

        for row in reader:
            writer.writerow({
                'key': f'{row["currency"]}|{_cass_ts_to_hbase_ts(row["timestamp_operation"])}',
                'rate:timestamp_received': _cass_ts_to_hbase_ts(row['timestamp_received']),
                'rate:rate': row['rate'],
            })


def convert_trade_date(cassandra_backup_path: str, hbase_backup_path: str):
    rows = 'predicted', 'prediction_error', 'accumulated_error'

    with gzip.open(cassandra_backup_path, mode='rt') as cass_f, gzip.open(hbase_backup_path, mode='wt') as hbase_f:
        reader = DictReader(cass_f, delimiter=';')
        writer = DictWriter(
            hbase_f,
            fieldnames=['key', *('rate:' + x for x in rows)],
            delimiter=';'
        )
        writer.writeheader()

        for row in reader:
            pairs = [('key', row['date'])]
            pairs.extend(('rate:' + x, row.get(x)) for x in rows)
            writer.writerow(dict(pairs))


def convert_nbrb(official_path: str, global_path: str, hbase_backup_path: str):
    with gzip.open(hbase_backup_path,mode='wt') as hbase_f:
        writer = DictWriter(
            hbase_f,
            delimiter=';',
            fieldnames=['key', 'rate:usd', 'rate:eur', 'rate:uah', 'rate:rub', 'rate:dxy']
        )
        writer.writeheader()

        with gzip.open(official_path, mode='rt') as official_f:
            reader = DictReader(official_f, delimiter=';')

            for row in reader:
                writer.writerow({
                    'key': f"official|{row['date']}",
                    'rate:usd': row['usd'],
                    'rate:eur': row['eur'],
                    'rate:rub': row['rub'],
                    'rate:uah': row['uah'],
                })

        with gzip.open(global_path, mode='rt') as global_f:
            reader = DictReader(global_f, delimiter=';')

            for row in reader:
                writer.writerow({
                    'key': f"global|{row['date']}",
                    'rate:dxy': row['dxy'],
                })


def convert_external_rate_live(cassandra_backup_path: str, hbase_backup_path: str):
    key_columns = 'currency', 'timestamp_open', 'volume',
    data_columns = 'close', 'timestamp_received',

    with gzip.open(cassandra_backup_path, mode='rt') as cass_f, gzip.open(hbase_backup_path, mode='wt') as hbase_f:
        reader = csv.reader(cass_f, delimiter=',')
        writer = DictWriter(
            hbase_f,
            fieldnames=['key', *('rate:' + x for x in data_columns)],
            delimiter=';'
        )
        writer.writeheader()

        for row in reader:
            ts_open = _cass_ts_to_hbase_ts(row[1][:-5])
            ts_received = _cass_ts_to_hbase_ts(row[4][:-5])

            writer.writerow({
                'key': f'{row[0]}|{ts_open}|{row[2]}',
                'rate:close': row[3],
                'rate:timestamp_received': ts_received,
            })


def convert_external_rate(cassandra_backup_path: str, hbase_backup_path: str):
    key_columns = ['currency', 'year', 'datetime']
    data_columns = ['close', 'high', 'low', 'open', 'volume']

    with gzip.open(cassandra_backup_path, mode='rt') as cass_f, gzip.open(hbase_backup_path, mode='wt') as hbase_f:
        reader = DictReader(
            cass_f,
            delimiter=',',
            fieldnames=key_columns + data_columns,
        )
        writer = DictWriter(
            hbase_f,
            fieldnames=['key', *('rate:' + x for x in data_columns)],
            delimiter=';'
        )
        writer.writeheader()

        for row in reader:
            ts_open = _cass_ts_to_hbase_ts(row['datetime'][:-5])
            pairs = [('key', f"{row['currency']}|{ts_open}")]
            pairs.extend(('rate:' + x, row.get(x)) for x in data_columns)
            writer.writerow(dict(pairs))


if __name__ == '__main__':
    if sys.argv[1] == 'bcse':
        convert_bcse(*sys.argv[2:])
    elif sys.argv[1] == 'trade_date':
        convert_trade_date(*sys.argv[2:])
    elif sys.argv[1] == 'nbrb':
        convert_nbrb(*sys.argv[2:])
    elif sys.argv[1] == 'external_rate_live':
        convert_external_rate_live(*sys.argv[2:])
    elif sys.argv[1] == 'external_rate':
        convert_external_rate(*sys.argv[2:])
    else:
        raise ValueError(sys.argv[0])


