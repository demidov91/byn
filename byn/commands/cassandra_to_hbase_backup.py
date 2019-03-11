import datetime
import gzip
import sys
from csv import DictReader, DictWriter

def _cass_ts_to_hbase_ts(cass_ts: str) -> str:
    return str(int(datetime.datetime.strptime(
        cass_ts,
        '%Y-%m-%d %H:%M:%S.%f' if '.' in cass_ts else '%Y-%m-%d %H:%M:%S'
    ).timestamp()))


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


if __name__ == '__main__':
    if sys.argv[1] == 'bcse':
        convert_bcse(*sys.argv[2:])
    elif sys.argv[1] == 'trade_date':
        convert_trade_date(*sys.argv[2:])
    else:
        raise ValueError(sys.argv[0])


