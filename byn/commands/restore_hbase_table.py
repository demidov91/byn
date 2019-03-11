import csv
import gzip
import sys

from byn.hbase_db import table


def run(table_name: str, path: str):
    if path.endswith('.csv'):
        _open = open
    elif path.endswith('.csv.gz'):
        _open = gzip.open
    else:
        raise ValueError('Unexpected dump file extension: %s' % path)

    with _open(path, mode='rt') as f, table(table_name) as the_table:
        with the_table.batch(batch_size=100) as batch:
            for row in csv.DictReader(f, delimiter=';'):
                key = row.pop('key').encode()
                data = {column.encode(): value for column, value in row.items() if value}
                batch.put(key, data)


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
