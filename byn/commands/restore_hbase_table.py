import csv
import gzip

from byn.hbase_db import table


def run(table_name: str, path: str, override: str):
    if override != '--override':
        raise ValueError('Please, provide --override key to be sure you understand that all existing data will be nuked.')


    if path.endswith('.csv'):
        _open = open
    elif path.endswith('.csv.gz'):
        _open = gzip.open
    else:
        raise ValueError('Unexpected dump file extension: %s' % path)

    with _open(path, mode='rt') as f, table(table_name) as the_table:
        with the_table.batch(batch_size=100) as batch:
            for row in csv.DictReader(f):
                key = row.pop('key').encode()
                data = {column.encode(): value for column, value in row.items() if value}
                batch.put(key, data)
