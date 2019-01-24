import sys

from byn.cassandra_db import db


def run(*table_names: str):
    for table in table_names:
        db.execute(f'drop table {table}')


if __name__ == '__main__':
    run(*sys.argv[1:])