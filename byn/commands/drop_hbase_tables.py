import sys

from byn.hbase_db import db


def run(*table_names: str):
    with db.connection() as connection:
        for table in table_names:
            connection.delete_table(table, disable=True)


if __name__ == '__main__':
    run(*sys.argv[1:])