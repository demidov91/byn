import sys

from byn.hbase_db import db


def run(*table_names: str):
    with db.connection() as connection:
        tables = [x.decode() for x in connection.tables()]

        for table_name in table_names:
            if table_name not in tables:
                print(f'{table_name} does not exist.')
                continue

            connection.delete_table(table_name, disable=True)
            print(f'{table_name} is deleted.')


if __name__ == '__main__':
    run(*sys.argv[1:])