from byn.hbase_db import db


def run():
    with db.connection() as connection:
        existing_tables = [x.decode() for x in connection.tables()]
        tables_to_create = (
            'nbrb',
            'rolling_average',
            'trade_date',
            'external_rate',
            'external_rate_live',
            'bcse',
            'prediction',
        )

        for table_name in tables_to_create:
            if table_name in existing_tables:
                print(f'{table_name} already exists')
                continue

            connection.create_table(table_name, {'rate': {}})
            print(f'{table_name} created.')


if __name__ == '__main__':
    run()

