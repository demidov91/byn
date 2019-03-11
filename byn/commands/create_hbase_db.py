from byn.hbase_db import db


def run():
    with db.connection() as connection:
        connection.create_table('nbrb', {'rate': {}})
        connection.create_table('rolling_average', {'rate': {}})
        connection.create_table('trade_date', {'rate': {}})
        connection.create_table('external_rate', {'rate': {}})
        connection.create_table('external_rate_live', {'rate': {}})
        connection.create_table('bcse', {'rate': {}})
        connection.create_table('prediction', {'rate': {}})


if __name__ == '__main__':
    run()

