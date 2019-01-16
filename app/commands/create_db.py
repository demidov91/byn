from cassandra_db import db


def run():
    db.execute('CREATE TABLE IF NOT EXISTS nbrb('
               'year int, '
               'month int, '
               'day int, '
               'date date, '
               'usd decimal,'
               'eur decimal,'
               'rub decimal,'
               'uah decimal, '
               'PRIMARY KEY (year, date)'
               ') WITH CLUSTERING ORDER BY (date DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS trade_dates('
               'date date PRIMARY KEY,'               
               ')')


if __name__ == '__main__':
    run()