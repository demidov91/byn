from cassandra_db import db


def run():
    db.execute('CREATE TABLE IF NOT EXISTS nbrb('
               'dummy boolean, ' # yes, I consider this table small enough to store on one partition (less than 2 mln. records)
               'date date, '
               'usd decimal,'
               'eur decimal,'
               'rub decimal,'
               'uah decimal, '
               'PRIMARY KEY (dummy, date)'
               ') WITH CLUSTERING ORDER BY (date DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS nbrb_local('
               'dummy boolean, ' # yes, I consider this table small enough to store on one partition (less than 2 mln. records)
               'date date, '
               'usd decimal,'
               'eur decimal,'
               'rub decimal,'
               'uah decimal, '
               'PRIMARY KEY (dummy, date)'
               ') WITH CLUSTERING ORDER BY (date DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS nbrb_global('
               'dummy boolean, ' # yes, I consider this table small enough to store on one partition (less than 2 mln. records)
               'date date, '
               'byn decimal,'
               'eur decimal,'
               'rub decimal,'
               'uah decimal, '
               'dxy decimal, '
               'PRIMARY KEY (dummy, date)'
               ') WITH CLUSTERING ORDER BY (date DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS trade_date('
               'dummy boolean, ' # yes, I consider this table small enough to store on one partition (less than 2 mln. records)
               'date date, '
               'PRIMARY KEY (dummy, date)'               
               ') WITH CLUSTERING ORDER BY (date DESC)')

    # db.execute('CREATE INDEX date')


if __name__ == '__main__':
    run()