from byn.cassandra_db import db


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

    db.execute(
        'CREATE TABLE IF NOT EXISTS rolling_average('
        'date date, '
        'duration int, '
        'eur decimal,'
        'rub decimal,'
        'uah decimal, '
        'dxy decimal, '
        'PRIMARY KEY (date, duration)'
        ') WITH CLUSTERING ORDER BY (duration ASC)'
    )

    db.execute('CREATE TABLE IF NOT EXISTS trade_date('
               'dummy boolean, ' # yes, I consider this table small enough to store on one partition (less than 2 mln. records)
               'date date, '
               'predicted decimal, '
               'prediction_error decimal, '
               'accumulated_error decimal, '
               'PRIMARY KEY (dummy, date)'               
               ') WITH CLUSTERING ORDER BY (date DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS external_rate('               
               'currency varchar, '
               'year int, '
               'datetime timestamp, '
               'open decimal , '
               'close decimal , '
               'low decimal, '
               'high decimal, '
               'volume int, '
               'PRIMARY KEY ((currency, year), datetime)'
               ') WITH CLUSTERING ORDER BY (datetime DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS external_rate_live('
               'currency varchar, '
               'timestamp_open timestamp, '
               'volume int, '
               'timestamp_received timestamp, '               
               'close varchar , '
               'PRIMARY KEY (currency, timestamp_open, volume)'
               ') WITH CLUSTERING ORDER BY (timestamp_open DESC, volume DESC)')

    db.execute('CREATE TABLE IF NOT EXISTS bcse('
               'currency varchar, '
               'timestamp_operation timestamp, '
               'timestamp_received timestamp, '
               'rate varchar , '
               'PRIMARY KEY (currency, timestamp_operation)'
               ') WITH CLUSTERING ORDER BY (timestamp_operation DESC)')



if __name__ == '__main__':
    run()
