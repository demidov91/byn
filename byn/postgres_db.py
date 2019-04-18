import asyncio
import os

import aiopg
import sqlalchemy as sa


metadata = sa.MetaData()

external_rate = sa.Table('external_rate', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('timestamp_close', sa.Integer),
               sa.Column('open', sa.DECIMAL),
               sa.Column('close', sa.DECIMAL),
               sa.Column('low', sa.DECIMAL),
               sa.Column('high', sa.DECIMAL),
               sa.Column('value', sa.SMALLINT),
               )

external_rate_live = sa.Table('external_rate_live', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('value', sa.SMALLINT, primary_key=True),
               sa.Column('timestamp_received', sa.INTEGER),
               sa.Column('close', sa.DECIMAL),
               )


bcse = sa.Table('bcse', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('timestamp_received', sa.INTEGER),
               sa.Column('rate', sa.DECIMAL),
               )

nbrb = sa.Table('nbrb', metadata,
               sa.Column('kind', sa.String(15), primary_key=True),
               sa.Column('date', sa.DATE, primary_key=True),
               sa.Column('usd', sa.DECIMAL),
               sa.Column('eur', sa.DECIMAL),
               sa.Column('rub', sa.DECIMAL),
               sa.Column('uah', sa.DECIMAL),
               sa.Column('byn', sa.DECIMAL),
               sa.Column('dxy', sa.DECIMAL),
               )

prediction = sa.Table('prediction', metadata,
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('external_rates', sa.JSON),
               sa.Column('bcse_full', sa.JSON),
               sa.Column('bcse_trusted_global', sa.JSON),
               sa.Column('prediction', sa.JSON),
               )

trade_date = sa.Table('trade_date', metadata,
               sa.Column('date', sa.DATE, primary_key=True),
               sa.Column('predicted', sa.DECIMAL),
               sa.Column('prediction_error', sa.DECIMAL),
               sa.Column('accumulated_error', sa.DECIMAL),
               )


DB_DATA = {
    'user': 'postgres',
    'password': os.environ["POSTGRES_PASSWORD"],
    'database': os.environ["POSTGRES_DB"],
    'host': os.environ["POSTGRES_HOST"],
    'port': os.environ["POSTGRES_PORT"],
}


async def init_pool():
    sa.create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**DB_DATA))
    metadata.create_all()
    return await aiopg.create_pool(
        'dbname={database} user={user} password={password} host={host} port={port}'.format(**DB_DATA)
    )


pool = asyncio.run(init_pool())