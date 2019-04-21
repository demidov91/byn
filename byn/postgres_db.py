import asyncio
import datetime
import logging
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from decimal import Decimal
from enum import Enum
from typing import Collection, Dict, Iterable, Iterator, Optional, Sequence, Tuple

import aiopg
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as psql_insert

from byn.datatypes import BcseData, ExternalRateData
from byn.predict.predictor import PredictionRecord


logger = logging.getLogger(__name__)
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
               sa.Column('rates', sa.DECIMAL),
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

rolling_average = sa.Table('rolling_average', metadata,
                   sa.Column('date', sa.DATE, primary_key=True),
                   sa.Column('duration', sa.SMALLINT, primary_key=True),
                   sa.Column('eur', sa.DECIMAL),
                   sa.Column('rub', sa.DECIMAL),
                   sa.Column('uah', sa.DECIMAL),
                   sa.Column('dxy', sa.DECIMAL),
               )


DB_DATA = {
    'user': 'postgres',
    'password': os.environ["POSTGRES_PASSWORD"],
    'database': os.environ["POSTGRES_DB"],
    'host': os.environ["POSTGRES_HOST"],
    'port': os.environ["POSTGRES_PORT"],
}


async def init_pool():
    try:
        metadata.create_all(
            sa.create_engine(
                'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**DB_DATA)
            )
        )
    except:
        logger.exception("Can't initialize db.")
        return None

    try:
        return await aiopg.create_pool(
            'dbname={database} user={user} password={password} host={host} port={port}'.format(**DB_DATA)
        )
    except:
        logger.exception("Can't initialize pool.")
        return None


pool = asyncio.run(init_pool())


class NbrbKind(Enum):
    OFFICIAL = 'official'
    LOCAL = 'local'
    GLOBAL = 'global'


_not_set = object()

async def anext(async_iter, default=_not_set):
    try:
        return async_iter.__aiter__().__anext__()
    except StopAsyncIteration as e:
        if default is _not_set:
            raise e

        return default


@asynccontextmanager
async def pool_cursor():
    with (await pool.cursor()) as cursor:
        yield cursor



############# SELECT ############
async def get_last_nbrb_record(kind: NbrbKind):
    async with pool_cursor() as cur:
        return await anext(
            cur.execute(
                nbrb.select(nbrb.c.kind==kind.value)
                    .order_by(sa.desc(nbrb.c.date))
                    .limit(1)
            ), None
        )

async def get_last_nbrb_global_with_rates():
    async with pool_cursor() as cur:
        return await anext(cur.execute(
            nbrb.select(nbrb.c.kind == NbrbKind.GLOBAL.value and nbrb.c.rate != None)
            .order_by(sa.desc(nbrb.c.date)
            .limit(1)
        )), None)


async def get_nbrb_rate(date: datetime.date, kind: NbrbKind):
    async with pool_cursor() as cur:
        await anext(cur.execute(
            nbrb.select((nbrb.c.date == date) & (nbrb.c.kind == kind.value))
        ), None)


async def get_last_external_currency_datetime(currency: str) -> datetime.datetime:
    async with pool_cursor() as cur:
        row = await anext(
            cur.execute(
                external_rate.select(external_rate.c.currency==currency)
                    .order_by(sa.desc(external_rate.c.timestamp))
                    .limit(1)
            ), None)

    if row is None:
        return datetime.datetime.fromtimestamp(0)

    return row.timestamp


async def get_last_rolling_average_date() -> Optional[datetime.date]:
    async with pool_cursor() as cur:
        row = await anext(cur.execute(
            rolling_average.select().order_by(sa.desc(rolling_average.c.date)).limit(1)
        ))

    return row and row.date

async def get_rolling_average_lte(date: datetime.date):
    async with pool_cursor() as cur:
        return await cur.execute(
            rolling_average.select(rolling_average.c.date <= date)
            .order_by(rolling_average.c.date)
        )


async def get_nbrb_gt(date: Optional[datetime.date], kind: NbrbKind):
    async with pool_cursor() as cur:
        return await cur.execute(
            nbrb.select((nbrb.c.date > date) & (nbrb.c.kind == kind)).order_by(nbrb.c.date)
        )


async def get_nbrb_lte(date: datetime.date, kind: NbrbKind):
    async with pool_cursor() as cur:
        return await cur.execute(
            nbrb.select(
                (nbrb.c.date <= date) &
                (nbrb.c.kind == kind.value)
            ).order_by(nbrb.c.date)
        )


async def get_valid_nbrb_gt(date: datetime.date, kind: NbrbKind):
    async with pool_cursor() as cur:
        return await cur.execute(
            nbrb.select(
                (nbrb.c.date > date) &
                (nbrb.c.kind == kind.value) &
                (nbrb.c.eur != None) &
                (nbrb.c.rub != None) &
                (nbrb.c.uah != None)
            ).order_by(nbrb.c.date)
        )



async def get_bcse_in(
        currency: str,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime = None
) -> Iterable[Tuple[int, Decimal]]:
    end_dt = end_dt or datetime.datetime(2035, 1, 1)

    async with pool_cursor() as cur:
        return await cur.execute(
            bcse.select([bcse.c.timestamp, bcse.c.rate])
            .where(
                (bcse.c.currency == currency) &
                (bcse.c.timestamp >= start_dt) &
                (bcse.c.timestamp < end_dt))
        )


async def get_external_rate_live(start_dt: datetime.datetime,
                           end_dt: Optional[datetime.datetime] = None) -> Dict[
    str, Iterable[Tuple[int, Decimal]]]:

    currency_to_rows = defaultdict(list)
    end_dt = end_dt or datetime.datetime(2100, 1, 1)


    async with pool_cursor() as cur:
        async for row in cur.execute(external_rate_live.select(
                (external_rate_live.c.timestamp >= start_dt) &
                (external_rate_live.c.timestamp < end_dt)
        ).order_by(external_rate_live.c.timestamp)):
            currency_to_rows[row.currency].append({
                'ts_open': row.timestamp,
                'ts_received': row.timestamp_received,
                'rate': row.rate,
            })

    for records in currency_to_rows.values():
        for i in range(len(records) - 1):
            ts_close = records[i+1]['ts_open'] - 1

            records[i]['ts_real'] = min(
                max(
                    records[i]['ts_open'],
                    records[i]['ts_received']
                ),
                ts_close
            )

        records[-1]['ts_real'] = max(
            records[-1]['ts_open'],
            records[-1]['ts_received']
        )

    return {
        key: ((x['ts_real'], x['rate']) for x in value)
        for key, value in currency_to_rows.items()
    }


def _parse_external_rate_row(row):
    return {
        'ts_open': row.timestamp,
        'rate_open': row.open,
        'rate_close': row.close,
    }


def _external_rate_data_into_pairs(rates: Sequence[dict]):
    pairs = []

    for i in range(len(rates) - 1):
        pairs.append((
            rates[i]['ts_open'],
            rates[i]['rate_open'],
        ))
        pairs.append((
            rates[i+1]['ts_open'] - 1,
            rates[i]['rate_close'],
        ))

    pairs.append((
        rates[-1]['ts_open'],
        rates[-1]['rate_open'],
    ))

    return pairs


async def get_the_last_external_rates(currencies: Iterable[str], end_dt: datetime.datetime) -> Dict[str, dict]:
    currency_to_data = {}

    async with pool_cursor() as cur:
        for currency in currencies:
            row = await anext(cur.execute(
                external_rate.select(
                    (external_rate.c.timestamp <= end_dt) &
                    (external_rate.c.currency == currency)
                )
                    .order_by(external_rate.c.timestamp)
                    .limit(1)
            ), None)

            if row is not None:
                currency_to_data[currency] = _parse_external_rate_row(row)

    return currency_to_data


async def get_latest_external_rates(
        start_dt: datetime.datetime,
        *,
        at_least_one: bool = False
) -> Dict[str, Iterable[list]]:


    currencies = 'EUR', 'RUB', 'UAH', 'DXY'
    currency_to_rows = defaultdict(list)


    if at_least_one:
        last_data = await get_the_last_external_rates(currencies, end_dt=start_dt)
        for currency, row in last_data.items():
            currency_to_rows[currency].append(row)

    async with pool_cursor() as cur:
        async for row in cur.execute(
            external_rate.select(external_rate.c.timestamp >= start_dt)
                .order_by(external_rate.c.timestamp)
        ):
            currency_to_rows[row.currency].apend(_parse_external_rate_row(row))

    return {
        currency: _external_rate_data_into_pairs(currency_to_rows[currency])
        for currency in currencies
    }


async def get_accumulated_error(date: datetime.date) -> Optional[Decimal]:
    async with pool_cursor() as cur:
        return await anext(cur.execute(
            trade_date.select([trade_date.c.accumulated_error])
            .where((trade_date.c.accumulated_error != None) & (trade_date.c.date <= date))
            .order_by(sa.desc(trade_date.c.date))
            .limit(1)
        ), None)


async def get_last_predicted_trade_date():
    async with pool_cursor() as cur:
        return await anext(cur.execute(
            trade_date.select(trade_date.c.predicted != None)
            .order_by(sa.desc(trade_date.c.date))
            .limit(1)
        ), None)


############## INSERT ############

async def insert_nbrb(data: Iterable[dict], *, kind: NbrbKind):
    data = [{x.lower: item[x] for x in item} for item in data]

    async with pool_cursor() as cur:
        await cur.execute(nbrb.insert(), [{
            kind: kind.value,
            **item
        } for item in data])


async def insert_trade_dates(trade_dates: Collection[str]):
    async with pool_cursor() as cur:
        await cur.execute(trade_date.insert(), [{'date': x} for x in trade_dates])


async def insert_trade_dates_prediction_data(data: Collection[dict]):
    async with pool_cursor() as cur:
        await cur.execute(
            psql_insert(trade_date, data).on_conflict_do_update(index_elements=['date'])
        )


async def insert_dxy_12MSK(data: Iterable[Tuple[str ,str]]):
    async with pool_cursor() as cur:
        await cur.execute(nbrb.insert(), [{
            'date': x[0],
            'dxy': x[1],
            'kind': NbrbKind.GLOBAL.value,
        } for x in data])


async def insert_external_rates(
        currency: str,
        data: Iterator[
            Tuple[
                int,
                str,
                str,
                str,
                str,
                int
            ]
        ]
):
    async with pool_cursor() as cur:
        await cur.execute(
            external_rate.insert(),
            [{
                'currency': currency,
                'timestamp': x[0],
                'open': x[1],
                'close': x[2],
                'low': x[3],
                'high': x[4],
                'volume': x[5],
            } for x in data]
        )


async def insert_external_rate_live(row: ExternalRateData):
    async with pool_cursor() as cur:
        await cur.execute(
            external_rate_live.insert().values(
                currency=row.currency,
                timestamp=row.timestamp_open,
                volume=row.volume,
                timestamp_received=row.timestamp_received,
                rate=row.close,
            )
        )


async def insert_bcse(data: Iterable[BcseData], **kwargs):
    async with pool_cursor() as cur:
        await cur.execute(bcse.insert(), [
            {
                'currency': x.currency,
                'timestamp': x.timestamp_operation,
                'timestamp_received': x.timestamp_received,
                'rate': x.rate,
            } for x in data
        ])


def _ndarray_to_tuple_of_tuples(numpy_array):
    return tuple(tuple(row) for row in numpy_array)


async def insert_prediction(
        *,
        timestamp: int,
        external_rates: Dict[str, str],
        bcse_full: Sequence[Sequence],
        bcse_trusted_global: Sequence[Sequence],
        prediction_record: PredictionRecord
):
    if bcse_full is not None:
        bcse_full = _ndarray_to_tuple_of_tuples(bcse_full)

    if bcse_trusted_global is not None:
        bcse_trusted_global = _ndarray_to_tuple_of_tuples(bcse_trusted_global)

    async with pool_cursor() as cur:
        await cur.execute(prediction.insert().values(
            timestamp=timestamp,
            external_rates=external_rates,
            bcse_full=bcse_full,
            bcse_trusted_global=bcse_trusted_global,
            prediction=prediction_record,
        ))


async def insert_rolling_average(date: datetime.date, duration: int, data: Sequence[Decimal]):
    async with pool_cursor() as cur:
        await cur.execute(rolling_average.insert().values(
            date=date,
            duration=duration,
            eur=data[0],
            rub=data[1],
            uah=data[2],
            dxy=data[3],
        ))
