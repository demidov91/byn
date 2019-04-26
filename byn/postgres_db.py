import datetime
import simplejson
import logging
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import asdict
from decimal import Decimal
from enum import Enum
from typing import Collection, Dict, Iterable, Iterator, Optional, Sequence, Tuple

import aiopg
import aiopg.sa
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as psql_insert

from byn.datatypes import BcseData, ExternalRateData
from byn.predict.predictor import PredictionRecord
from byn.utils import (
    EnumAwareEncoder,
    anext,
    atuple,
)


logger = logging.getLogger(__name__)
metadata = sa.MetaData()

external_rate = sa.Table('external_rate', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('timestamp_close', sa.Integer),
               sa.Column('open', sa.DECIMAL(12, 6)),
               sa.Column('close', sa.DECIMAL(12, 6)),
               sa.Column('low', sa.DECIMAL(12, 6)),
               sa.Column('high', sa.DECIMAL(12, 6)),
               sa.Column('volume', sa.SMALLINT),
               )

external_rate_live = sa.Table('external_rate_live', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('volume', sa.SMALLINT, primary_key=True),
               sa.Column('timestamp_received', sa.INTEGER),
               sa.Column('rate', sa.DECIMAL(12, 6)),
               )


bcse = sa.Table('bcse', metadata,
               sa.Column('currency', sa.String(3), primary_key=True),
               sa.Column('timestamp', sa.Integer, primary_key=True),
               sa.Column('timestamp_received', sa.INTEGER),
               sa.Column('rate', sa.DECIMAL(12, 6)),
               )

nbrb = sa.Table('nbrb', metadata,
               sa.Column('kind', sa.String(15), primary_key=True),
               sa.Column('date', sa.DATE, primary_key=True),
               sa.Column('usd', sa.DECIMAL(31, 26)),
               sa.Column('eur', sa.DECIMAL(31, 26)),
               sa.Column('rub', sa.DECIMAL(31, 26)),
               sa.Column('uah', sa.DECIMAL(31, 26)),
               sa.Column('byn', sa.DECIMAL(31, 26)),
               sa.Column('dxy', sa.DECIMAL(31, 26)),
               )

prediction = sa.Table('prediction', metadata,
               sa.Column('timestamp', sa.BIGINT, primary_key=True),
               sa.Column('external_rates', sa.JSON),
               sa.Column('bcse_full', sa.JSON),
               sa.Column('bcse_trusted_global', sa.JSON),
               sa.Column('prediction', sa.JSON),
               )

trade_date = sa.Table('trade_date', metadata,
               sa.Column('date', sa.DATE, primary_key=True),
               sa.Column('predicted', sa.DECIMAL(31, 26)),
               sa.Column('prediction_error', sa.DECIMAL(31, 26)),
               sa.Column('accumulated_error', sa.DECIMAL(31, 26)),
               )

rolling_average = sa.Table('rolling_average', metadata,
                   sa.Column('date', sa.DATE, primary_key=True),
                   sa.Column('duration', sa.SMALLINT, primary_key=True),
                   sa.Column('eur', sa.DECIMAL(31, 26)),
                   sa.Column('rub', sa.DECIMAL(31, 26)),
                   sa.Column('uah', sa.DECIMAL(31, 26)),
                   sa.Column('dxy', sa.DECIMAL(31, 26)),
               )


LAST_ROLLING_AVERAGE_MAGIC_DATE = datetime.date(2100, 1, 1)


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
        return await aiopg.sa.create_engine(**DB_DATA)
    except:
        logger.exception("Can't initialize pool.")
        return None



import contextvars
local_engine = contextvars.ContextVar('local_engine')

async def get_engine():
    engine = local_engine.get(None)
    if engine is None:
        engine = await init_pool()
        local_engine.set(engine)
    return engine


@asynccontextmanager
async def connection():
    async with (await get_engine()).acquire() as connection:
        yield connection



class NbrbKind(Enum):
    OFFICIAL = 'official'
    LOCAL = 'local'
    GLOBAL = 'global'


############# SELECT ############
async def get_last_nbrb_record(kind: NbrbKind):
    async with connection() as cur:
        return await anext(
            cur.execute(
                nbrb.select(nbrb.c.kind==kind.value)
                    .order_by(sa.desc(nbrb.c.date))
                    .limit(1)
            ), None
        )

async def get_last_nbrb_global_with_rates():
    async with connection() as cur:
        return await anext(cur.execute(
            nbrb.select(
                (nbrb.c.kind == NbrbKind.GLOBAL.value) &
                (nbrb.c.byn != None)
            )
            .order_by(sa.desc(nbrb.c.date))
            .limit(1)
        ), None)


async def get_nbrb_rate(date: datetime.date, kind: NbrbKind):
    async with connection() as cur:
        return await anext(cur.execute(
            nbrb.select(
                (nbrb.c.date == date) &
                (nbrb.c.kind == kind.value)
            )
        ), None)


async def get_last_external_currency_datetime(currency: str) -> datetime.datetime:
    async with connection() as cur:
        row = await anext(
            cur.execute(
                external_rate.select(external_rate.c.currency==currency)
                    .order_by(sa.desc(external_rate.c.timestamp))
                    .limit(1)
            ), None)

    return datetime.datetime.fromtimestamp(row.timestamp if row is not None else 0)


async def get_last_rolling_average_date() -> Optional[datetime.date]:
    async with connection() as cur:
        row = await anext(cur.execute(
            rolling_average.select(
                rolling_average.c.date < LAST_ROLLING_AVERAGE_MAGIC_DATE
            ).order_by(sa.desc(rolling_average.c.date)).limit(1)
        ), None)

    return row and row.date

async def get_rolling_average_lte(date: datetime.date) -> Tuple:
    if date >= LAST_ROLLING_AVERAGE_MAGIC_DATE:
        raise ValueError(date)

    async with connection() as cur:
        return await atuple(cur.execute(
            rolling_average.select(rolling_average.c.date <= date)
            .order_by(rolling_average.c.date, rolling_average.c.duration)
        ))


async def get_magic_rolling_average():
    async with connection() as conn:
        return await atuple(conn.execute(
            rolling_average
            .select(rolling_average.c.date == LAST_ROLLING_AVERAGE_MAGIC_DATE)
            .order_by(rolling_average.c.duration)
        ))


async def get_nbrb_gt(date: Optional[datetime.date], kind: NbrbKind) -> Tuple:
    query = nbrb.c.kind == kind.value
    if date is not None:
        query &= (nbrb.c.date > date)

    async with connection() as cur:
        return await atuple(cur.execute(nbrb.select(query).order_by(nbrb.c.date)))


async def get_nbrb_lte(date: datetime.date, kind: NbrbKind) -> Tuple:
    async with connection() as cur:
        return await atuple(cur.execute(
            nbrb.select(
                (nbrb.c.date <= date) &
                (nbrb.c.kind == kind.value)
            ).order_by(nbrb.c.date)
        ))


async def get_valid_nbrb_gt(date: datetime.date, kind: NbrbKind) -> Tuple:
    async with connection() as cur:
        return await atuple(cur.execute(
            nbrb.select(
                (nbrb.c.date > date) &
                (nbrb.c.kind == kind.value) &
                (nbrb.c.eur != None) &
                (nbrb.c.rub != None) &
                (nbrb.c.uah != None)
            ).order_by(nbrb.c.date)
        ))



async def get_bcse_in(
        currency: str,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime = None
) -> Iterable[Tuple[int, Decimal]]:
    end_dt = end_dt or datetime.datetime(2035, 1, 1)

    start_dt = start_dt.timestamp()
    end_dt = end_dt.timestamp()

    async with connection() as cur:
        return [
            x.as_tuple() async for x in cur.execute(
            sa.select([bcse.c.timestamp, bcse.c.rate])
            .select_from(bcse)
            .where(
                (bcse.c.currency == currency) &
                (bcse.c.timestamp >= start_dt) &
                (bcse.c.timestamp < end_dt))
            )
        ]


async def get_external_rate_live(start_dt: datetime.datetime,
                           end_dt: Optional[datetime.datetime] = None) -> Dict[
    str, Iterable[Tuple[int, Decimal]]]:

    currency_to_rows = defaultdict(list)
    end_dt = end_dt or datetime.datetime(2100, 1, 1)
    start_dt = start_dt.timestamp()
    end_dt = end_dt.timestamp()


    async with connection() as cur:
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
    end_dt = end_dt.timestamp()

    async with connection() as cur:
        for currency in currencies:
            row = await anext(cur.execute(
                external_rate.select(
                    (external_rate.c.timestamp <= end_dt) &
                    (external_rate.c.currency == currency)
                )
                    .order_by(sa.desc(external_rate.c.timestamp))
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

    async with connection() as cur:
        async for row in cur.execute(
            external_rate.select(external_rate.c.timestamp >= start_dt.timestamp())
                .order_by(external_rate.c.timestamp)
        ):
            currency_to_rows[row.currency].append(_parse_external_rate_row(row))

    return {
        currency: _external_rate_data_into_pairs(currency_to_rows[currency])
        for currency in currencies
    }


async def get_accumulated_error(date: datetime.date) -> Optional[Decimal]:
    async with connection() as cur:
        row = await anext(cur.execute(
            sa.select([trade_date.c.accumulated_error])
            .select_from(trade_date)
            .where((trade_date.c.accumulated_error != None) & (trade_date.c.date <= date))
            .order_by(sa.desc(trade_date.c.date))
            .limit(1)
        ), None)

        return row and row.accumulated_error


async def get_last_predicted_trade_date():
    async with connection() as cur:
        return await anext(cur.execute(
            trade_date.select(trade_date.c.predicted != None)
            .order_by(sa.desc(trade_date.c.date))
            .limit(1)
        ), None)


############## INSERT ############

async def insert_nbrb(data: Iterable[dict], *, kind: NbrbKind):
    data = [{x.lower(): item[x] for x in item} for item in data]

    if not data:
        return

    async with connection() as conn:
        for item in data:
            date = item.pop('date')
            await conn.execute(psql_insert(nbrb, {
                'kind': kind.value,
                'date': date,
                **item
            }).on_conflict_do_update(
                index_elements=['kind', 'date'],
                set_=item
            ))


async def insert_trade_dates(trade_dates: Collection[str]):
    async with connection() as cur:
        await cur.execute(psql_insert(
            trade_date, [{'date': x} for x in trade_dates]
        ).on_conflict_do_nothing())


async def insert_trade_dates_prediction_data(data: Collection[dict]):
    async with connection() as cur:
        for item in data:
            await cur.execute(
                psql_insert(trade_date, item).on_conflict_do_update(
                    index_elements=['date'],
                    set_={
                        'predicted': item['predicted'],
                        'prediction_error': item['prediction_error'],
                        'accumulated_error': item['accumulated_error'],
                    }
                )
            )


async def insert_dxy_12MSK(data: Iterable[Tuple[str ,str]]):
    values = [{
        'date': x[0],
        'dxy': x[1],
        'kind': NbrbKind.GLOBAL.value,
    } for x in data]

    if not values:
        return

    async with connection() as cur:
        await cur.execute(nbrb.insert().values(values))


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
    values = [{
        'currency': currency,
        'timestamp': x[0],
        'open': x[1],
        'close': x[2],
        'low': x[3],
        'high': x[4],
        'volume': x[5],
    } for x in data]

    if not values:
        return

    async with connection() as cur:
        await cur.execute(
            psql_insert(external_rate, values).on_conflict_do_nothing()
        )


async def insert_external_rate_live(row: ExternalRateData):
    async with connection() as cur:
        await cur.execute(
            psql_insert(external_rate_live, dict(
                currency=row.currency,
                timestamp=row.timestamp_open,
                volume=row.volume,
                timestamp_received=row.timestamp_received,
                rate=row.close,
            )).on_conflict_do_update(index_elements=['currency', 'timestamp', 'volume'], set_={
                'timestamp_received': row.timestamp_received,
                'rate': row.close,
            })
        )


async def insert_bcse(data: Iterable[BcseData]):
    values = [{
        'currency': x.currency,
        'timestamp': x.timestamp_operation,
        'timestamp_received': x.timestamp_received,
        'rate': x.rate,
    } for x in data]

    if not values:
        return

    async with connection() as cur:
        await cur.execute(
            psql_insert(bcse, values).on_conflict_do_nothing()
        )


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

    async with connection() as cur:
        await cur.execute(prediction.insert().values(
            timestamp=timestamp,
            external_rates=simplejson.dumps(external_rates, cls=EnumAwareEncoder),
            bcse_full=simplejson.dumps(bcse_full, cls=EnumAwareEncoder),
            bcse_trusted_global=simplejson.dumps(bcse_trusted_global, cls=EnumAwareEncoder),
            prediction=simplejson.dumps(asdict(prediction_record), cls=EnumAwareEncoder),
        ))


async def insert_rolling_average(date: datetime.date, duration: int, data: Sequence[Decimal]):
    data = dict(
        date=date,
        duration=duration,
        eur=data[0],
        rub=data[1],
        uah=data[2],
        dxy=data[3],
    )

    async with connection() as cur:
        await cur.execute(
            psql_insert(rolling_average, data).on_conflict_do_update(
                index_elements=['date', 'duration'],
                set_=data
            )
        )
