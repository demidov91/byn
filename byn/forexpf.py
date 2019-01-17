import datetime
from collections import OrderedDict
from decimal import Decimal
from typing import Sequence

import requests


client = requests.Session()


CURRENCY_CODES = {
    'RUB': 29,
    'DXY': 11,
    'EUR': 42,
    'UAH': 379,
}


def get_data(
        *,
        currency_code: int=None,
        currency: str=None,
        resolution: int,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
) -> dict:
    if currency_code is None:
        currency_code = CURRENCY_CODES[currency]

    data_url = f'https://charts.forexpf.ru/html/tw/history?' \
               f'symbol={currency_code}&' \
               f'resolution={resolution}&' \
               f'from={int(start_dt.timestamp())}&' \
               f'to={int(end_dt.timestamp())}'

    return client.get(data_url).json(parse_float=Decimal)



def forexpf_data_into_dicts(data: dict) -> Sequence[OrderedDict]:
    return [
        OrderedDict((
            ('time', x[0]),
            ('open', x[1]),
            ('close', x[2]),
            ('high', x[3]),
            ('low', x[4]),
            ('value', x[5]),
        )) for x in zip(
            data['t'],
            data['o'],
            data['c'],
            data['l'],
            data['h'],
            data['v'],
        )
    ]


def forexpf_dicts_to_tuples(data):
    for row in data:
        for rate_key in ('open', 'close', 'low', 'high'):
            row[rate_key] = str(row[rate_key])

    return [tuple(x.values()) for x in data]



def forexpf_data_into_tuples(data: dict) -> Sequence[tuple]:
    return forexpf_dicts_to_tuples(forexpf_data_into_dicts(data))


def get_forexpf_dicts(
    *args, **kwargs
) -> Sequence[OrderedDict]:
    return forexpf_data_into_dicts(get_data(*args, **kwargs))




def get_forexpf_tuples(*args, **kwargs):
    return forexpf_dicts_to_tuples(get_forexpf_dicts(*args, **kwargs))