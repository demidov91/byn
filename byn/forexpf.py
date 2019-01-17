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



def forexpf_data_into_rows(data: dict) -> Sequence[OrderedDict]:
    return [
        OrderedDict(
            ('time', x[0]),
            ('open', x[1]),
            ('close', x[2]),
            ('high', x[3]),
            ('low', x[4]),
            ('value', x[5]),
        ) for x in zip(
            data['t'],
            data['o'],
            data['c'],
            data['h'],
            data['l'],
            data['v'],
        )
    ]


def get_forexpf_rows(
    *args, **kwargs
) -> Sequence[OrderedDict]:
    return forexpf_data_into_rows(get_data(*args, **kwargs))