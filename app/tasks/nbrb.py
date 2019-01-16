"""
nbrb official rates.

Periodical: once a day.
"""
import datetime
import json
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Collection

import requests

import constants as const
from tasks.launch import app
from cassandra_db import get_last_rates, insert_trade_dates, insert_nbrb_rates


client = requests.Session()
logger = logging.getLogger(__name__)


CURR_IDS = {
    '145': 'USD',
    '292': 'EUR',
    '298': 'RUB',
    '290': 'UAH',
}

TRADE_DATES_URL = 'https://banki24.by/exchange/allowed?code=USD'


@app.task
def extract_nbrb(last_trade_dates: Collection[str]):
    date_to_start = get_last_rates()['date']
    if date_to_start is None:
        logger.error('No nbrb rates!')
        return

    date_to_start = (date_to_start + datetime.timedelta(days=2)).date()
    date_to_end = datetime.date.today() + datetime.timedelta(days=1)

    if date_to_end < date_to_start:
        logger.info('Nothing to update.')
        return

    raw_rates = []
    for curr_id in CURR_IDS.keys():
        raw_rates.extend(
            client.get(
                f'http://www.nbrb.by/API/ExRates/Rates/Dynamics/{curr_id}?startDate={date_to_start:%Y-%m-%d}&endDate={date_to_end:%Y-%m-%d}'
            ).json(parse_int=Decimal, parse_float=Decimal)
        )

    formatted_rates = [
        {
            'Date': (datetime.datetime.strptime(x['Date'][:10], '%Y-%m-%d').date() - datetime.timedelta(days=1)).isoformat(),
            'cur': CURR_IDS[str(x['Cur_ID'])],
            'rate': str(x['Cur_OfficialRate']),
        } for x in raw_rates
    ]

    return [x for x in formatted_rates if x['Date'] in last_trade_dates]



@app.task
def update_trade_dates():
    dates = client.get(TRADE_DATES_URL).json()
    insert_trade_dates(dates)
    return dates[-50:]


def update_normalized_nbrb():
    pass


def rates_to_cassandra(rates):
    cass_rates = defaultdict(dict)
    for rate in rates:
        cass_rates[rate['Date']][rate['cur']] = Decimal(rate['rate'])

    for date, rates in cass_rates.items():
        rates['date'] = date

    insert_nbrb_rates(tuple(cass_rates.values()))


def nbrb_file_to_cassandra():
    with open(const.CLEAN_NBRB_DATA, mode='rt') as f:
        data = json.load(f)

    rates_to_cassandra(data)
