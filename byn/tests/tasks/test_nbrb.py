from decimal import Decimal
from unittest import mock

from byn.postgres_db import NbrbKind
from byn.tasks import nbrb


class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


@mock.patch('byn.tasks.nbrb.insert_nbrb', new_callable=AsyncMock)
def test_load_nbrb(patched):
    source = [
        {"Date": "2015-11-02", "cur": "USD", "rate": "1.7421"},
        {"Date": "2015-11-03", "cur": "USD", "rate": "1.7447"},
        {"Date": "2015-11-02", "cur": "EUR", "rate": "1.9225"},
        {"Date": "2015-11-03", "cur": "EUR", "rate": "1.9222"},
        {"Date": "2015-11-02", "cur": "RUB", "rate": "2.7304"},
        {"Date": "2015-11-03", "cur": "RUB", "rate": "2.7314"},
        {"Date": "2015-11-02", "cur": "UAH", "rate": "7.5743"},
        {"Date": "2015-11-03", "cur": "UAH", "rate": "7.561"},
    ]

    nbrb.load_nbrb(source)

    patched.assert_called_once_with((
        {
            'date': '2015-11-02',
            'USD': Decimal('1.7421'),
            'EUR': Decimal('1.9225'),
            'RUB': Decimal('2.7304'),
            'UAH': Decimal('7.5743'),
        },
        {
            'date': '2015-11-03',
            'USD': Decimal('1.7447'),
            'EUR': Decimal('1.9222'),
            'RUB': Decimal('2.7314'),
            'UAH': Decimal('7.561'),
        },
    ), kind=NbrbKind.OFFICIAL)
