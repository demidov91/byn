import datetime

import pytest

from byn.realtime import bcse


@pytest.mark.parametrize('date,expected', [
    (datetime.date(2019, 4, 29), False),
    (datetime.date(2019, 4, 30), False),
    (datetime.date(2019, 5, 1), True),
    (datetime.date(2019, 5, 2), False),
    (datetime.date(2019, 5, 3), False),
    (datetime.date(2019, 5, 4), False),
    (datetime.date(2019, 5, 5), True),
    (datetime.date(2019, 5, 6), True),
    (datetime.date(2019, 5, 7), True),
    (datetime.date(2019, 5, 8), True),
    (datetime.date(2019, 5, 9), True),
    (datetime.date(2019, 5, 10), False),
    (datetime.date(2019, 5, 11), False),
    (datetime.date(2019, 5, 12), True),
])
def test_is_holiday__may_2019(date, expected):
    assert bcse.is_holiday(date) is expected


@pytest.mark.parametrize('date,expected', [
    (datetime.date(2019, 11, 4), False),
    (datetime.date(2019, 11, 5), False),
    (datetime.date(2019, 11, 6), False),
    (datetime.date(2019, 11, 7), True),
    (datetime.date(2019, 11, 8), True),
    (datetime.date(2019, 11, 9), True),
    (datetime.date(2019, 11, 10), True),
])
def test_is_holiday__november_2019(date, expected):
    assert bcse.is_holiday(date) is expected


@pytest.mark.parametrize('date', [
    datetime.date(2001, 1, 1),
    datetime.date(2002, 1, 7),
    datetime.date(2003, 3, 8),
    datetime.date(2004, 5, 1),
    datetime.date(2005, 5, 9),
    datetime.date(2006, 5, 9),
    datetime.date(2007, 7, 3),
    datetime.date(2008, 11, 7),
    datetime.date(2009, 12, 25),
])
def test_is_holiday__annual_holidays(date):
    assert bcse.is_holiday(date)
