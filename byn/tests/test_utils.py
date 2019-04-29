from unittest import mock

import pytest
from byn.utils import alist, atuple, once_per


async def _f():
    yield 1
    yield 2
    yield 3


@pytest.mark.asyncio
async def test_alist():
    assert (await alist(_f()) == [1, 2, 3])


@pytest.mark.asyncio
async def test_atuple():
    assert (await atuple(_f()) == (1, 2, 3))


def test_one_per():
    inner = mock.Mock()
    period = 5

    tested = once_per(period)(inner)

    for _ in range(period - 1):
        tested('text', 41)
    inner.assert_not_called()

    tested('text', 42)
    inner.assert_called_once_with('text', 42)

    for _ in range(period):
        tested('text', 43)
    assert inner.call_count == 2
