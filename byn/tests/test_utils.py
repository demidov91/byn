import pytest
from byn.utils import alist, atuple


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