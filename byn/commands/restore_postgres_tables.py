import asyncio
import os
import sys

from byn.commands.restore_postgres_table import run as restore_table


async def run(path):
    paths = [x for x in os.listdir(path) if x.endswith('.csv.gz')]
    await asyncio.gather(*[restore_table(x[:-len('.csv.gz')], os.path.join(path, x)) for x in paths])


if __name__ == '__main__':
    asyncio.run(run(sys.argv[1]))